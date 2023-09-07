module HitchhikerSetMap where

import           ClassyPrelude

import           Data.Map      (Map)
import           Data.Set      (Set)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import qualified HitchhikerSet as HS

import qualified Data.Map      as M
import qualified Data.Set      as S

empty :: TreeConfig -> HitchhikerSetMap k v
empty config = HITCHHIKERSETMAP config Nothing

-- SetMap is the complicated one, because we use two different representations:
-- one for the leafs (where we use HitchhikerSets to collect the mass keys) and
-- one for hitchhikers (which are normal sets for quick insertion).
--
hhSetMapTF
  :: (Show k, Show v, Ord k, Ord v)
  => TreeConfig
  -> TreeFun k
             v
             (HitchhikerSetMapNode k v)
             (Map k (Set v))
             (Map k (NakedHitchhikerSet v))
hhSetMapTF setConfig = TreeFun {
  mkNode = HitchhikerSetMapNodeIndex,
  mkLeaf = HitchhikerSetMapNodeLeaf,
  caseNode = \case
      HitchhikerSetMapNodeIndex a b -> Left (a, b)
      HitchhikerSetMapNodeLeaf l    -> Right l,

  leafInsert = leafInsertImpl setConfig,
  leafMerge = leafMergeImpl setConfig {- M.unionWith HS.union -},
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = \k mybV sm -> case mybV of
      Just v  -> M.update (\s -> Just $ strip $ HS.delete v (weave setConfig s)) k sm
      Nothing -> error "Impossible leaf delete in setmap",

  hhMerge = M.unionWith S.union,
  -- For the hitchhikers, we count the number of all items in a set instead of
  -- the number of keys.
  hhLength = sum . map S.size . M.elems,
  hhSplit = splitImpl,
  hhEmpty = M.empty,
  hhDelete = \k mybV sm -> case mybV of
      Just v  -> M.update (Just . S.delete v) k sm
      Nothing -> error "Impossible leaf delete in setmap"
  }

strip :: HitchhikerSet v -> NakedHitchhikerSet v
strip (HITCHHIKERSET _ x) = NAKEDSET x

weave :: TreeConfig -> NakedHitchhikerSet v -> HitchhikerSet v
weave tc (NAKEDSET x) = (HITCHHIKERSET tc x)

leafInsertImpl :: forall k v
                . (Show k, Show v, Ord k, Ord v)
               => TreeConfig
               -> Map k (NakedHitchhikerSet v)
               -> Map k (Set v)
               -> Map k (NakedHitchhikerSet v)
leafInsertImpl config leaf hh = foldl' merge leaf (M.toList hh)
  where
    merge items (k, vSet) = M.alter (alt vSet) k items

    alt :: Set v
        -> Maybe (NakedHitchhikerSet v)
        -> Maybe (NakedHitchhikerSet v)
    alt new Nothing    = Just (strip $ HS.fromSet config new)
    alt new (Just old) = Just (strip $ HS.insertMany new (weave config old))

leafMergeImpl :: forall k v
               . (Show k, Show v, Ord k, Ord v)
              => TreeConfig
              -> Map k (NakedHitchhikerSet v)
              -> Map k (NakedHitchhikerSet v)
              -> Map k (NakedHitchhikerSet v)
leafMergeImpl config = M.unionWith doUnion
  where
    doUnion :: NakedHitchhikerSet v
            -> NakedHitchhikerSet v
            -> NakedHitchhikerSet v
    doUnion a b = strip $ HS.union (weave config a) (weave config b)

splitImpl :: Ord k => k -> Map k v -> (Map k v, Map k v)
splitImpl k m = M.spanAntitone (< k) m

insert :: (Show k, Show v, Ord k, Ord v)
       => k -> v -> HitchhikerSetMap k v -> HitchhikerSetMap k v
insert !k !v !(HITCHHIKERSETMAP config (Just root)) =
    HITCHHIKERSETMAP config $ Just $
    fixUp config (hhSetMapTF config) $
    insertRec config (hhSetMapTF config) (M.singleton k (S.singleton v)) root

insert !k !v (HITCHHIKERSETMAP config Nothing)
  = HITCHHIKERSETMAP config $ Just $ HitchhikerSetMapNodeLeaf $
                       M.singleton k (strip $ HS.singleton config v)

insertMany :: (Show k, Show v, Ord k, Ord v)
           => [(k, v)] -> HitchhikerSetMap k v -> HitchhikerSetMap k v
insertMany !items !(HITCHHIKERSETMAP config Nothing)
  = HITCHHIKERSETMAP config $ Just $ fixUp config (hhSetMapTF config) $
    splitLeafMany (hhSetMapTF config)
                  (maxLeafItems config)
                  (listToLeaves config items)

insertMany !items !(HITCHHIKERSETMAP config (Just root))
  = HITCHHIKERSETMAP config $ Just $
    fixUp config (hhSetMapTF config) $
    insertRec config (hhSetMapTF config) (listToHitchhikers items) root

delete :: (Show k, Ord k, Show v, Ord v)
       => k -> v -> HitchhikerSetMap k v -> HitchhikerSetMap k v
delete _ _ !(HITCHHIKERSETMAP config Nothing) = HITCHHIKERSETMAP config Nothing
delete !k !v !(HITCHHIKERSETMAP config (Just root)) =
  case deleteRec config (hhSetMapTF config) k (Just v) root of
    HitchhikerSetMapNodeIndex index hitchhikers
      | Just childNode <- fromSingletonIndex index ->
          if M.null hitchhikers then HITCHHIKERSETMAP config (Just childNode)
          else insertMany (mapSetToList hitchhikers) $
               HITCHHIKERSETMAP config (Just childNode)
    HitchhikerSetMapNodeLeaf items
      | M.null items -> HITCHHIKERSETMAP config Nothing
    newRootNode -> HITCHHIKERSETMAP config (Just newRootNode)

listToLeaves :: (Show v, Show k, Ord k, Ord v)
             => TreeConfig -> [(k, v)] -> Map k (NakedHitchhikerSet v)
listToLeaves config = go M.empty
  where
    go m []         = m
    go m ((k,v):xs) = go (M.alter (alt v) k m) xs

    alt v Nothing  = Just (strip $ HS.singleton config v)
    alt v (Just s) = Just (strip $ HS.insert v (weave config s))

listToHitchhikers :: (Show v, Show k, Ord k, Ord v) => [(k, v)] -> Map k (Set v)
listToHitchhikers = go M.empty
  where
    go m []         = m
    go m ((k,v):xs) = go (M.alter (alt v) k m) xs

    alt v Nothing  = Just (S.singleton v)
    alt v (Just s) = Just (S.insert v s)

-- For SetMap lookup, we must always resolve our hitchhikers downwards to a
-- leaf for combination into a set. We can't just stop halfway.
--
-- Unlike Map lookup and Set member, we can't just stop when we find a
-- hitchhiker. We must take the hitchhikers which match and carry them
-- down to the leaves.
lookup :: forall k v
        . (Show k, Show v, Ord k, Ord v)
       => k
       -> HitchhikerSetMap k v
       -> HitchhikerSet v
lookup key (HITCHHIKERSETMAP config Nothing)    = HS.empty config
lookup key (HITCHHIKERSETMAP config (Just top)) = lookInNode S.empty top
  where
    lookInNode :: Set v -> HitchhikerSetMapNode k v -> HitchhikerSet v
    lookInNode hh b =
      case b of
        HitchhikerSetMapNodeIndex index hitchhikers ->
          lookInNode (hh <> matchHitchhikers hitchhikers) $
            findSubnodeByKey key index
        HitchhikerSetMapNodeLeaf items ->
          buildSetFrom items hh

    matchHitchhikers :: Map k (Set v) -> Set v
    matchHitchhikers hh = fromMaybe (S.empty) (M.lookup key hh)

    -- We have a list of hitchhikers and the leaf values. Turn that into a
    -- final list.
    buildSetFrom leaves hh = case M.lookup key leaves of
      Nothing  -> HS.fromSet config hh
      Just ret -> HS.insertMany hh (weave config ret)
