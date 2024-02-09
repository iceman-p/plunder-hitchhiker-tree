module HitchhikerSetMap where

import           ClassyPrelude

import           Data.Map      (Map)
import           Data.Set      (Set)

import           Safe          (tailSafe)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import           Data.Sorted

import qualified HitchhikerSet as HS

import qualified Data.Map      as M
import qualified Data.Set      as S
import qualified Data.Vector   as V

empty :: TreeConfig -> HitchhikerSetMap k v
empty config = HITCHHIKERSETMAP config Nothing

-- Takes a list of leaves and make a valid
fromLeafMaps :: (Show k, Show v, Ord k, Ord v)
             => TreeConfig
             -> [Map k (NakedHitchhikerSet v)]
             -> HitchhikerSetMap k v
fromLeafMaps config []   = HitchhikerSetMap.empty config
fromLeafMaps config [m]  = HITCHHIKERSETMAP config $ Just
                         $ HitchhikerSetMapNodeLeaf m
fromLeafMaps config rawMaps = HITCHHIKERSETMAP config $ Just node
  where
    node = fixUp config (hhSetMapTF config) treeIndex
    treeIndex = indexFromList idxV vals
    idxV = V.fromList $ tailSafe $ map (fst . M.findMin) rawMaps
    vals = V.fromList $ map HitchhikerSetMapNodeLeaf rawMaps

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
             (Map k (ArraySet v))
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

  hhMerge = M.unionWith ssetUnion,
  -- For the hitchhikers, we count the number of all items in a set instead of
  -- the number of keys.
  hhLength = sum . map ssetSize . M.elems,
  hhSplit = splitImpl,
  hhEmpty = M.empty,
  hhDelete = \k mybV sm -> case mybV of
      Just v  -> M.update (Just . ssetDelete v) k sm
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
               -> Map k (ArraySet v)
               -> Map k (NakedHitchhikerSet v)
leafInsertImpl config leaf hh = foldl' merge leaf (M.toList hh)
  where
    merge items (k, vSet) = M.alter (alt vSet) k items

    alt :: ArraySet v
        -> Maybe (NakedHitchhikerSet v)
        -> Maybe (NakedHitchhikerSet v)
    alt new Nothing    = Just (strip $ HS.fromArraySet config new)
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
    HITCHHIKERSETMAP config $ Just $ insertRaw config k v root

insert !k !v (HITCHHIKERSETMAP config Nothing)
  = HITCHHIKERSETMAP config $ Just $ HitchhikerSetMapNodeLeaf $
                       M.singleton k (strip $ HS.singleton config v)

insertRaw :: (Show k, Show v, Ord k, Ord v)
          => TreeConfig -> k -> v
          -> HitchhikerSetMapNode k v
          -> HitchhikerSetMapNode k v
insertRaw config !k !v root =
    fixUp config (hhSetMapTF config) $
    insertRec config (hhSetMapTF config) (M.singleton k (ssetSingleton v)) root

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

listToHitchhikers :: (Show v, Show k, Ord k, Ord v)
                  => [(k, v)] -> Map k (ArraySet v)
listToHitchhikers = go M.empty
  where
    go m []         = m
    go m ((k,v):xs) = go (M.alter (alt v) k m) xs

    alt v Nothing  = Just (ssetSingleton v)
    alt v (Just s) = Just (ssetInsert v s)

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
lookup key (HITCHHIKERSETMAP config (Just top)) = lookInNode mempty top
  where
    lookInNode :: ArraySet v -> HitchhikerSetMapNode k v -> HitchhikerSet v
    lookInNode hh b =
      case b of
        HitchhikerSetMapNodeIndex index hitchhikers ->
          lookInNode (hh <> matchHitchhikers hitchhikers) $
            findSubnodeByKey key index
        HitchhikerSetMapNodeLeaf items ->
          buildSetFrom items hh

    matchHitchhikers :: Map k (ArraySet v) -> ArraySet v
    matchHitchhikers hh = fromMaybe mempty (M.lookup key hh)

    -- We have a list of hitchhikers and the leaf values. Turn that into a
    -- final list.
    buildSetFrom leaves hh = case M.lookup key leaves of
      Nothing  -> HS.fromArraySet config hh
      Just ret -> HS.insertMany hh (weave config ret)

-- TODO: This isn't the best performing implementation, but it's Good Enough
-- for now. This is an attempt at using something like the
-- MultiIntesersectV3Naive code in a different context. This should be good
-- enough at "small" (<100K) items, and will probably be faster on the naive
-- tree walking interpreter than a proper tree pruning implementation for the
-- same reason V6Stack was: function frames are super expensive.
--
restrictKeys :: forall k v
              . (Show k, Show v, Ord k, Ord v)
             => HitchhikerSet k
             -> HitchhikerSetMap k v
             -> HitchhikerSetMap k v
restrictKeys _ orig@(HITCHHIKERSETMAP _ Nothing) = orig

restrictKeys (HITCHHIKERSET _ Nothing) (HITCHHIKERSETMAP config _) =
  HitchhikerSetMap.empty config

restrictKeys (HITCHHIKERSET sConfig (Just a))
             (HITCHHIKERSETMAP mConfig (Just b)) =
  fromLeafMaps mConfig $ setlistMaplistIntersect [] as bs
  where
    as = getLeafList HS.hhSetTF a
    bs = getLeafList (hhSetMapTF mConfig) b

-- We walk the as and bs list in parallel. When the current a overlaps with
-- the current b, we put a into a list of partial mapping sets and keep
-- accumulating those until we don't have a
setlistMaplistIntersect :: forall k v
         . (Show k, Ord k, Show v)
        => [ArraySet k]
        -> [ArraySet k]
        -> [Map k v]
        -> [Map k v]
setlistMaplistIntersect _ a []                = []
setlistMaplistIntersect [] [] b                = []
setlistMaplistIntersect partial ao@(a:as) bo@(b:bs) =
    let aMin = ssetFindMin a
        aMax = ssetFindMax a
        bMin = fst $ M.findMin b
        bMax = fst $ M.findMax b
        overlap = aMin <= bMax && bMin <= aMax

        filteredBy s rest = let f = M.restrictKeys b s
                            in if M.null f then rest
                                           else f:rest

        toSet :: [ArraySet k] -> Set k
        toSet = foldl' S.union S.empty . map (S.fromList . ssetToAscList)

    in case (partial, overlap) of
         ([], False)
           | aMax > bMax -> setlistMaplistIntersect [] ao bs
           | otherwise   -> setlistMaplistIntersect [] as bo
         (partial, False) -> error "Should be impossible"
         (partial, True)
           | aMax == bMax ->
               filteredBy (toSet (a:partial)) $ setlistMaplistIntersect [] as bs
           | aMax < bMax ->
               setlistMaplistIntersect (a:partial) as bo
           | otherwise ->
               filteredBy (toSet (a:partial)) $ setlistMaplistIntersect [] ao bs

a = setlistMaplistIntersect [] [ssetFromList [1, 2], ssetFromList [3, 4]] [M.fromList [(1, "a"), (2, "b"), (3, "c"), (4, "d")]]

b = setlistMaplistIntersect [] [ssetFromList [1, 2], ssetFromList [3, 5], ssetFromList [6]]
            [M.fromList [(1, "a"), (2, "b"), (3, "c")],
             M.fromList [(4, "d"), (5, "e"), (6, "f")]]



-- Dang. Now to debug the above pile of garbage.
