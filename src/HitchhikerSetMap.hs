module HitchhikerSetMap where

import           Data.Map      (Map)
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))

import           Impl.Tree
import           Index
import           Leaf
import           Types
import           Utils

import qualified HitchhikerSet as S

import qualified Data.Map      as M
import qualified Data.Sequence as Q

empty :: TreeConfig -> HitchhikerSetMap k v
empty config = HITCHHIKERSETMAP config Nothing

hhSetMapTF
  :: (Show k, Show v, Ord k, Ord v)
  => TreeConfig
  -> TreeFun k (HitchhikerSetMapNode k v) (k, v) (k, HitchhikerSet v)
hhSetMapTF setConfig = TreeFun {
  mkIndex = HitchhikerSetMapNodeIndex,
  mkLeaf = HitchhikerSetMapNodeLeaf,
  caseNode = \case
      HitchhikerSetMapNodeIndex a b -> Left (a, b)
      HitchhikerSetMapNodeLeaf l    -> Right l,
  leafMerge = hhSetLeafMerge setConfig,
  hhMerge = foldl $ \items (k, v) -> qSortedAssocSetInsert k v items,
  leafKey = fst,
  hhKey = fst
  }

hhSetLeafMerge :: (Show k, Show v, Ord k, Ord v)
               => TreeConfig
               -> Seq (k, HitchhikerSet v)
               -> Seq (k, v)
               -> Seq (k, HitchhikerSet v)
hhSetLeafMerge config sets Empty = sets
hhSetLeafMerge config sets ((k, v) :<| xs) = hhSetLeafMerge config combined xs
  where
    combined = case Q.findIndexL (\(i, _) -> k <= i) sets of
      Nothing  -> sets |> (k, S.singleton config v)
      Just idx -> case Q.lookup idx sets of
        Just (curk, curset)
          | curk == k -> Q.update idx (k, S.insert v curset) sets
        Just _        -> Q.insertAt idx (k, S.singleton config v) sets
        Nothing       -> error "impossible"

insert :: (Show k, Show v, Ord k, Ord v)
       => k -> v -> HitchhikerSetMap k v -> HitchhikerSetMap k v
insert !k !v !(HITCHHIKERSETMAP config (Just root)) =
    HITCHHIKERSETMAP config (Just newRoot)
  where
    newRoot =
      let newRootIdx =
            insertRec config (hhSetMapTF config) (Q.singleton (k, v)) root
      in case fromSingletonIndex newRootIdx of
          Just newRootNode ->
            -- The result from the recursive insert is a single node. Use
            -- this as a new root.
            newRootNode
          Nothing ->
            -- The insert resulted in a index with multiple nodes, i.e.
            -- the splitting propagated to the root. Create a new 'Idx'
            -- node with the index. This increments the height.
            HitchhikerSetMapNodeIndex newRootIdx mempty

insert !k !v (HITCHHIKERSETMAP config Nothing)
  = HITCHHIKERSETMAP config $ Just $ HitchhikerSetMapNodeLeaf $
                       Q.singleton (k, S.singleton config v)

-- For SetMap lookup, we must always resolve our hitchhikers downwards to a
-- leaf for combination into a set. We can't just stop halfway.
--
-- Unlike Map lookup and Set member, we can't just stop when we find a
-- hitchhiker. We must take the hitchhikers which match and carry them
-- down to the leaves.
lookup :: (Show k, Show v, Ord k, Ord v)
       => k
       -> HitchhikerSetMap k v
       -> HitchhikerSet v
lookup key (HITCHHIKERSETMAP config Nothing) = S.empty config
lookup key (HITCHHIKERSETMAP config (Just top)) = lookInNode Q.empty top
  where
    lookInNode hh b =
      case b of
        HitchhikerSetMapNodeIndex index hitchhikers ->
          lookInNode (hh <> matchHitchhikers hitchhikers) $
            findSubnodeByKey key index
        HitchhikerSetMapNodeLeaf items ->
          buildSetFrom items hh

    -- TODO: Make this binary search in production.
    matchHitchhikers = (fmap snd) . (Q.filter \(k, _) -> k == key)

    -- We have a list of hitchhikers and the leaf values. Turn that into a
    -- final list.
    buildSetFrom leaves hh = case findInLeaves key leaves of
      Nothing  -> S.fromSeq config hh
      Just ret -> foldl (flip S.insert) ret hh
