module HitchhikerMap where

import           Data.Map      (Map)
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Debug.Trace

import           Impl.Tree
import           Index
import           Leaf
import           Types
import           Utils

import qualified Data.Map      as M
import qualified Data.Sequence as Q

-- Stolen directly from hasky-btree for testing. Too small in practice to hold
-- up a real large channel database.
twoThreeConfig :: TreeConfig
twoThreeConfig = TREECONFIG {
    minFanout = minFanout'
  , maxFanout = maxFanout'
  , minIdxKeys = minFanout' - 1
  , maxIdxKeys = maxFanout' - 1
  , minLeafItems = minFanout'
  , maxLeafItems = 2*minFanout' - 1
  , maxHitchhikers = minFanout'
  }
  where
    minFanout' = 2
    maxFanout' = 2*minFanout' - 1

empty :: TreeConfig -> HitchhikerMap k v
empty config = HITCHHIKERTREE config Nothing

insert :: Ord k => k -> v -> HitchhikerMap k v -> HitchhikerMap k v
insert !k !v !(HITCHHIKERTREE config (Just root)) =
  HITCHHIKERTREE config (Just newRoot)
  where
    newRoot = let newRootIdx =
                    insertRec config hhMapTF (Q.singleton (k, v)) root
      in case fromSingletonIndex newRootIdx of
          Just newRootNode ->
            -- The result from the recursive insert is a single node. Use
            -- this as a new root.
            newRootNode
          Nothing ->
            -- The insert resulted in a index with multiple nodes, i.e.
            -- the splitting propagated to the root. Create a new 'Idx'
            -- node with the index. This increments the height.
            HitchhikerNodeIndex newRootIdx mempty

insert k v (HITCHHIKERTREE config Nothing)
  = HITCHHIKERTREE config (Just $ HitchhikerNodeLeaf $ Q.singleton (k, v))

-- -----------------------------------------------------------------------

hhMapTF :: Ord k => TreeFun k (HitchhikerMapNode k v) (k, v) (k, v)
hhMapTF = TreeFun {
  mkIndex = HitchhikerNodeIndex,
  mkLeaf = HitchhikerNodeLeaf,
  caseNode = \case
      HitchhikerNodeIndex a b -> Left (a, b)
      HitchhikerNodeLeaf l    -> Right l,
  leafMerge = foldl $ \items (k, v) -> qSortedInsert k v items,
  hhMerge = foldl $ \items (k, v) -> qSortedInsert k v items,
  leafKey = \(k, _) -> k,
  hhKey = \(k, _) -> k
  }

-- Lookup --------------------------------------------------------------------

lookup :: Ord k => k -> HitchhikerMap k v -> Maybe v
lookup key (HITCHHIKERTREE _ Nothing) = Nothing
lookup key (HITCHHIKERTREE _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerNodeIndex index hitchhikers ->
        case findInHitchhikers key hitchhikers of
          Just v  -> Just v
          Nothing -> lookInNode $ findSubnodeByKey key index
      HitchhikerNodeLeaf items -> findInLeaves key items
