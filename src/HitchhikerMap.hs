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

empty :: TreeConfig -> HitchhikerMap k v
empty config = HITCHHIKERMAP config Nothing

insert :: (Show k, Show v, Ord k) => k -> v -> HitchhikerMap k v -> HitchhikerMap k v
insert !k !v !(HITCHHIKERMAP config (Just root)) =
  HITCHHIKERMAP config (Just newRoot)
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
            HitchhikerMapNodeIndex newRootIdx mempty

insert !k !v (HITCHHIKERMAP config Nothing)
  = HITCHHIKERMAP config (Just $ HitchhikerMapNodeLeaf $ Q.singleton (k, v))

-- -----------------------------------------------------------------------

hhMapTF :: Ord k => TreeFun k (HitchhikerMapNode k v) (k, v) (k, v)
hhMapTF = TreeFun {
  mkIndex = HitchhikerMapNodeIndex,
  mkLeaf = HitchhikerMapNodeLeaf,
  caseNode = \case
      HitchhikerMapNodeIndex a b -> Left (a, b)
      HitchhikerMapNodeLeaf l    -> Right l,
  leafMerge = foldl $ \items (k, v) -> qSortedAssocInsert k v items,
  hhMerge = foldl $ \items (k, v) -> qSortedAssocInsert k v items,
  leafKey = fst,
  hhKey = fst
  }

-- Lookup --------------------------------------------------------------------

lookup :: Ord k => k -> HitchhikerMap k v -> Maybe v
lookup key (HITCHHIKERMAP _ Nothing) = Nothing
lookup key (HITCHHIKERMAP _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerMapNodeIndex index hitchhikers ->
        case findInHitchhikers key hitchhikers of
          Just v  -> Just v
          Nothing -> lookInNode $ findSubnodeByKey key index
      HitchhikerMapNodeLeaf items -> findInLeaves key items
