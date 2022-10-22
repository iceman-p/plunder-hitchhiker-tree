module HitchhikerSet where

import           Data.Map      (Map)
import           Data.Maybe
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Debug.Trace

import           Impl.Tree
import           Index
import           Leaf
import           Types
import           Utils

import qualified Data.Map      as M
import qualified Data.Sequence as Q

empty :: TreeConfig -> HitchhikerSet k
empty config = HITCHHIKERSET config Nothing

insert :: Ord k => k -> HitchhikerSet k -> HitchhikerSet k
insert !k !(HITCHHIKERSET config (Just root)) =
  HITCHHIKERSET config (Just newRoot)
  where
    newRoot = let newRootIdx =
                    insertRec config hhSetTF (Q.singleton k) root
      in case fromSingletonIndex newRootIdx of
          Just newRootNode ->
            -- The result from the recursive insert is a single node. Use
            -- this as a new root.
            newRootNode
          Nothing ->
            -- The insert resulted in a index with multiple nodes, i.e.
            -- the splitting propagated to the root. Create a new 'Idx'
            -- node with the index. This increments the height.
            HitchhikerSetNodeIndex newRootIdx mempty

insert !k (HITCHHIKERSET config Nothing)
  = HITCHHIKERSET config (Just $ HitchhikerSetNodeLeaf $ Q.singleton k)

-- -----------------------------------------------------------------------

hhSetTF :: Ord k => TreeFun k (HitchhikerSetNode k) k k
hhSetTF = TreeFun {
  mkIndex = HitchhikerSetNodeIndex,
  mkLeaf = HitchhikerSetNodeLeaf,
  caseNode = \case
      HitchhikerSetNodeIndex a b -> Left (a, b)
      HitchhikerSetNodeLeaf l    -> Right l,
  leafMerge = foldl $ \items k -> qSortedInsert k items,
  hhMerge = foldl $ \items k -> qSortedInsert k items,
  leafKey = id,
  hhKey = id
  }

-- -----------------------------------------------------------------------

member :: Ord k => k -> HitchhikerSet k -> Bool
member key (HITCHHIKERSET _ Nothing) = False
member key (HITCHHIKERSET _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerSetNodeIndex index hitchhikers ->
        case findInSetHitchhikers key hitchhikers of
          True  -> True
          False -> lookInNode $ findSubnodeByKey key index
      HitchhikerSetNodeLeaf items -> findInSetLeaves key items

findInSetHitchhikers :: Eq k => k -> Seq k -> Bool
findInSetHitchhikers key hh = isJust $ Q.findIndexR (== key) hh

findInSetLeaves :: Eq k => k -> Seq k -> Bool
findInSetLeaves key leaves = isJust $ Q.findIndexL (== key) leaves
