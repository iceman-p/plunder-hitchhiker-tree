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

insert :: (Show k, Show v, Ord k)
       => k -> v -> HitchhikerMap k v -> HitchhikerMap k v
insert !k !v !(HITCHHIKERMAP config (Just root)) =
  HITCHHIKERMAP config $ Just $
  fixUp config hhMapTF $
  insertRec config hhMapTF (Q.singleton (k, v)) root

insert !k !v (HITCHHIKERMAP config Nothing)
  = HITCHHIKERMAP config (Just $ HitchhikerMapNodeLeaf $ Q.singleton (k, v))

insertMany :: (Show k, Show v, Ord k, Ord v)
           => Seq (k, v) -> HitchhikerMap k v -> HitchhikerMap k v

insertMany !items !(HITCHHIKERMAP config Nothing) =
  HITCHHIKERMAP config $ Just $
  fixUp config hhMapTF $
  splitLeafMany (maxLeafItems config) HitchhikerMapNodeLeaf fst $
  hhMergeImpl Q.empty items

insertMany !items !(HITCHHIKERMAP config (Just root)) =
  HITCHHIKERMAP config $ Just $
  fixUp config hhMapTF $
  insertRec config hhMapTF (hhMergeImpl Q.empty items) root

-- -----------------------------------------------------------------------

hhMapTF :: Ord k => TreeFun k (HitchhikerMapNode k v) (k, v) (k, v)
hhMapTF = TreeFun {
  mkIndex = HitchhikerMapNodeIndex,
  mkLeaf = HitchhikerMapNodeLeaf,
  caseNode = \case
      HitchhikerMapNodeIndex a b -> Left (a, b)
      HitchhikerMapNodeLeaf l    -> Right l,
  leafMerge = foldl $ \items (k, v) -> qSortedAssocInsert k v items,
  hhMerge = hhMergeImpl,
  leafKey = fst,
  hhKey = fst
  }

hhMergeImpl :: Ord k => Seq (k, v) -> Seq (k, v) -> Seq (k, v)
hhMergeImpl = foldl $ \items (k, v) -> qSortedAssocInsert k v items

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
