module HitchhikerMap where

import           Data.Map      (Map)
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Debug.Trace

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
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
  insertRec config hhMapTF (M.singleton k v) root

insert !k !v (HITCHHIKERMAP config Nothing)
  = HITCHHIKERMAP config (Just $ HitchhikerMapNodeLeaf $ M.singleton k v)

insertMany :: (Show k, Show v, Ord k, Ord v)
           => Map k v -> HitchhikerMap k v -> HitchhikerMap k v

insertMany !items !(HITCHHIKERMAP config Nothing) =
  HITCHHIKERMAP config $ Just $
  fixUp config hhMapTF $
  splitLeafMany2 hhMapTF (maxLeafItems config) items

insertMany !items !(HITCHHIKERMAP config (Just root)) =
  HITCHHIKERMAP config $ Just $
  fixUp config hhMapTF $
  insertRec config hhMapTF items root

-- -----------------------------------------------------------------------

hhMapTF :: (Show k, Show v, Ord k)
        => TreeFun k (HitchhikerMapNode k v) (Map k v) (Map k v)
hhMapTF = TreeFun {
  mkNode = HitchhikerMapNodeIndex,
  mkLeaf = HitchhikerMapNodeLeaf,
  caseNode = \case
      HitchhikerMapNodeIndex a b -> Left (a, b)
      HitchhikerMapNodeLeaf l    -> Right l,

  leafMerge = M.unionWith (\a b -> b),
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,

  hhMerge = M.unionWith (\a b -> b),
  hhLength = M.size,
  hhSplit = splitImpl,
  hhEmpty = M.empty
  }

splitImpl :: Ord k => k -> Map k v -> (Map k v, Map k v)
splitImpl k m = M.spanAntitone (< k) m

-- Lookup --------------------------------------------------------------------

lookup :: Ord k => k -> HitchhikerMap k v -> Maybe v
lookup key (HITCHHIKERMAP _ Nothing) = Nothing
lookup key (HITCHHIKERMAP _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerMapNodeIndex index hitchhikers ->
        case M.lookup key hitchhikers of
          Just v  -> Just v
          Nothing -> lookInNode $ findSubnodeByKey key index
      HitchhikerMapNodeLeaf items -> M.lookup key items
