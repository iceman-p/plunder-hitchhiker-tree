module HitchhikerMap where

import           ClassyPrelude hiding (empty)

import           Data.Map      (Map)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import qualified Data.Map      as M

empty :: TreeConfig -> HitchhikerMap k v
empty config = HITCHHIKERMAP config Nothing

insert :: (Show k, Show v, Ord k)
       => k -> v -> HitchhikerMap k v -> HitchhikerMap k v
insert !k !v !(HITCHHIKERMAP config (Just root)) =
  HITCHHIKERMAP config $ Just $ insertRaw config k v root

insert !k !v (HITCHHIKERMAP config Nothing)
  = HITCHHIKERMAP config (Just $ HitchhikerMapNodeLeaf $ M.singleton k v)

insertRaw :: (Show k, Show v, Ord k)
          => TreeConfig -> k -> v -> HitchhikerMapNode k v
          -> HitchhikerMapNode k v
insertRaw config !k !v root =
  fixUp config hhMapTF $
  insertRec config hhMapTF (M.singleton k v) root

insertMany :: (Show k, Show v, Ord k, Ord v)
           => Map k v -> HitchhikerMap k v -> HitchhikerMap k v

insertMany !items !(HITCHHIKERMAP config Nothing) =
  HITCHHIKERMAP config $ Just $
  fixUp config hhMapTF $
  splitLeafMany hhMapTF (maxLeafItems config) items

insertMany !items !(HITCHHIKERMAP config (Just root)) =
  HITCHHIKERMAP config $ Just $
  fixUp config hhMapTF $
  insertRec config hhMapTF items root

-- -----------------------------------------------------------------------

delete :: (Show k, Show v, Ord k, Ord v)
       => k -> HitchhikerMap k v -> HitchhikerMap k v
delete _ !(HITCHHIKERMAP config Nothing) = HITCHHIKERMAP config Nothing
delete !k !(HITCHHIKERMAP config (Just root)) =
  case deleteRec config hhMapTF k Nothing root of
    HitchhikerMapNodeIndex index hitchhikers
      | Just childNode <- fromSingletonIndex index ->
          if M.null hitchhikers then HITCHHIKERMAP config (Just childNode)
          else insertMany hitchhikers $ HITCHHIKERMAP config (Just childNode)
    HitchhikerMapNodeLeaf items
      | M.null items -> HITCHHIKERMAP config Nothing
    newRootNode -> HITCHHIKERMAP config (Just newRootNode)

-- -----------------------------------------------------------------------

hhMapTF :: (Show k, Show v, Ord k)
        => TreeFun k v (HitchhikerMapNode k v) (Map k v) (Map k v)
hhMapTF = TreeFun {
  mkNode = HitchhikerMapNodeIndex,
  mkLeaf = HitchhikerMapNodeLeaf,
  caseNode = \case
      HitchhikerMapNodeIndex a b -> Left (a, b)
      HitchhikerMapNodeLeaf l    -> Right l,

  leafInsert = M.unionWith (\a b -> b),
  leafMerge = (<>),
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = \k mybV m -> case mybV of
      Nothing -> M.delete k m
      Just _  -> error "Can't delete specific values in map",

  hhMerge = M.unionWith (\a b -> b),
  hhLength = M.size,
  hhSplit = splitImpl,
  hhEmpty = M.empty,
  hhDelete = \k mybV m -> case mybV of
      Nothing -> M.delete k m
      Just _  -> error "Can't delete specific values in map"
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
