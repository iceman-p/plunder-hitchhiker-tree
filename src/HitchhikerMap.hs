module HitchhikerMap where


import           Control.DeepSeq
import           Data.Map        (Map)
import           Data.Sequence   (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Debug.Trace

import           Index
import           Leaf
import           Types
import           Utils

import qualified Data.Map        as M
import qualified Data.Sequence   as Q

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

emptyIndex = Index mempty mempty

empty :: TreeConfig -> HitchhikerMap k v
empty config = HITCHHIKERTREE config Nothing

insert :: Ord k => k -> v -> HitchhikerMap k v -> HitchhikerMap k v
insert !k !v !(HITCHHIKERTREE config (Just root)) =
  HITCHHIKERTREE config (Just newRoot)
  where
    newRoot = let newRootIdx = insertRec config (Q.singleton (k, v)) root
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

insertRec :: Ord k
          => TreeConfig
          -> Hitchhikers k v
          -> HitchhikerMapNode k v
          -> Index k (HitchhikerMapNode k v)
insertRec config Empty node = singletonIndex node
insertRec config toAdd node = case node of
    HitchhikerNodeIndex children hitchhikers
      | Q.length merged > maxHitchhikers config ->
          -- We have reached the maximum number of hitchhikers, we now need to
          -- flush these downwards.
          extendIndex (maxLeafItems config) HitchhikerNodeIndex $
             distributeDownwards config merged children emptyIndex

      | otherwise ->
          -- All we must do is rebuild the node with the new k/v pair added on
          -- as a hitchhiker to this node.
          singletonIndex $ HitchhikerNodeIndex children merged
      where
        merged = mergeItems hitchhikers toAdd

    HitchhikerNodeLeaf items ->
      splitLeafMany (maxLeafItems config) HitchhikerNodeLeaf $ mergeItems items toAdd

-- Given a sorted list of hitchhikers, try to distribute each downward to the
-- next level. This function is responsible for sending the right output to
-- indexRec, and parsing that return value back into a coherent index.
distributeDownwards :: Ord k
                    => TreeConfig
                    -> Hitchhikers k v
                    -> Index k (HitchhikerMapNode k v)  -- input
                    -> Index k (HitchhikerMapNode k v)  -- building output
                    -> Index k (HitchhikerMapNode k v)

-- Base case: single subtree or end of list:
distributeDownwards config
                    hitchhikers
                    (Index Empty (node :<| Empty))
                    (Index oKeys oHashes) =
  case hitchhikers of
    Empty ->
      -- there are no hitchhikers, running insertRec will just break the node
      -- structure, since removeNode will just remove the existing node.
      Index oKeys (oHashes |> node)
    hh ->
      -- all remaining hitchhikers are either to the right or are in the mono
      -- node.
      let (Index endKeys endHashes) = insertRec config hitchhikers node
      in Index (oKeys <> endKeys) (oHashes <> endHashes)

distributeDownwards config
                    hitchhikers
                    i@(Index (key :<| restKeys) (node :<| restNodes))
                    (Index outKeys outNodes) =
  let (toAdd, rest) = Q.spanl (\(k, v) -> k < key) hitchhikers
  in case toAdd of
    Empty ->
      -- There are no things to distribute downward to this subtree.
      distributeDownwards config
                          hitchhikers
                          (Index restKeys restNodes)
                          (Index (outKeys |> key) (outNodes |> node))
    toAdd ->
      -- We distribute downwards all items up to the split point.
      let inner@(Index subKeys subHashes) = insertRec config toAdd node
          out = Index (outKeys <> subKeys <> (Q.singleton key))
                      (outNodes <> subHashes)
      in distributeDownwards config
                             rest
                             (Index restKeys restNodes)
                             out

mergeItems :: (Ord k)
           => LeafVector k v -> Hitchhikers k v -> LeafVector k v
mergeItems = foldl $ \items (k, v) -> qSortedInsert k v items

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
