module HitchhikerTree where

import           Control.Monad.State (State, evalState, get, modify, runState)

import           Data.Hashable
import           Data.Map            (Map)
import           Data.Sequence       (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Debug.Trace

import           Index
import           Types
import           Utils

import qualified Data.Map            as M
import qualified Data.Sequence       as Q


-- What are the design tradeoffs?
--
-- - Trying to make any sort of index shadow tree just does more work than
--   making hash links canonical in a single data structure, so don't do that.
--
-- - Doing explicit hashing instead of letting the system handle tracking pins
--   itself adds work now instead of doing things lazily.


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

empty :: TreeConfig -> FullTree k v
empty config = FULLTREE config Nothing mempty

insert :: (Show k, Show v, Ord k, Hashable k, Hashable v)
       => k -> v -> FullTree k v -> FullTree k v
insert k v (FULLTREE config (Just rootHash) storage) =
  trace ("Inserting (" ++ show k ++ ", " ++ show v ++ ") -> {newRoot=" ++
         show newRoot ++ ", storage=" ++ show newStorage ++ "}")
  FULLTREE config newRoot newStorage
  where
    (newRoot, newStorage) = runState runInsert storage

    runInsert = do
      traceM $ "runInsert"
      newRootIdx <- insertRec config (Q.singleton (k, v)) rootHash
      case fromSingletonIndex newRootIdx of
        Just newRootNode -> do
          traceM $ "Just newRootNode"
          -- The result from the recursive insert is a single node. Use
          -- this as a new root.
          pure $ Just newRootNode
        Nothing -> do
          traceM $ "Nothing"
          -- The insert resulted in a index with multiple nodes, i.e.
          -- the splitting propagated to the root. Create a new 'Idx'
          -- node with the index. This increments the height.
          --
          -- TODO: Need to figure out if you can just have an empty hitchhiker
          -- list here. I am 90% sure this is correct, but think about it more.
          newIndex <- addNode $ NodeIndex newRootIdx mempty
          pure $ Just newIndex

insert k v (FULLTREE config Nothing _)
  = FULLTREE config (Just topHash) (M.singleton topHash topNode)
  where
    topNode = NodeLeaf $ Q.singleton (k, v)
    topHash = hash topNode

-- -----------------------------------------------------------------------

replace :: (Hashable k, Hashable v)
        => Hash256 -> TreeNode k v -> State (NodeStorage k v) Hash256
replace old new = do
  removeNode old
  addNode new

addNode :: (Hashable k, Hashable v)
        => TreeNode k v -> State (NodeStorage k v) Hash256
addNode new = do
  let h = hash new
  modify (M.insert h new)
  pure h

removeNode :: Hash256 -> State (NodeStorage k v) ()
removeNode h = do
  traceM $ "Removing node " ++ show h
  modify (M.delete h)

getNode :: Hash256 -> State (NodeStorage k v) (TreeNode k v)
getNode h = do
  l <- get
  case M.lookup h l of
    Nothing -> error "TreeError: invalid hash in getNode"
    Just v  -> pure v

insertRec :: (Show k, Show v, Ord k, Hashable k, Hashable v)
          => TreeConfig -> Hitchhikers k v -> Hash256
          -> State (NodeStorage k v) (Index k Hash256)
insertRec config toAdd node = getNode node >>= \case
  NodeIndex children hitchhikers
    | (Q.length hitchhikers + Q.length toAdd) > (maxHitchhikers config) -> do
        -- We have reached the maximum number of hitchhikers, we now need to
        -- flush these downwards.
        --
        -- Vague algorithm: sort the list of hitchhikers into buckets with the
        -- same number as children, for each child with a bucket,
        --
        let allAdd = sortByKey $ hitchhikers <> toAdd

        -- The old algorithm was to just punch one `hole` in the current
        -- NodeIndex and keep the "IndexCtx" around with that hole. We instead
        -- have to check that we are

        newFullIdx <- distributeDownwards config allAdd children emptyIndex
        removeNode node
        extendIndex (maxLeafItems config) newFullIdx

    | otherwise -> do
        -- All we must do is rebuild the node with the new k/v pair added on as
        -- a hitchhiker to this node.
        removeNode node
        newIdx <- addNode $ NodeIndex children (hitchhikers <> toAdd)
        pure $ singletonIndex newIdx

  NodeLeaf items -> do
    removeNode node
    -- TODO: mergeItems needs to instead deal with added items replacing kv
    -- pairs in the leaf.
    splitLeafMany (maxLeafItems config) $ mergeItems items toAdd

-- Given a sorted list of hitchhikers, try to distribute each downward to the
-- next level. This function is responsible for sending the right output to
-- indexRec, and parsing that return value back into a coherent index.
distributeDownwards :: (Show k, Show v, Ord k, Hashable k, Hashable v)
                    => TreeConfig
                    -> Hitchhikers k v
                    -> Index k Hash256   -- input
                    -> Index k Hash256  -- building output
                    -> State (NodeStorage k v) (Index k Hash256)

-- Base case: single subtree or end of list:
distributeDownwards config
                    hitchhikers
                    (Index Empty (node :<| Empty))
                    (Index oKeys oHashes) = do
  case hitchhikers of
    Empty -> do
      -- there are no hitchhikers, running insertRec will just break the node
      -- structure, since removeNode will just remove the existing node.
      let out = Index oKeys (oHashes |> node)
      pure out
    hh -> do
      -- all remaining hitchhikers are either to the right or are in the mono
      -- node.
      (Index endKeys endHashes) <- insertRec config hitchhikers node
      removeNode node

      let out = Index (oKeys <> endKeys) (oHashes <> endHashes)
      traceM $ "distributeDownwards base case: " ++ show out
      pure out

distributeDownwards config
                    hitchhikers
                    i@(Index (key :<| restKeys) (node :<| restNodes))
                    (Index outKeys outNodes) = do
  traceM $ "Start distributeDownwards round: hh=" ++ show hitchhikers ++
           ", i=" ++ show i
  let (toAdd, rest) = Q.spanl (\(k, v) -> k < key) hitchhikers
  traceM $ "rangeidx = " ++ show toAdd
  case toAdd of
    Empty ->
      -- There are no things to distribute downward to this subtree.
      distributeDownwards config
                          hitchhikers
                          (Index restKeys restNodes)
                          (Index (outKeys |> key) (outNodes |> node))
    toAdd -> do
      -- We distribute downwards all items up to the split point.
      inner@(Index subKeys subHashes) <- insertRec config toAdd node
      removeNode node
      traceM $ "dd recur inner: " ++ show inner

      let out = Index (outKeys <> subKeys <> (Q.singleton key))
                      (outNodes <> subHashes)
      traceM $ "distributeDownards recur case: " ++ show out
      distributeDownwards config
                          rest
                          (Index restKeys restNodes)
                          out


sortByKey :: Ord k => Seq (k, v) -> Seq (k, v)
sortByKey = Q.sortBy (\(a, _) (b, _) -> compare a b)

mergeItems :: (Ord k)
           => LeafVector k v -> Hitchhikers k v -> LeafVector k v
mergeItems = foldl $ \items (k, v) -> qSortedInsert k v items

-- Given a pure index with no hitchhikers, create a node.
extendIndex :: (Show k, Show v, Hashable k, Hashable v)
            => Int -> Index k Hash256
            -> State (NodeStorage k v) (Index k Hash256)
extendIndex maxIdxKeys = go
  where
    maxIdxVals = maxIdxKeys + 1

    go index
      | numVals <= maxIdxVals = do
          idxHash <- addNode $ NodeIndex index mempty
          pure $ singletonIndex idxHash

      | numVals <= 2 * maxIdxVals = do
          let (leftIndex, middleKey, rightIndex) =
                splitIndexAt (div numVals 2 - 1) index
          leftHash <- addNode $ NodeIndex leftIndex mempty
          rightHash <- addNode $ NodeIndex rightIndex mempty
          pure $ indexFromList (Q.singleton middleKey)
                               (Q.fromList [leftHash, rightHash])

      | otherwise = error "TODO: Implement extendIndex for more than 2 split"
      where
        numVals = indexNumVals index

-- splitLeafMany returns an index to all the leaves, even if it's a singleton
-- leaf.
splitLeafMany :: (Show k, Show v, Hashable k, Hashable v)
              => Int
              -> LeafVector k v
              -> State (NodeStorage k v) (Index k Hash256)
splitLeafMany maxLeafItems items
  -- Leaf items don't overflow a single node.
  | Q.length items <= maxLeafItems = do
      traceM $ "splitLeafMany items: " ++ (show items)
      leafHash <- addNode $ NodeLeaf items
      pure $ singletonIndex leafHash

  -- We have to split, but only into two nodes.
  | Q.length items <= 2 * maxLeafItems = do
      let numLeft = div (Q.length items) 2
      let (leftLeaf, rightLeaf) = Q.splitAt numLeft items
      leftHash <- addNode $ NodeLeaf leftLeaf
      rightHash <- addNode $ NodeLeaf rightLeaf
      let rightFirstItem = fst $ qHeadUnsafe rightLeaf
      traceM $ "splitLeafMany 2: splitAt=" ++ (show rightFirstItem)
      pure $ indexFromList (Q.singleton rightFirstItem)
                           (Q.fromList [leftHash, rightHash])

  -- We have to split the node into more than two nodes.
  | otherwise = error "TODO: Implement the hard case when we do bulk loading."

-- Lookup --------------------------------------------------------------------

lookup :: Ord k => k -> FullTree k v -> Maybe v
lookup key (FULLTREE _ Nothing _) = Nothing
lookup key (FULLTREE _ (Just top) storage) = evalState (lookInNode top) storage
  where
    lookInNode current = getNode current >>= \case
      NodeIndex index hitchhikers -> do
        case findInHitchhikers key hitchhikers of
          Just v  -> pure $ Just v
          Nothing -> let (_, v) = findSubnodeByKey key index
                         in lookInNode v
      NodeLeaf items -> pure $ findInLeaves key items
