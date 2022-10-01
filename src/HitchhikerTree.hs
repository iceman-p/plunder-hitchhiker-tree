module HitchhikerTree where

import           Control.Monad.State (State, evalState, get, modify, runState)

import           Data.Hashable
import           Data.Map            (Map)
import           Data.Sequence       (Seq)
import           Debug.Trace

import           Types

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
removeNode h = modify (M.delete h)

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
                    (Index Q.Empty (node Q.:<| Q.Empty))
                    (Index oKeys oHashes) = do
  -- all remaining hitchhikers are either to the right or are in the mono node.
  (Index endKeys endHashes) <- insertRec config hitchhikers node
  removeNode node

  let out = Index (oKeys <> endKeys) (oHashes <> endHashes)
  traceM $ "distributeDownwards base case: " ++ show out
  pure out

distributeDownwards config
                    hitchhikers
                    (Index (key Q.:<| restKeys) (node Q.:<| restNodes))
                    (Index outKeys outNodes) = do
  let rangeIdx = Q.findIndexL (\(k, _) -> k >= key) hitchhikers
  case rangeIdx of
    Nothing ->
      -- There are no things to distribute downward to this subtree.
      distributeDownwards config
                          hitchhikers
                          (Index restKeys restNodes)
                          (Index (outKeys Q.|> key) (outNodes Q.|> node))
    Just splitIdx -> do
      -- We distribute downwards all items up to the split point.
      let (toAdd, rest) = Q.splitAt splitIdx hitchhikers
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
      pure $ indexFromList (Q.singleton rightFirstItem)
                           (Q.fromList [leftHash, rightHash])

  -- We have to split the node into more than two nodes.
  | otherwise = error "TODO: Implement the hard case when we do bulk loading."

-- Lookup --------------------------------------------------------------------

-- Lookup procedure:

lookup :: Ord k => k -> FullTree k v -> Maybe v
lookup key (FULLTREE _ Nothing _) = Nothing
lookup key (FULLTREE _ (Just top) storage) = evalState (lookInNode top) storage
  where
    lookInNode current = getNode current >>= \case
      NodeIndex (Index keys vals) hitchhikers -> do
        -- First try walking across the hitchhikers backwards. Backwards
        -- because the hitchhikers form a log. If we find a
        let idx = Q.findIndexR (\(k, v) -> k == key) hitchhikers
        case idx of
          Just idx -> case Q.lookup idx hitchhikers of
            Nothing     -> error "impossible"
            Just (_, v) -> pure $ Just v
          Nothing -> do
            -- There's nothing in the hitchhikers. Recurse downward to the
            -- bottom.
            --
            -- TODO: This could be simplified, but is hacked out of the
            -- previous implementation.
            let (leftKeys, _) = Q.spanl (<=key) keys
                n = Q.length leftKeys
                (_, valAndRightVals) = Q.splitAt n vals
                Just (val, _) = qUncons valAndRightVals
            lookInNode val

      NodeLeaf items -> do
        -- TODO: This could be a binary search instead, since this seq is
        -- ordered.
        let idx = Q.findIndexL (\(k, v) -> k == key) items
        case idx of
          Just idx -> case Q.lookup idx items of
            Nothing     -> error "impossible"
            Just (_, v) -> pure $ Just v
          Nothing -> pure Nothing



-- Index ---------------------------------------------------------------------

indexFromList :: Seq k -> Seq Hash256 -> Index k Hash256
indexFromList keys valPtrs = Index keys valPtrs

singletonIndex :: Hash256 -> Index k Hash256
singletonIndex = Index Q.empty . Q.singleton

fromSingletonIndex :: Index key Hash256 -> Maybe Hash256
fromSingletonIndex (Index _keys vals) =
    if Q.length vals == 1 then Just $! qHeadUnsafe vals else Nothing

indexNumKeys :: Index key Hash256 -> Int
indexNumKeys (Index keys _vals) = Q.length keys

indexNumVals :: Index key Hash256 -> Int
indexNumVals (Index _keys vals) = Q.length vals

splitIndexAt :: Int -> Index key Hash256
             -> (Index key Hash256, key, Index key Hash256)
splitIndexAt numLeftKeys (Index keys vals)
    | (leftKeys, middleKeyAndRightKeys) <- Q.splitAt numLeftKeys     keys
    , (leftVals, rightVals)             <- Q.splitAt (numLeftKeys+1) vals
    = case qUncons middleKeyAndRightKeys of
        Just (middleKey,rightKeys) ->
            (Index leftKeys leftVals, middleKey, Index rightKeys rightVals)
        Nothing -> error "splitIndex: cannot split an empty index"

-- -----------------------------------------------------------------------

qHeadUnsafe :: Seq a -> a
qHeadUnsafe (first Q.:<| _) = first
qHeadUnsafe _               = error "qHeadUnsafe"

qSortedInsert :: (Ord k) => k -> v -> Seq (k, v) -> Seq (k, v)
qSortedInsert k v s = case Q.findIndexL (\(i, _) -> k <= i) s of
  Nothing  -> s Q.|> (k, v)
  Just idx -> case Q.lookup idx s of
    Just (curk, curv) | curk == k -> Q.update idx (k,v) s
    Just _                        -> Q.insertAt idx (k,v) s
    Nothing                       -> error "impossible"

qUncons :: Seq a -> Maybe (a, Seq a)
qUncons = \case
  (head Q.:<| rest) -> Just (head, rest)
  Q.Empty           -> Nothing


-- -----------------------------------------------------------------------

someFunc :: IO ()
someFunc = putStrLn "TODO"
