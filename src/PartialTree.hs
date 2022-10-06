module PartialTree (PartialTree(..),
                    FetchRequest(..),
                    newPartialFromRoot,
                    ensureRange,
                    PartialTree.lookup,
                    fetched
                    ) where

import           Control.Monad
import           Data.Hashable
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Data.Set      (Set)
import           Debug.Trace
import           Types
import           Utils

import qualified Data.Map      as M
import qualified Data.Sequence as Q

-- A PartialTree is a partial mirror of a FullTree. PartialTree is meant to
-- replicate as much structure as possible from a FullTree and be updatable
-- when the root of the FullTree changes.


-- The server side tree is a set of hashes, but the client side tree needs to
-- keep track of ranges of the tree that it already has.

data PartialTree k v = PartialTree (Maybe (PartialTreeNode k v))

type PartialIndex k v = Index k (Hash256, PartialTreeNode k v)

-- -----------------------------------------------------------------------

-- | The toplevel of a partially synced tree. All value nodes keep track of
-- their own Hash256.
data PartialTreeNode k v
  -- An index node that has known holes in it.
  --
  -- When the last hole is filled, this gets switched to a Completed node.
  = IncompleteIndex Hash256 (Index k (PartialTreeNode k v)) (Hitchhikers k v)

  -- | We don't have a copy of this node. We store instead the part of the
  -- index that covered this range in hopes of reconstituting grandchild nodes
  -- in the future.
  | MissingNode Hash256 (Index k (PartialTreeNode k v))

  -- A node which has completed synchronization.
  | Completed (CompletedSync k v)
  deriving (Show, Eq)

-- | Subtrees which have completed synchronization and don't hold references to
-- old data.
data CompletedSync k v
  -- | Index node that's completed synchronization. We keep the hash around in
  -- the index so we can hopefully reuse parts of the tree when we
  = CompletedIndex Hash256 (Index k (CompletedSync k v)) (Hitchhikers k v)

  -- | All leaf nodes are complete; you either have it in its entirety or you
  -- do not.
  | CompletedLeaf Hash256 (LeafVector k v)
  deriving (Show, Eq)

-- | A pair of a KeyLoc and the hash to search by content address.
type FetchRequest k = (Maybe k, Hash256)

-- | Inclusive range query based on key type.
data Range k
  = LessThanEq k
  | Between k k
  | GreaterThenEq k

-- -----------------------------------------------------------------------

toCompleted :: PartialTreeNode k v -> Maybe (CompletedSync k v)
toCompleted (IncompleteIndex h (Index keys vals) hh) = do
  completedVals <- mapM toCompleted vals
  pure $ CompletedIndex h (Index keys completedVals) hh
toCompleted (MissingNode _ _) = Nothing
toCompleted (Completed s) = Just s

upgradeWhenComplete :: PartialTreeNode k v -> PartialTreeNode k v
upgradeWhenComplete ptn = case toCompleted ptn of
  Nothing        -> ptn
  Just completed -> Completed completed

-- | Given the value of a root node, create a PartialTree to track it.
newPartialFromRoot :: Hash256 -> PartialTree k v

newPartialFromRoot hash =
  PartialTree (Just $ MissingNode hash $ Index mempty mempty)


-- | Given a range (from, to), calculate all the nodes we know we'd need to
-- fetch to make progress on fully syncing the range.
--
-- Returns an empty set when the range is fully synced.
ensureRange :: forall k v. Ord k
            => Range k -> PartialTree k v -> Seq (FetchRequest k)
ensureRange _     (PartialTree Nothing)     = Empty
ensureRange range (PartialTree (Just root)) = scan range Nothing root
  where
    scan _     _   (Completed _)             = Empty
    scan range loc (MissingNode hash _)      = Q.singleton (loc, hash)
    scan range _   (IncompleteIndex _ idx _) =
      join $ fmap (\(k,v) -> scan range k v) $ indexPairs $
                    filterNodes range idx

    filterNodes (LessThanEq k)    = removeGreaterThan k
    filterNodes (Between lt gt)   = removeGreaterThan lt . removeLessThan gt
    filterNodes (GreaterThenEq k) = removeLessThan k

-- | Attempts to lookup a piece of data. Returns either a request for another
-- node in the tree or a definitive answer of whether the key is there or not.
lookup :: Ord k
       => k
       -> PartialTree k v
       -> Either (FetchRequest k) (Maybe v)

lookup key (PartialTree Nothing)     = Right $ Nothing

lookup key (PartialTree (Just node)) = lookupInPartial (Nothing, node)
  where
    lookupInPartial (_, (IncompleteIndex hash idx hitchhikers)) =
      case findInHitchhikers key hitchhikers of
        Just v  -> Right $ Just v
        Nothing -> lookupInPartial $ findSubnodeByKey key idx

    lookupInPartial (loc, (MissingNode hash idx)) = Left $ (loc, hash)

    lookupInPartial (_, (Completed completed)) = lookupInCompleted completed

    lookupInCompleted (CompletedIndex _ idx hitchhikers) =
      case findInHitchhikers key hitchhikers of
        Just v  -> Right $ Just v
        Nothing -> lookupInCompleted $ snd $ findSubnodeByKey key idx

    lookupInCompleted (CompletedLeaf _ leaves) =
      Right $ findInLeaves key leaves

-- | We have received a piece requested. We now insert that back into the
-- structure, reconstituting as much of the tree as we can if the MissingNode
-- had
fetched :: forall k v. (Ord k, Show k, Show v)
        => FetchRequest k
        -> TreeNode k v
        -> PartialTree k v
        -> PartialTree k v
fetched _ _ (PartialTree Nothing) = PartialTree Nothing
fetched fr@(fetchK, hash) fetched (PartialTree (Just tree))
  = PartialTree $ Just $ findTarget tree
  -- What does this do? First we must navigate downward to the place in the
  -- tree where we need to perform the completion, then we need to
  where
    findTarget :: PartialTreeNode k v -> PartialTreeNode k v
    -- Navigate through the tree to the node containing the MissingNode subref.
    findTarget ii@(IncompleteIndex nodeHash idx hh) =
      trace ("fetch request: " ++ show fr) $
      trace ("fetched: " ++ show fetched) $
      trace ("fetched state: " ++ show tree) $
      -- Find the place in the index where we could have (k, hash) be a thing
      -- that needs to be replaced.
      upgradeWhenComplete $ IncompleteIndex nodeHash newIdx hh
      where
        newIdx = mapSubnodeByLoc findTarget fetchK idx


    findTarget mn@(MissingNode nodeHash prevIndex)
      | hash == nodeHash = case fetched of
          NodeLeaf leafVector -> Completed $ CompletedLeaf nodeHash leafVector
          NodeIndex hashIdx hh -> upgradeWhenComplete $
            IncompleteIndex nodeHash (updateIndex hashIdx) hh
      | otherwise = error $ "MISSING NODE HASH MISMATCH: " ++ (show hash) ++ ", " ++ (show nodeHash)
      where
        -- TODO: To make forward progress, I'm punting on trying to rebuild
        -- parts of the index from an older index. Come back here and complete
        -- this when I have subscription like things working.
        updateIndex hashIdx = mapIndexWithLoc fakeMissing hashIdx
          where
            fakeMissing (_, _, hash) = MissingNode hash (Index mempty mempty)
        {-
        updateIndex = mapIndexWithLoc completeFromPrevious

        -- OK, I'm confused about the

        -- Given a location and a hash, we return a node. Either a MissingNode
        -- with a restricted prevIndex if there's nothing in `prevIndex` to
        -- complete with, or
        completeFromPrevious (prev, next, hash) =
          let narrowedIdx = narrowIndexTo prev next prevIndex
          in case lookupByHash next hash narrowedIdx of
            Nothing         -> MissingNode hash narrowedIdx
            Just completion -> completion
        -}

    -- Don't navigate into completed areas of the tree.
    findTarget c@(Completed _) = c

{-
-- Given a hash, try to find the associated
lookupByHash :: Maybe k -> Hash256 -> Index k v -> Maybe v
lookupByHash key hash idx = case findSubnodeByKey key idx of
  (right, ii@(IncompleteIndex iiHash subIdx _))
    | hash == iiHash -> Just ii

narrowIndexTo :: Maybe k -> Maybe k -> Index k v -> Index k v
narrowIndexTo prev next idx = undefined {- final
  where
    prevNarrowed = case prev of
      Nothing -> prev
      Just k -> removeLessThan
    final =
-}
-}

-- -----------------------------------------------------------------------

{-
-- | We've learned that the tree we're caching has changed its root node.
replaceRoot :: PartialTreeNode k v -> TreeNode k v -> PartialTreeNode k v
replaceRoot = go (Index mempty mempty)
  where
    go :: PartialIndex k v
       -> PartialTreeNode k v
       -> TreeNode k v
       -> PartialTreeNode k v
    go partIdx (CompleteIndex prevIdx prevHH) (NodeIndex inIdx inHH)
      -- If the indexes are equivalent, the update is just to the hitchhikers.
      | indexesSame prevIdx inIdx = CompleteIndex prevIdx inHH

      -- Otherwise we need to walk across the two indexes in parallel, copying
      -- previously
      | otherwise = undefined

    indexesSame :: Index k (Hash256, a) -> Index k Hash256 -> Bool
    indexesSame (Index _ p) (Index _ prevHashes) = prevHashes == fmap fst p
-}

