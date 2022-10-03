module PartialTree (PartialTree(..),
                    FetchRequest(..),
                    newPartialFromRoot,
                    ensureRange
                    ) where

import           Control.Monad
import           Data.Hashable
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Data.Set      (Set)
import           Types
import           Utils

import qualified Data.Map      as M
import qualified Data.Sequence as Q

-- OK: So v0 of this is


-- Google gives nothing. Instead, we're going to think very hard.
--
-- You receive a new root. That root:
--
-- - Will never contain the old root hash as a subtree: if something changed
--   that pushes up a new root, that root's hash has changed.


-- The server side tree is a set of hashes, but the client side tree needs to
-- keep track of ranges of the tree that it already has.

data PartialTree k v = PartialTree { root :: Maybe (PartialTreeNode k v) }

type PartialIndex k v = Index k (Hash256, PartialTreeNode k v)

-- -----------------------------------------------------------------------

-- | The toplevel of a partially synced tree.
data PartialTreeNode k v
  -- An index node that has known holes in it.
  --
  -- When the last hole is filled, this gets switched to a Completed node.
  = IncompleteIndex (Index k (Hash256, Subref k v)) (Hitchhikers k v)

  -- A node which has completed synchronization.
  | Completed (CompletedSync k v)
  deriving (Show, Eq)

-- | Subtrees which have completed synchronization and don't hold Subrefs to
-- possibly old data.
data CompletedSync k v
  -- | Index node that's completed synchronization. We keep the hash around in
  -- the index so we can hopefully reuse parts of the tree when we
  = CompletedIndex (Index k (Hash256, CompletedSync k v)) (Hitchhikers k v)

  -- | All leaf nodes are complete; you either have it in its entirety or you
  -- do not.
  | CompletedLeaf (LeafVector k v)
  deriving (Show, Eq)

-- | A reference in an IncompleteIndex.
data Subref k v
  -- | Everything under this hash has been completely loaded.
  = Complete (CompletedSync k v)

  -- | We don't have a copy of this node. We store instead the part of the
  -- index that previously covered.
  | MissingNode (Index k (Hash256, Subref k v))

  -- | W
  | PartialNode (PartialTreeNode k v)
  deriving (Show, Eq)

-- | A pair of the rightmost index of the node (for traversing back to it) and
-- the hash to search by content address.
type FetchRequest k = (Maybe k, Hash256)

-- | Inclusive range query based on key type.
data Range k
  = LessThanEq k
  | Between k k
  | GreaterThenEq k

-- -----------------------------------------------------------------------

-- | Given the value of a root node, create a PartialTree to track it.
newPartialFromRoot :: TreeNode k v -> PartialTree k v

newPartialFromRoot (NodeLeaf leaves) =
  PartialTree (Just $ Completed $ CompletedLeaf leaves)

newPartialFromRoot (NodeIndex (Index keys hashes) hh) =
  PartialTree (Just $ IncompleteIndex (Index keys hashesWithRefs) hh)
  where
    hashesWithRefs = fmap (\h -> (h, MissingNode $ Index mempty mempty)) hashes


-- | Given a range (from, to), calculate all the nodes we know we'd need to
-- fetch to make progress on fully syncing the range.
--
-- Returns an empty set when the range is fully synced.
ensureRange :: forall k v. Ord k
            => Range k -> PartialTree k v -> Seq (FetchRequest k)
ensureRange _     (PartialTree Nothing)     = Empty
ensureRange range (PartialTree (Just root)) = scan range root
  where
    scan :: Range k -> PartialTreeNode k v -> Seq (FetchRequest k)
    scan _ (Completed _)               = Empty
    scan range (IncompleteIndex idx _) =
      join $ fmap (checkNode range) $ reqAlignedNodes $ filterNodes range idx

    -- We need to walk across the indexes that match the
    filterNodes :: Range k -> Index k (Hash256, Subref k v)
                -> Index k (Hash256, Subref k v)
    filterNodes (LessThanEq k)    = removeGreaterThan k
    filterNodes (Between lt gt)   = removeGreaterThan lt . removeLessThan gt
    filterNodes (GreaterThenEq k) = removeLessThan k

    checkNode :: Range k -> (Maybe k, (Hash256, Subref k v))
              -> Seq (FetchRequest k)
    checkNode _ (_, (_, Complete _))              = Empty
    checkNode _ (key, (hash, MissingNode _))      = Q.singleton (key, hash)
    checkNode range (key, (hash, PartialNode pn)) = scan range pn

    reqAlignedNodes :: Index k (Hash256, Subref k v)
                    -> Seq (Maybe k, (Hash256, Subref k v))
    reqAlignedNodes (Index keys vals) = Q.zip prepareKeys vals
      where
        prepareKeys = (fmap Just keys) |> Nothing

-- | Attempts to lookup a piece of data. Returns either a request for another
-- node in the tree or a definitive answer of whether the key is there or not.
lookup :: Ord k
       => k
       -> PartialTree k v
       -> IO (Either (FetchRequest k) (Maybe v))

lookup key (PartialTree Nothing)     = pure $ Right $ Nothing

lookup key (PartialTree (Just node)) = lookupInPartialTreeNode node
  where
    lookupInPartialTreeNode (Completed completedSync) =
      lookupInCompleted completedSync
    lookupInPartialTreeNode (IncompleteIndex idx hh) =
      lookupInIncomplete idx hh

    lookupInSubref _ _ (Complete cs)        = lookupInCompleted cs
    lookupInSubref key hash (MissingNode _) = pure $ Left $ (key, hash)
    lookupInSubref _ _ (PartialNode ptn)    = lookupInPartialTreeNode ptn

    lookupInIncomplete idx hitchhikers =
      case findInHitchhikers key hitchhikers of
        Just v  -> pure $ Right $ Just v
        Nothing ->
          let (closestKey, (hash, subref)) = findSubnodeByKey key idx
          in lookupInSubref closestKey hash subref

    lookupInCompleted (CompletedIndex idx hitchhikers) =
      case findInHitchhikers key hitchhikers of
        Just v  -> pure $ Right $ Just v
        Nothing -> let (_, (_, child)) = findSubnodeByKey key idx in
          lookupInCompleted child
    lookupInCompleted (CompletedLeaf leaves) =
      pure $ Right $ findInLeaves key leaves


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


-- | We have received a piece requested. We now insert that back into the
-- structure, reconstituting
fetched :: FetchRequest k
        -> TreeNode k v
        -> PartialTreeNode k v
        -> PartialTreeNode k v
fetched (k, hash) fetched tree =
  -- What does this do? First we must navigate downward to the place in the
  -- tree where we need to perform the completion, then we need to

  undefined
