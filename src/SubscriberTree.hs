module SubscriberTree (SubscriberTree(..),
                       FetchRequest(..),
                       newSubscriberFromRoot,
                       updateRoot,
                       ensureRange,
                       SubscriberTree.lookup,
                       fetched
                      ) where

import           Control.Monad
import           Data.Hashable
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Data.Set      (Set)
import           Index
import           Types
import           Utils

import qualified Data.Map      as M
import qualified Data.Sequence as Q

-- A SubscriberTree is a partial mirror of a FullTree. SubscriberTree is meant
-- to replicate as much structure as possible from a FullTree and be updatable
-- when the root of the FullTree changes.


-- The server side tree is a set of hashes, but the client side tree needs to
-- keep track of ranges of the tree that it already has.

data SubscriberTree k v = SubscriberTree (Maybe (SubscriberTreeNode k v))
  deriving (Show)

type PartialIndex k v = Index k (Hash256, SubscriberTreeNode k v)

-- -----------------------------------------------------------------------

-- | The toplevel of a partially synced tree. All value nodes keep track of
-- their own Hash256.
data SubscriberTreeNode k v
  -- An index node that may have holes in it.
  = PartialIndex Hash256 (Index k (SubscriberTreeNode k v)) (Hitchhikers k v)

  -- | We don't have a copy of this node. We store instead the part of the
  -- index that covered this range in hopes of reconstituting grandchild nodes
  -- in the future.
  | MissingNode Hash256 (Maybe (Index k (SubscriberTreeNode k v)))

  -- All leaf nodes are complete.
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

-- | Given the value of a root node, create a SubscriberTree to track it.
newSubscriberFromRoot :: Maybe Hash256 -> SubscriberTree k v
newSubscriberFromRoot Nothing = SubscriberTree Nothing
newSubscriberFromRoot (Just hash) =
  SubscriberTree (Just $ MissingNode hash Nothing)


-- | We have received a new root hash for the tree we are
-- monitoring. Immediately apply the new root node hash while keeping the old
-- index.
updateRoot :: (Maybe Hash256) -> SubscriberTree k v -> SubscriberTree k v
updateRoot Nothing _ = SubscriberTree Nothing
updateRoot (Just newHash) oldTree =
  SubscriberTree (Just $ MissingNode newHash $ partialTreeToIndex oldTree)

partialTreeToIndex :: SubscriberTree k v
                   -> Maybe (Index k (SubscriberTreeNode k v))
partialTreeToIndex (SubscriberTree ptn) = fmap singletonIndex ptn

-- | Given a range (from, to), calculate all the nodes we know we'd need to
-- fetch to make progress on fully syncing the range.
--
-- Returns an empty set when the range is fully synced.
ensureRange :: forall k v. Ord k
            => Range k -> SubscriberTree k v -> Seq (FetchRequest k)
ensureRange _     (SubscriberTree Nothing)     = Empty
ensureRange range (SubscriberTree (Just root)) = scan range Nothing root
  where
    scan _     _   (CompletedLeaf _ _)             = Empty
    scan range loc (MissingNode hash _)      = Q.singleton (loc, hash)
    scan range _   (PartialIndex _ idx _) =
      join $ fmap (\(k,v) -> scan range k v) $ indexPairs $
                    filterNodes range idx

    filterNodes (LessThanEq k)    = removeGreaterThan k
    filterNodes (Between lt gt)   = removeGreaterThan lt . removeLessThan gt
    filterNodes (GreaterThenEq k) = removeLessThan k

-- | Attempts to lookup a piece of data. Returns either a request for another
-- node in the tree or a definitive answer of whether the key is there or not.
lookup :: (Show k, Show v, Ord k)
       => k
       -> SubscriberTree k v
       -> Either (FetchRequest k) (Maybe v)

lookup key (SubscriberTree Nothing)     = Right $ Nothing

lookup key (SubscriberTree (Just node)) = lookupIn node
  where
    lookupIn (PartialIndex hash idx hitchhikers) =
      case findInHitchhikers key hitchhikers of
        Just v  -> Right $ Just v
        Nothing -> lookupIn $ findSubnodeByKey key idx
    lookupIn (MissingNode hash idx) = Left $ (Just key, hash)
    lookupIn (CompletedLeaf _ leaves) =
      Right $ findInLeaves key leaves

-- | We have received a piece requested. We now insert that back into the
-- structure, reconstituting as much of the tree as we can if the MissingNode
-- had
fetched :: forall k v. (Ord k, Show k, Show v)
        => FetchRequest k
        -> PublishTreeNode k v
        -> SubscriberTree k v
        -> SubscriberTree k v
fetched _ _ (SubscriberTree Nothing) = SubscriberTree Nothing
fetched (fetchK, hash) fetched (SubscriberTree (Just tree))
  = SubscriberTree $ Just $ findTarget tree
  -- What does this do? First we must navigate downward to the place in the
  -- tree where we need to perform the completion, then we need to
  where
    findTarget :: SubscriberTreeNode k v -> SubscriberTreeNode k v
    -- Navigate through the tree to the node containing the MissingNode subref.
    findTarget ii@(PartialIndex nodeHash idx hh) =
      -- Find the place in the index where we could have (k, hash) be a thing
      -- that needs to be replaced.
      PartialIndex nodeHash newIdx hh
      where
        newIdx = mapSubnodeByLoc findTarget fetchK idx

    -- Don't navigate into completed areas of the tree.
    findTarget c@(CompletedLeaf h _) = c

    findTarget mn@(MissingNode nodeHash prevIndex)
      | hash == nodeHash = case fetched of
          PublishNodeLeaf leafVector  -> CompletedLeaf nodeHash leafVector
          PublishNodeIndex hashIdx hh ->
            PartialIndex nodeHash (completeSubnodes hashIdx) hh
      | otherwise = error $
                    "MISSING NODE HASH MISMATCH: " ++ (show hash) ++ ", " ++
                    (show nodeHash)
      where
        -- We changed a MissingNode to an IncompleteIndex. We now have to take
        -- the previous Index from the MissingNode and try to use it to
        -- complete each node in the
        --
        -- We do this recursively because nodes that we would be able to
        -- compelte might have moved downwards.
        completeSubnodes newIdx =
          mapIndexWithLoc (completeFromPrevious prevIndex) newIdx

        -- Given a location and a hash, we return a node. Either a MissingNode
        -- with a restricted prevIndex if there's nothing in `prevIndex` to
        -- complete with, or
        completeFromPrevious :: Maybe (Index k (SubscriberTreeNode k v))
                             -> (Maybe k, Maybe k, Hash256)
                             -> SubscriberTreeNode k v

        completeFromPrevious Nothing (_, _, targetHash) =
          MissingNode targetHash Nothing

        completeFromPrevious (Just prevIndex) (prev, next, targetHash) =
          -- We have an index here, and we want to recursively descend through
          -- it to find targetHash.
          case findHashInIndex (next, targetHash) prevIndex of
            -- We couldn't complete this node. Return a narrowed MissingNode.
            Nothing ->
              MissingNode targetHash $ Just $ narrowIndex prev next prevIndex
            Just node -> node


-- Given a location to follow and a hash, try to find the hash in a given
-- index, returning the node if found.
findHashInIndex :: (Show k, Show v, Ord k)
                => (Maybe k, Hash256)
                -> Index k (SubscriberTreeNode k v)
                -> Maybe (SubscriberTreeNode k v)
findHashInIndex (left, hash) index =
  case getSubnodeByLoc left index of
    pi@(PartialIndex piHash subindex _)
      | hash == piHash -> Just pi
      | otherwise      -> findHashInIndex (left, hash) subindex
    mn@(MissingNode mnHash _)
      | hash == mnHash -> Just mn
      | otherwise      -> Nothing
    cl@(CompletedLeaf clHash _)
      | hash == clHash -> Just cl
      | otherwise      -> Nothing

narrowIndex :: Ord k
            => Maybe k
            -> Maybe k
            -> Index k (SubscriberTreeNode k v)
            -> Index k (SubscriberTreeNode k v)
narrowIndex prev next idx = rightNarrowed
  where
    leftNarrowed = case prev of
      Nothing -> idx
      Just k  -> removeLessThan k idx

    rightNarrowed = case next of
      Nothing -> leftNarrowed
      Just k  -> removeGreaterThan k leftNarrowed
