module Utils where

import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Debug.Trace
import           Types

import qualified Data.Sequence as Q


-- Seq utils -----------------------------------------------------------------

qHeadUnsafe :: Seq a -> a
qHeadUnsafe (first :<| _) = first
qHeadUnsafe _             = error "qHeadUnsafe"

qSortedInsert :: (Ord k) => k -> v -> Seq (k, v) -> Seq (k, v)
qSortedInsert k v s = case Q.findIndexL (\(i, _) -> k <= i) s of
  Nothing  -> s |> (k, v)
  Just idx -> case Q.lookup idx s of
    Just (curk, curv) | curk == k -> Q.update idx (k,v) s
    Just _                        -> Q.insertAt idx (k,v) s
    Nothing                       -> error "impossible"

qUncons :: Seq a -> Maybe (a, Seq a)
qUncons = \case
  (head :<| rest) -> Just (head, rest)
  Q.Empty         -> Nothing

qUnsnoc :: Seq a -> Maybe (a, Seq a)
qUnsnoc = \case
  (rest :|> head) -> Just (head, rest)
  Q.Empty         -> Nothing

-- Hitchhiker utils ----------------------------------------------------------

-- | Finds a value in the hitchhikers by scanning from the back, since
-- placement in the hitchhiker tree is an event log.
findInHitchhikers :: Eq k => k -> Hitchhikers k v -> Maybe v
findInHitchhikers key hh =
  case Q.findIndexR (\(k, v) -> k == key) hh of
    Just idx -> case Q.lookup idx hh of
      Nothing     -> Nothing -- impossible
      Just (_, v) -> Just v
    Nothing -> Nothing

findInLeaves :: Eq k => k -> LeafVector k v -> Maybe v
findInLeaves key leaves =
  -- TODO: This could be a binary search instead, since this seq is
  -- ordered.
  case Q.findIndexL (\(k, v) -> k == key) leaves of
    Just idx -> case Q.lookup idx leaves of
      Nothing     -> Nothing --impossible
      Just (_, v) -> Just v
    Nothing -> Nothing

-- Index transformation ------------------------------------------------------

-- | Find the val for recursing downward. Returns a pair of the key to the
-- right of the subnode value (or Nothing for the final value) and value.
--
-- TODO: A real version here could use binary search on keys, and O(1) position
-- lookup.
findSubnodeByKey :: (Show k, Show v, Ord k) => k -> Index k v -> v
findSubnodeByKey key i@(Index keys vals) = subnode
  where
    (leftKeys, rightKeys) = Q.spanl (<=key) keys
    n = Q.length leftKeys
    (_, valAndRightVals) = Q.splitAt n vals
    subnode = case qUncons valAndRightVals of
      Nothing       -> error "Impossible in findSubnodeByKey"
      Just (val, _) -> val

-- | Apply fun to the node whose rightmost key is the given KeyLoc.
mapSubnodeByLoc :: Ord k => (v -> v) -> Maybe k -> Index k v -> Index k v

mapSubnodeByLoc fun Nothing (Index keys vals) = Index keys $
  case vals of
    x :<| xs -> fun x <| xs
    Empty    -> Empty

mapSubnodeByLoc fun (Just k) (Index keys vals) = Index keys newVals
  where
    i = Q.length $ fst $ Q.spanl (<= k) keys
    newVals =  Q.adjust fun i vals

-- | Get the subnode from an index by location.
getSubnodeByLoc :: Ord k => Maybe k -> Index k v -> v

getSubnodeByLoc Nothing (Index keys vals) = case vals of
  x :<| xs -> x
  Empty    -> error "Impossible empty Index"

getSubnodeByLoc (Just k) (Index keys vals) = Q.index vals i
  where
    i = Q.length $ fst $ Q.spanl (<= k) keys

-- | Return the index in a form where the leftmost key is returned with each
-- vector, or Nothing at the beginning.
indexPairs :: Index k v -> Seq (Maybe k, v)
indexPairs (Index keys vals) = Q.zip (Nothing <| fmap Just keys) vals

-- | Returns the index with the left key and the right key to every node.
indexTriples :: Index k v -> Seq (Maybe k, Maybe k, v)
indexTriples (Index keys vals) =
  Q.zip3 (Nothing <| fmap Just keys) (fmap Just keys |> Nothing) vals

mapIndexWithLoc :: ((Maybe k, Maybe k, v) -> u) -> Index k v -> Index k u
mapIndexWithLoc fun idx@(Index keys _) =
  Index keys $ fmap fun $ indexTriples idx

-- Limits the index to nodes that contain values greater than a value
removeGreaterThan :: Ord k => k -> Index k v -> Index k v
removeGreaterThan key (Index keys vals) = Index prunedKeys prunedVals
  where
    (prunedKeys, _) = Q.spanl (< key) keys
    n = Q.length prunedKeys
    (prunedVals, _) = Q.splitAt (n + 1) vals

removeLessThan :: Ord k => k -> Index k v -> Index k v
removeLessThan key (Index keys vals) = Index prunedKeys prunedVals
  where
    (dropped, prunedKeys) = Q.spanl (key >=) keys
    n = Q.length dropped
    (_, prunedVals) = Q.splitAt n vals
