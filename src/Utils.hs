module Utils where

import           Data.Sequence (Seq)
import           Types

import qualified Data.Sequence as Q


-- Seq utils -----------------------------------------------------------------

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
findSubnodeByKey :: Ord k => k -> Index k v -> (Maybe k, v)
findSubnodeByKey key (Index keys vals) = (rightKey, subnode)
  where
    (leftKeys, rightKeys) = Q.spanl (<=key) keys
    n = Q.length leftKeys
    (_, valAndRightVals) = Q.splitAt n vals
    subnode = case qUncons valAndRightVals of
      Nothing       -> error "Impossible in findSubnodeByKey"
      Just (val, _) -> val
    rightKey = case qUncons rightKeys of
      Nothing       -> Nothing
      Just (key, _) -> Just key


-- Limits the index to nodes that contain values greater than a value
removeGreaterThan :: Ord k => k -> Index k v -> Index k v
removeGreaterThan key (Index keys vals) = Index prunedKeys prunedVals
  where
    (prunedKeys, _) = Q.spanl (<= key) keys
    n = Q.length prunedKeys
    (prunedVals, _) = Q.splitAt (n + 1) vals

removeLessThan :: Ord k => k -> Index k v -> Index k v
removeLessThan key (Index keys vals) = Index prunedKeys prunedVals
  where
    (dropped, prunedKeys) = Q.spanl (key >=) keys
    n = Q.length dropped
    (_, prunedVals) = Q.splitAt n vals
