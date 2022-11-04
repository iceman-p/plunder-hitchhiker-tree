module Utils where

import           Data.Bits     (shiftR)
import           Data.Vector   (Vector)
import           Debug.Trace

import           Impl.Types
import           Types

import qualified Control.Arrow as Arrow
import qualified Data.Vector   as V

-- Vector search -------------------------------------------------------------

-- Binary search that finds the lower bound index where an a could be inserted.
vBinarySearchLower :: (a -> Ordering) -> Vector a -> Int
vBinarySearchLower cmp v = go 0 (V.length v)
  where
    go !l !u
      | u <= l = l
      | otherwise = let !k = (u + l) `shiftR` 1
                        !x = v V.! k
                    in case cmp x of
                          GT -> go (k+1) u
                          _  -> go l k

vBinarySearchLowerEq :: (a -> Ordering) -> Vector a -> Int
vBinarySearchLowerEq cmp v = go 0 (V.length v)
  where
    go !l !u
      | u <= l = l
      | otherwise = let !k = (u + l) `shiftR` 1
                        !x = v V.! k
                    in case cmp x of
                          LT -> go l k
                          EQ -> (k + 1)
                          _  -> go (k+1) u

-- Index transformation ------------------------------------------------------

-- | Find the val for recursing downward.
findSubnodeByKey :: Ord k => k -> Index k v -> v
findSubnodeByKey key i@(Index keys vals) = vals V.! n
  where
    n = vBinarySearchLowerEq (compare key) keys

getLocIdx :: Ord k => Maybe k -> Vector k -> Int
getLocIdx Nothing _  = 0
getLocIdx (Just x) a = vBinarySearchLowerEq (compare x) a

-- | Apply fun to the node whose rightmost key is the given KeyLoc.
mapSubnodeByLoc :: Ord k => (v -> v) -> Maybe k -> Index k v -> Index k v

mapSubnodeByLoc fun loc (Index keys vals) =
  Index keys $ V.update vals $ V.fromList [(idx, fun (vals V.! idx))]
  where
    idx = getLocIdx loc keys

-- | Get the subnode from an index by location.
getSubnodeByLoc :: Ord k => Maybe k -> Index k v -> v
getSubnodeByLoc loc (Index keys vals) = vals V.! idx
  where
    idx = getLocIdx loc keys

-- | Return the index in a form where the leftmost key is returned with each
-- vector, or Nothing at the beginning.
indexPairs :: Index k v -> [(Maybe k, v)]
indexPairs (Index keys vals) =
  zip (Nothing : (fmap Just $ V.toList keys)) (V.toList vals)

-- | Returns the index with the left key and the right key to every node.
indexTriples :: Index k v -> [(Maybe k, Maybe k, v)]
indexTriples (Index keys vals) =
  let keyList = V.toList keys
  in zip3 (Nothing : (fmap Just keyList))
          ((fmap Just keyList) ++ [Nothing])
          (V.toList vals)

mapIndexWithLoc :: ((Maybe k, Maybe k, v) -> u) -> Index k v -> Index k u
mapIndexWithLoc fun idx@(Index keys _) =
  Index keys $ V.fromList $ map fun $ indexTriples idx

-- Limits the index to nodes that contain values greater than a value
removeGreaterThan :: Ord k => k -> Index k v -> Index k v
removeGreaterThan key (Index keys vals) =
  Index (V.take n keys) (V.take (n+1) vals)
  where
    n = vBinarySearchLower (compare key) keys

removeLessThan :: Ord k => k -> Index k v -> Index k v
removeLessThan key (Index keys vals) = Index (V.drop n keys) (V.drop n vals)
  where
    n = vBinarySearchLower (compare key) keys

-- Other ---------------------------------------------------------------------

concatUnzip :: [(Vector a, Vector b)] -> (Vector a, Vector b)
concatUnzip = (V.concat Arrow.*** V.concat) . unzip
