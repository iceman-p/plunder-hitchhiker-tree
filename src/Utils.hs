module Utils where

import           ClassyPrelude

import           Data.Bits       (shiftR)
import           Data.Vector     (Vector)

import           Impl.Types
import           Types

import           Data.Sorted
import           Data.Sorted.Set

import qualified Control.Arrow   as Arrow
import qualified Data.Map.Strict as M
import qualified Data.Set        as S
import qualified Data.Vector     as V

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
findSubnodeByKey :: Ord k => k -> TreeIndex k v -> v
findSubnodeByKey key i@(TreeIndex keys vals) = vals V.! n
  where
    n = vBinarySearchLowerEq (compare key) keys

getLocIdx :: Ord k => Maybe k -> Vector k -> Int
getLocIdx Nothing _  = 0
getLocIdx (Just x) a = vBinarySearchLowerEq (compare x) a

-- | Apply fun to the node whose rightmost key is the given KeyLoc.
mapSubnodeByLoc :: Ord k => (v -> v) -> Maybe k -> TreeIndex k v
                -> TreeIndex k v

mapSubnodeByLoc fun loc (TreeIndex keys vals) =
  TreeIndex keys $ V.update vals $ V.fromList [(idx, fun (vals V.! idx))]
  where
    idx = getLocIdx loc keys

-- | Get the subnode from an index by location.
getSubnodeByLoc :: Ord k => Maybe k -> TreeIndex k v -> v
getSubnodeByLoc loc (TreeIndex keys vals) = vals V.! idx
  where
    idx = getLocIdx loc keys

-- | Return the index in a form where the leftmost key is returned with each
-- vector, or Nothing at the beginning.
indexPairs :: TreeIndex k v -> [(Maybe k, v)]
indexPairs (TreeIndex keys vals) =
  zip (Nothing : (fmap Just $ V.toList keys)) (V.toList vals)

-- | Returns the index with the left key and the right key to every node.
indexTriples :: TreeIndex k v -> [(Maybe k, Maybe k, v)]
indexTriples (TreeIndex keys vals) =
  let keyList = V.toList keys
  in zip3 (Nothing : (fmap Just keyList))
          ((fmap Just keyList) ++ [Nothing])
          (V.toList vals)

mapIndexWithLoc :: ((Maybe k, Maybe k, v) -> u) -> TreeIndex k v
                -> TreeIndex k u
mapIndexWithLoc fun idx@(TreeIndex keys _) =
  TreeIndex keys $ V.fromList $ map fun $ indexTriples idx

-- Limits the index to nodes that contain values greater than a value
removeGreaterThan :: Ord k => k -> TreeIndex k v -> TreeIndex k v
removeGreaterThan key (TreeIndex keys vals) =
  TreeIndex (V.take n keys) (V.take (n+1) vals)
  where
    n = vBinarySearchLower (compare key) keys

removeLessThan :: Ord k => k -> TreeIndex k v -> TreeIndex k v
removeLessThan key (TreeIndex keys vals) =
  TreeIndex (V.drop n keys) (V.drop n vals)
  where
    n = vBinarySearchLower (compare key) keys

-- Other ---------------------------------------------------------------------

concatUnzip :: [(Vector a, Vector b)] -> (Vector a, Vector b)
concatUnzip = (V.concat Arrow.*** V.concat) . unzip

mapSetToList :: Map k (ArraySet v) -> [(k, v)]
mapSetToList = fixup . M.toList
  where
    fixup = join . map setToPair
    setToPair (k, s) = map (\v -> (k, v)) $ ssetToAscList s
