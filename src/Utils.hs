module Utils where

import           ClassyPrelude

import           Data.Bits            (shiftR)

import           Impl.Types
import           Types

import           Data.Primitive.Array
import           Data.Sorted
import           Data.Sorted.Row
import           Data.Sorted.Set
import           Data.Sorted.Types

import qualified Control.Arrow        as Arrow
import qualified Data.Map.Strict      as M
import qualified Data.Set             as S

-- Vector search -------------------------------------------------------------

-- Binary search that finds the lower bound index where an a could be inserted.
vBinarySearchLower :: (a -> Ordering) -> Row a -> Int
vBinarySearchLower cmp v = go 0 (length v)
  where
    go !l !u
      | u <= l = l
      | otherwise = let !k = (u + l) `shiftR` 1
                        !x = v ! k
                    in case cmp x of
                          GT -> go (k+1) u
                          _  -> go l k

vBinarySearchLowerEq :: (a -> Ordering) -> Row a -> Int
vBinarySearchLowerEq cmp v = go 0 (length v)
  where
    go !l !u
      | u <= l = l
      | otherwise = let !k = (u + l) `shiftR` 1
                        !x = v ! k
                    in case cmp x of
                          LT -> go l k
                          EQ -> (k + 1)
                          _  -> go (k+1) u

-- Index transformation ------------------------------------------------------

-- | Find the val for recursing downward.
findSubnodeByKey :: Ord k => k -> TreeIndex k v -> v
findSubnodeByKey key i@(TreeIndex keys vals) = vals ! n
  where
    n = vBinarySearchLowerEq (compare key) keys

getLocIdx :: Ord k => Maybe k -> Row k -> Int
getLocIdx Nothing _  = 0
getLocIdx (Just x) a = vBinarySearchLowerEq (compare x) a

-- | Apply fun to the node whose rightmost key is the given KeyLoc.
mapSubnodeByLoc :: Ord k => (v -> v) -> Maybe k -> TreeIndex k v
                -> TreeIndex k v

mapSubnodeByLoc fun loc (TreeIndex keys vals) =
  TreeIndex keys $ rowPut idx (fun (vals ! idx)) vals
  where
    idx = getLocIdx loc keys

-- | Get the subnode from an index by location.
getSubnodeByLoc :: Ord k => Maybe k -> TreeIndex k v -> v
getSubnodeByLoc loc (TreeIndex keys vals) = vals ! idx
  where
    idx = getLocIdx loc keys

-- | Return the index in a form where the leftmost key is returned with each
-- vector, or Nothing at the beginning.
indexPairs :: TreeIndex k v -> [(Maybe k, v)]
indexPairs (TreeIndex keys vals) =
  zip (Nothing : (fmap Just $ toList keys)) (toList vals)

-- | Returns the index with the left key and the right key to every node.
indexTriples :: TreeIndex k v -> [(Maybe k, Maybe k, v)]
indexTriples (TreeIndex keys vals) =
  let keyList = toList keys
  in zip3 (Nothing : (fmap Just keyList))
          ((fmap Just keyList) ++ [Nothing])
          (toList vals)

mapIndexWithLoc :: ((Maybe k, Maybe k, v) -> u) -> TreeIndex k v
                -> TreeIndex k u
mapIndexWithLoc fun idx@(TreeIndex keys _) =
  TreeIndex keys $ arrayFromList $ map fun $ indexTriples idx

-- Limits the index to nodes that contain values greater than a value
removeGreaterThan :: Ord k => k -> TreeIndex k v -> TreeIndex k v
removeGreaterThan key (TreeIndex keys vals) =
  TreeIndex (rowTake n keys) (rowTake (n+1) vals)
  where
    n = vBinarySearchLower (compare key) keys

removeLessThan :: Ord k => k -> TreeIndex k v -> TreeIndex k v
removeLessThan key (TreeIndex keys vals) =
  TreeIndex (rowDrop n keys) (rowDrop n vals)
  where
    n = vBinarySearchLower (compare key) keys

-- Other ---------------------------------------------------------------------

concatUnzip :: [(Row a, Row b)] -> (Row a, Row b)
concatUnzip = (concat Arrow.*** concat) . unzip

mapSetToList :: Map k (ArraySet v) -> [(k, v)]
mapSetToList = fixup . M.toList
  where
    fixup = join . map setToPair
    setToPair (k, s) = map (\v -> (k, v)) $ ssetToAscList s
