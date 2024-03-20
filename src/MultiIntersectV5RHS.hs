module MultiIntersectV5RHS where

import           ClassyPrelude

import           Impl.Strict
import           Impl.Tree
import           Impl.Types
import           Types

import           Data.Sorted
import           Data.Sorted.Row
import           Data.Sorted.Types

import qualified Data.Set          as S
import qualified HitchhikerSet     as HS

-- Intuition: We can maybe reuse the tree structure for accelerated searching
-- and lazy traversal, if we grab the first set and take it's leaves and then
-- walk through each subsequent lazy set, obeying the tree structure.
--
-- This probably should be written as lazily as possible, since Sol thinks V3
-- is fast because of that.

-- TODO: When we ended on Monday evening, this was sometimes faster, sometimes
-- not than the naive implementation, since this has higher variance.

-- Open questions:
--
-- - [ ] Are the sets just too small? Are we just doing singleton sets all the
--       time? Test that. I wonder if we're just dealing with a bunch of
--       singleton sets. Take the tag console, and instrument it to show the
--       size of all sets on each additional tag.
--
--       The hypothesis is that we are degenerating down to a linear scan.
--

{-# INLINE idx #-}
idx i v = unsafeIndex v i

step :: forall k. (Show k, Ord k)
     => HitchhikerSetNode k
     -> [ArraySet k]
     -> ([ArraySet k] -> [ArraySet k])
     -> [ArraySet k]
step _ [] _ = []

step orig@(HitchhikerSetNodeIndex (TreeIndex ks vs) _)
     input
     cont = it 0 input
  where
    l = length ks

    it :: Int -> [ArraySet k] -> [ArraySet k]
    it i [] = []
    it i rall@(r:rs) =
      if | i == 0                         -> step (idx 0 vs) rall (it 1)
         | i == l                         -> step (idx l vs) rall cont
         | ssetFindMax r < idx (i - 1) ks -> it i rs
         | ssetFindMin r >= idx i ks      -> it (i+1) rall
         | otherwise                      -> step (idx i vs) rall (it (i + 1))

step hl@(HitchhikerSetNodeLeaf leaf) a continuation = loop a
  where
    bMin = ssetFindMin leaf
    bMax = ssetFindMax leaf

    loop [] = []
    loop ao@(a:as) =
      let aMin = ssetFindMin a
          aMax = ssetFindMax a
          overlap = aMin <= bMax && bMin <= aMax

          intersection = ssetIntersection a leaf

          rest = case aMax `compare` bMax of
            EQ -> continuation as
            GT -> continuation ao
            LT -> loop as
      in if overlap && (not $ ssetIsEmpty intersection)
            then intersection:rest
            else rest


exampleTree :: HitchhikerSetNode Int
exampleTree = HitchhikerSetNodeIndex (TreeIndex keys vals) emptyCL
  where
    keys = fromList [5, 9]
    vals = fromList [
      HitchhikerSetNodeLeaf $ ssetFromList [1, 2],
      HitchhikerSetNodeLeaf $ ssetFromList [5, 7],
      HitchhikerSetNodeLeaf $ ssetFromList [9, 10]]

exampleList :: [ArraySet Int]
exampleList = [ssetFromList [2, 9], ssetFromList [13]]

mkStepExample :: [ArraySet Int]
mkStepExample =
  let x = force $ step exampleTree exampleList \a -> []
  in x

rhsIntersection :: forall k. (NFData k, Ord k, Show k)
                => [HitchhikerSet k] -> [ArraySet k]
rhsIntersection [] = []
rhsIntersection [x] = case HS.rawNode x of
  SNothing   -> []
  SJust node -> getLeafList HS.hhSetTF node
rhsIntersection sets@((HITCHHIKERSET config _):_) =
  let byWeight a b = HS.weightEstimate a `compare` HS.weightEstimate b
      orderedByWeight = sortBy byWeight sets
      nodesByWeight = sCatMaybes $ map HS.rawNode orderedByWeight
      -- minItems = minLeafItems config `div` 2
  in if any HS.null sets
     then []
     else case nodesByWeight of
       []     -> []
       [x]    -> getLeafList HS.hhSetTF x
       (x:xs) ->
         let a :: [ArraySet k] = getLeafList HS.hhSetTF x
             bs :: [HitchhikerSetNode k] = map (flushDownwards HS.hhSetTF) xs
         in foldl' (\a b -> force $ step b a \c -> []) a bs

mkHHSet :: [Int] -> HitchhikerSet Int
mkHHSet = go (HS.empty twoThreeConfig)
  where
    go h []     = h
    go h (k:ks) = go (HS.insert k h) ks


mkTestA = [ssetFromList [24]] == (rhsIntersection $ map mkHHSet [
    [0, 24],
    [1, 24, 0, 2],
    [26,24,1,25,1,-1,-2],
    [0,24]])

mkTestB = rhsIntersection $ map mkHHSet [
    [28],
    [28],
    [29,0,28,30,0,1,2],
    [28]]

mkTestC = rhsIntersection $ map mkHHSet [
      [0,-41],
      [-41,0],
      [0,-41],
      [0,-42,-43,1,-41,0,-1]]
