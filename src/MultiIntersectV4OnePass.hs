{-# OPTIONS_GHC -w   #-}
module MultiIntersectV4OnePass where

import           Prelude

import           Data.Set        (Set (..))
import           Debug.Trace

import           Data.Maybe      (catMaybes, fromMaybe, isNothing)

import           HitchhikerSet
import           Impl.Strict
import           Impl.Tree
import           Types

import           Data.Sorted
import           Data.Sorted.Set

-- Inspired by V2: tries to use checkOverlap to detect multiple overlaps at
-- once.
--
-- Inspired by V3: tries to do everything in one pass.

--
-- AND ITS SLOWER. 2x slower over V3.
--
-- Damnit.
--
-- It feels like trying to do ANYTHING smart breaks fucking everything. Having
-- to do ANY extra traversals kills performance.


checkOverlap :: Ord k => [[ArraySet k]] -> (Bool, [[ArraySet k]])
checkOverlap ranges
  | Prelude.null ranges = (False, [])
  | otherwise = (matches, fromMaybe [] $ mapM advanceItem ranges)
  where
    largestMin = maximum $ map getMin ranges
    smallestMax = minimum $ map getMax ranges
    matches = smallestMax >= largestMin

    getMin (s:_) = ssetFindMin s

    getMax (s:_) = ssetFindMax s

    advanceItem [] = Nothing
    advanceItem orig@(x:[])
      | ssetFindMax x == smallestMax = Nothing
      | otherwise = Just orig
    advanceItem orig@(x:xs)
      | ssetFindMax x == smallestMax = Just xs
      | otherwise = Just orig

fullIntersect :: (Show k, Ord k) => [[ArraySet k]] -> [ArraySet k]
fullIntersect [] = []
fullIntersect [a] = a
fullIntersect all =
  --trace ("Full intersect: " ++ show all) $
  let (matches, next) = checkOverlap all
      i = foldl1 ssetIntersection $ map head all
  in if matches && (not $ ssetIsEmpty i)
     then i:(fullIntersect next)
     else fullIntersect next


onePassIntersection :: forall k. (Show k, Ord k)
                    => [HitchhikerSet k] -> [ArraySet k]
onePassIntersection sets = output
  where
    mybNodes :: [StrictMaybe (HitchhikerSetNode k)]
    mybNodes = map rawNode sets

    output = case (or $ map sIsNothing mybNodes) of
      True -> []
      False -> fullIntersect
             $ map (getLeafList hhSetTF)
             $ sCatMaybes mybNodes

fiList :: (Show k, Ord k) => [[[k]]] -> [ArraySet k]
fiList = fullIntersect . map (map ssetFromList)

-- Vague full one pass algorithm if the above works:
--
-- 1) Across all sets, find the largestMin and smallestMax.
-- 2) Build lazy rows for each hitchhiker set clamped to those bounds.
-- 3) fullIntersect based on above.
