module MultiIntersectV3Naive where

import           ClassyPrelude

import           Debug.Trace

import           HitchhikerSet
import           Impl.Tree
import           Types

import           Data.Sorted

import qualified Data.Set      as S

-- The strategy in this experiment is to just make a naive simple `[ArraySet k]
-- -> [ArraySet k] -> [ArraySet k]` intersection primitive.

setlistIntersect :: (Show k, Ord k)
                 => [ArraySet k] -> [ArraySet k] -> [ArraySet k]
setlistIntersect a  [] = []
setlistIntersect [] b  = []
setlistIntersect ao@(a:as) bo@(b:bs) =
  let aMin = ssetFindMin a
      aMax = ssetFindMax a
      bMin = ssetFindMin b
      bMax = ssetFindMax b
      overlap = aMin <= bMax && bMin <= aMax

      i = ssetIntersection a b

      rest = if | aMax == bMax -> setlistIntersect as bs
                | aMax > bMax  -> setlistIntersect ao bs
                | otherwise    -> setlistIntersect as bo

  in if overlap && (not $ ssetIsEmpty i)
     then (i):rest
     else rest

-- The dumbest, most naive intersection possible.
naiveIntersection :: (Show k, Ord k)
                  => HitchhikerSet k -> HitchhikerSet k -> [ArraySet k]
naiveIntersection n@(HITCHHIKERSET _ Nothing) _ = []
naiveIntersection _ n@(HITCHHIKERSET _ Nothing) = []
naiveIntersection (HITCHHIKERSET conf (Just a)) (HITCHHIKERSET _ (Just b)) =
  setlistIntersect as bs
  where
    as = getLeafList hhSetTF a
    bs = getLeafList hhSetTF b

-- Easier to type in ghci.
slTest :: (Show k, Ord k) => [[k]] -> [[k]] -> [ArraySet k]
slTest as bs = setlistIntersect (map ssetFromList as) (map ssetFromList bs)

-- -----------------------------------------------------------------------
