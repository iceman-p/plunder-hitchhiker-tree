module MultiIntersectV3Naive where

import           Prelude

import           Data.Set      (Set (..))
import           Debug.Trace

import           HitchhikerSet
import           Impl.Tree
import           Types

import qualified Data.Set      as S

-- The strategy in this experiment is to just make a naive simple `[Set k] ->
-- [Set k] -> [Set k]` intersection primitive.

setlistIntersect :: (Show k, Ord k) => [Set k] -> [Set k] -> [Set k]
setlistIntersect a  [] = []
setlistIntersect [] b  = []
setlistIntersect ao@(a:as) bo@(b:bs) =
  let aMin = S.findMin a
      aMax = S.findMax a
      bMin = S.findMin b
      bMax = S.findMax b
      overlap = aMin <= bMax && bMin <= aMax

      i = S.intersection a b

      rest = if | aMax == bMax -> setlistIntersect as bs
                | aMax > bMax  -> setlistIntersect ao bs
                | otherwise    -> setlistIntersect as bo

  in if overlap && (not $ S.null i)
     then (i):rest
     else rest

-- The dumbest, most naive intersection possible.
naiveIntersection :: (Show k, Ord k)
                  => HitchhikerSet k -> HitchhikerSet k -> [Set k]
naiveIntersection n@(HITCHHIKERSET _ Nothing) _ = []
naiveIntersection _ n@(HITCHHIKERSET _ Nothing) = []
naiveIntersection (HITCHHIKERSET conf (Just a)) (HITCHHIKERSET _ (Just b)) =
  setlistIntersect as bs
  where
    as = getLeafList hhSetTF a
    bs = getLeafList hhSetTF b

-- Easier to type in ghci.
slTest :: (Show k, Ord k) => [[k]] -> [[k]] -> [Set k]
slTest as bs = setlistIntersect (map S.fromList as) (map S.fromList bs)

-- -----------------------------------------------------------------------

