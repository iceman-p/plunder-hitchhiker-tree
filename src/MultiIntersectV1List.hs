{-# OPTIONS_GHC -w   #-}
module MultiIntersectV1List where

import           Prelude

import           Data.List     (findIndices)
import           Data.Maybe    (catMaybes, fromMaybe, isNothing)
import           Data.Vector   ((!))
import           Debug.Trace

import           HitchhikerSet
import           Impl.Tree
import           Impl.Types
import           Types

import           Data.Sorted

import qualified Data.List     as L
import qualified Data.Set      as S
import qualified Data.Vector   as V

-- ---------------------------------------------------------------------------

-- Copied with GHC.Utils.Misc for now.
partitionWith :: (a -> Either b c) -> [a] -> ([b], [c])
partitionWith _ [] = ([],[])
partitionWith f (x:xs) = case f x of
                         Left  b -> (b:bs, cs)
                         Right c -> (bs, c:cs)
    where (bs,cs) = partitionWith f xs

-- ---------------------------------------------------------------------------

-- Given an optional (min, max) to constrain everything to,

checkOverlap :: Ord k => Maybe (k, k) -> [[k]] -> (Bool, Maybe k)
checkOverlap concreteSet ranges
  | Prelude.null ranges = (False, Nothing)
  | Prelude.null smallestList = (True, Nothing)
  | otherwise = (matches, Just smallestMax)
  where
    largestMin  = maximum $ map getMin ranges
    smallestList = catMaybes $ map getMax ranges
    smallestMax = minimum $ smallestList

    matches = case concreteSet of
      Nothing -> smallestMax > largestMin
      Just (boundingMin, boundingMax) ->
        (smallestMax > largestMin) &&
        (smallestMax > boundingMin) &&
        (largestMin <= boundingMax)

    getMin :: [k] -> k
    getMin [k]       = k
    getMin (min:_:_) = min

    getMax :: [k] -> Maybe k
    getMax [_]       = Nothing
    getMax (_:max:_) = Just max

findIndexOverlap :: forall k v. (Show k, Ord k)
                 => Maybe (k, k)
                 -> [[k]]
                 -> [[v]]
                 -> [[v]]
findIndexOverlap constraint =
  -- trace ("findIndexOverlap: " ++ show constraint ++ ", indexVals: "
  --      ++ show indexVals) $
  loop
  where
    loop :: [[k]] -> [[v]] -> [[v]]
    loop ranges vals =
      case checkOverlap constraint ranges of
        (True, Nothing)         -> (map head vals):[]
        (True, Just smallestMax)  ->
          (map head vals):(advanceBySmallestMax smallestMax ranges vals)
        (False, Just smallestMax) ->
          advanceBySmallestMax smallestMax ranges vals

    advanceBySmallestMax smallestMax ranges vals =
      loop nuRanges nuVals
      where
        (nuRanges, nuVals) = mapRanges ranges vals
        mapRanges [] _ = ([], [])
        mapRanges _ [] = ([], [])
        mapRanges (r:rs) (i:is) =
          let (rx, ix) = mapRanges rs is
          in case r of
            (_:x:_) | x == smallestMax -> ((nextRange r):rx, (tail i):ix)
            _                          -> (r:rx, i:ix)

        nextRange (min:max:[]) = (max:[])
        nextRange x@(_:[])     = x
        nextRange (_:xs)       = xs

partitionSetNodes :: [HitchhikerSetNode k]
                  -> ( [TreeIndex k (HitchhikerSetNode k)]
                     , [ArraySet k])
partitionSetNodes = partitionWith match
  where
    match (HitchhikerSetNodeIndex ti hh)
      | L.null hh = Left ti
      | otherwise = error "Can't work with hitchhikers"
    match (HitchhikerSetNodeLeaf s)     = Right s

findMinMax :: Ord k => ArraySet k -> (k, k)
findMinMax s = (ssetFindMin s, ssetFindMax s)

nuFindImpl :: forall k. (Show k, Ord k)
           => Maybe (ArraySet k)
           -> [HitchhikerSetNode k]
           -> [ArraySet k]
nuFindImpl inputConstraint inputNodes = output
  where
    (idxNodes, leaves) = partitionSetNodes inputNodes

    -- The constraint is a set which is made up of the intersection of all leaf
    -- nodes we've seen so far. This happens when the tree depth of the
    -- different `HitchhikerSet`s differ: a short tree's leaves keep getting
    -- compared to the remaining trees we keep descending.
    constraint :: Maybe (ArraySet k)
    constraint = case (leaves, inputConstraint) of
      ([], Nothing)  -> Nothing
      (xs, Nothing)  -> Just $ foldl1 ssetIntersection xs
      ([], Just set) -> Just set
      (xs, Just set) -> Just $ foldl ssetIntersection set xs

    -- For every item in idxes, we want to turn that index into a list of lists
    -- which we can traverse in parallel.
    mkRangeAndVals (TreeIndex ks vals) =
      ( [getLeftmostValue $ vals ! 0] ++ V.toList ks
      , V.toList vals)
    (idxRanges, idxVals) = unzip $ map mkRangeAndVals idxNodes

    -- Each list in this list are indexes into idxVals as parallel arrays for
    -- the next step.
    idxCandidates :: [[HitchhikerSetNode k]]
    idxCandidates = case (constraint, idxRanges) of
      -- If we had leaves and/or an input constraint which are impossible to
      -- satisfy, just bail.
      (Just s, _) | ssetIsEmpty s -> []
      (_, [])                -> []
      (const, ranges)        ->
        findIndexOverlap (fmap findMinMax const) ranges idxVals
        -- let x =
        -- in trace ("Result: " ++ show x) x

    output = case (idxCandidates, constraint) of
      ([], Nothing)              -> []
      (_, Just set) | ssetIsEmpty set -> []
      ([], Just set)             -> [set]
      (xs, constraint)           ->
        concat $ map (nuFindImpl constraint) xs

-- Toplevel which flushes things downward.
nuIntersect :: forall k. (Show k, Ord k)
            => [HitchhikerSet k] -> [ArraySet k]
nuIntersect [] = []
--nuIntersect [a] = [a]  -- TODO: Handle this.
nuIntersect sets = output
  where
    mybNodes :: [Maybe (HitchhikerSetNode k)]
    mybNodes = map rawNode sets

    output = case (or $ map isNothing mybNodes) of
      True -> []
      False -> nuFindImpl Nothing $ map (flushDownwards hhSetTF) $ catMaybes mybNodes
