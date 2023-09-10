module MultiIntersectV2Vector where

import           Prelude

import           Data.List     (findIndices)
import           Data.Maybe    (catMaybes, fromMaybe, isNothing)
import           Data.Vector   (Vector (..), (!))
import           Debug.Trace

import           HitchhikerSet
import           Impl.Tree
import           Impl.Types
import           Types

import           Data.Sorted

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

checkOverlap :: Ord k => Maybe (k, k) -> Vector [k] -> (Bool, Maybe k)
checkOverlap concreteSet ranges
  | V.null ranges = (False, Nothing)
  | V.null smallestList = (True, Nothing)
  | otherwise = (matches, Just smallestMax)
  where
    largestMin  = V.maximum $ V.map getMin ranges
    smallestList = V.catMaybes $ V.map getMax ranges
    smallestMax = V.minimum $ smallestList

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
                 -> Vector [k]
                 -> Vector [v]
                 -> Vector [v]
findIndexOverlap constraint ranges vals =
  -- trace ("findIndexOverlap: " ++ show constraint ++ ", indexVals: "
  --      ++ show indexVals) $
  V.unfoldr step (ranges, vals)
  where
    step :: (Vector [k], Vector [v])
         -> Maybe ([v], (Vector [k], Vector [v]))
    step (ranges, vals)
      | V.null ranges = Nothing
      | V.null vals = Nothing
      | otherwise = case checkOverlap constraint ranges of
          (True, Nothing)          ->
              Just (V.toList $ V.map head vals,
                    (mempty, mempty))
          (True, Just smallestMax) ->
              Just (V.toList $ V.map head vals,
                    advanceBySmallestMax smallestMax ranges vals)
          (False, Just smallestMax) ->
              step $ advanceBySmallestMax smallestMax ranges vals

    advanceBySmallestMax :: k -> Vector [k] -> Vector [v]
                         -> (Vector [k], Vector [v])
    advanceBySmallestMax smallestMax ranges vals =
      V.unzip $ V.map check $ V.zip ranges vals
      where
        check (r, i) = case r of
            (_:x:_) | x == smallestMax -> ((nextRange r), (tail i))
            _                          -> (r, i)

        nextRange (min:max:[]) = (max:[])
        nextRange x@(_:[])     = x
        nextRange (_:xs)       = xs

partitionSetNodes :: Vector (HitchhikerSetNode k)
                  -> ( Vector (TreeIndex k (HitchhikerSetNode k))
                     , Vector (ArraySet k))
partitionSetNodes = V.partitionWith match
  where
    match (HitchhikerSetNodeIndex ti hh)
      | ssetIsEmpty hh = Left ti
      | otherwise = error "Can't work with hitchhikers"
    match (HitchhikerSetNodeLeaf s)     = Right s

findMinMax :: Ord k => ArraySet k -> (k, k)
findMinMax s = (ssetFindMin s, ssetFindMax s)

nuFindImpl :: forall k. (Show k, Ord k)
           => Maybe (ArraySet k)
           -> Vector (HitchhikerSetNode k)
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
      (xs, Nothing)
        | V.null xs -> Nothing
        | otherwise -> Just $ V.foldl1 ssetIntersection xs
      (xs, Just set)
        | V.null xs -> Just set
        | otherwise -> Just $ V.foldl ssetIntersection set xs

    -- For every item in idxes, we want to turn that index into a list of lists
    -- which we can traverse in parallel.
    mkRangeAndVals (TreeIndex ks vals) =
--      V.cons (getLeftmostValue $ vals ! 0) ks
      ( [getLeftmostValue $ vals ! 0] ++ V.toList ks
      , V.toList vals)
    (idxRanges, idxVals) = V.unzip $ V.map mkRangeAndVals idxNodes

    -- Each list in this list are indexes into idxVals as parallel arrays for
    -- the next step.
    idxCandidates :: [[HitchhikerSetNode k]]
    idxCandidates = case (constraint, idxRanges) of
      -- If we had leaves and/or an input constraint which are impossible to
      -- satisfy, just bail.
      (Just s, _) | ssetIsEmpty s -> []
      (const, ranges)
        | V.null ranges      -> []
        | otherwise ->
            V.toList $ findIndexOverlap (fmap findMinMax const) ranges idxVals
        -- let x =
        -- in trace ("Result: " ++ show x) x

    output = case (idxCandidates, constraint) of
      ([], Nothing)              -> []
      (_, Just set) | ssetIsEmpty set -> []
      ([], Just set)             -> [set]
      (xs, constraint)           ->
        concat $ map (nuFindImpl constraint . V.fromList) xs

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
      False -> nuFindImpl Nothing $ V.fromList $ map (flushDownwards hhSetTF) $ catMaybes mybNodes
