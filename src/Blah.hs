module Blah where

import           Prelude

-- for listAdjust
import           Control.Arrow (second)

import           Data.List     (findIndices)
import           Data.Maybe    (catMaybes, fromMaybe, isNothing)
import           Data.Vector   ((!))
import           Debug.Trace

import           HitchhikerSet
import           Impl.Tree
import           Impl.Types
import           Types

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

-- Copied from Math.FFT.Base, modified to have the [] check.
--
-- | A generally useful list utility
listAdjust :: (a -> a) -> Int -> [a] -> [a]
listAdjust f i = uncurry (++) . second doit . splitAt i
  where
    doit []     = error "empty in listAdjust"
    doit (x:xs) = f x : xs

-- ---------------------------------------------------------------------------

-- Given an optional (min, max) to constrain everything to,

data Range k
  -- inclusive greater than k
  = END_GT k
  -- inclusive begin, exclusive end
  | RANGE k k
  deriving (Show, Eq)

checkOverlap :: Ord k => Maybe (k, k) -> [Range k] -> (Bool, [Int])
checkOverlap concreteSet ranges
  | Prelude.null ranges = (False, [])
  | Prelude.null smallestList = (True, [])
  | otherwise = (matches, toAdvance)
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

    toAdvance = findIndices isEqSmallest ranges
    isEqSmallest (END_GT _)  = False
    isEqSmallest (RANGE _ x) = x == smallestMax

    getMin :: Range k -> k
    getMin (END_GT k)    = k
    getMin (RANGE min _) = min

    getMax :: Range k -> Maybe k
    getMax (END_GT _)    = Nothing
    getMax (RANGE _ max) = Just max

findIndexOverlap :: forall k. (Show k, Ord k)
                 => Maybe (k, k)
                 -> [[k]]
                 -> [[Int]]
findIndexOverlap constraint indexVals =
  -- trace ("findIndexOverlap: " ++ show constraint ++ ", indexVals: "
  --      ++ show indexVals) $
  loop (map asPairs indexVals) (replicate (length indexVals) 0)
  where
    asPairs :: [k] -> [Range k]
    asPairs (x:y:xs) = (RANGE x y):(asPairs (y:xs))
    asPairs (x:[])   = []
    asPairs []       = []

    loop :: [[Range k]] -> [Int] -> [[Int]]
    loop ranges offsets =
      case checkOverlap constraint $ map head ranges of
        (True, [])         -> offsets:[]
        (True, toAdvance)  -> offsets:(advance ranges offsets toAdvance)
        (False, toAdvance) -> advance ranges offsets toAdvance

    advance :: [[Range k]] -> [Int] -> [Int] -> [[Int]]
    advance ranges offsets [] = loop ranges offsets
    advance ranges offsets (a:as) =
      advance (advanceRange a ranges) (advanceOffset a offsets) as

    advanceRange :: Int -> [[Range k]] -> [[Range k]]
    advanceRange = listAdjust nextRange
      where
        nextRange ((RANGE min max):[]) = (END_GT max):[]
        nextRange x@((END_GT _):[])    = x
        nextRange (_:xs)               = xs

    advanceOffset :: Int -> [Int] -> [Int]
    advanceOffset = listAdjust (+1)


partitionSetNodes :: [HitchhikerSetNode k]
                  -> ( [TreeIndex k (HitchhikerSetNode k)]
                     , [S.Set k])
partitionSetNodes = partitionWith match
  where
    match (HitchhikerSetNodeIndex ti hh)
      | S.null hh = Left ti
      | otherwise = error "Can't work with hitchhikers"
    match (HitchhikerSetNodeLeaf s)     = Right s

findMinMax :: Ord k => S.Set k -> (k, k)
findMinMax s = (S.findMin s, S.findMax s)

nuFindImpl :: forall k. (Show k, Ord k)
           => Maybe (S.Set k)
           -> [HitchhikerSetNode k]
           -> [S.Set k]
nuFindImpl inputConstraint inputNodes = output
  where
    (idxNodes, leaves) = partitionSetNodes inputNodes

    -- The constraint is a set which is made up of the intersection of all leaf
    -- nodes we've seen so far. This happens when the tree depth of the
    -- different `HitchhikerSet`s differ: a short tree's leaves keep getting
    -- compared to the remaining trees we keep descending.
    constraint :: Maybe (S.Set k)
    constraint = case (leaves, inputConstraint) of
      ([], Nothing)  -> Nothing
      (xs, Nothing)  -> Just $ foldl1 S.intersection xs
      ([], Just set) -> Just set
      (xs, Just set) -> Just $ foldl S.intersection set xs

    -- For every item in idxes, we want to turn that index into a list of lists
    -- which we can traverse in parallel.
    mkRangeAndVals (TreeIndex ks vals) =
      ( [getLeftmostValue $ vals ! 0] ++ V.toList ks
      , V.toList vals)
    (idxRanges, idxVals) = unzip $ map mkRangeAndVals idxNodes

    -- Each list in this list are indexes into idxVals as parallel arrays for
    -- the next step.
    idxCandidates :: [[Int]]
    idxCandidates = case (constraint, idxRanges) of
      -- If we had leaves and/or an input constraint which are impossible to
      -- satisfy, just bail.
      (Just s, _) | S.null s -> []
      (_, [])                -> []
      (const, ranges)        -> findIndexOverlap (fmap findMinMax const) ranges

    recurIndexes :: Maybe (S.Set k) -> [Int] -> [S.Set k]
    recurIndexes constraint idxes =
      nuFindImpl constraint $ zipWith (!!) idxVals idxes

    output = case (idxCandidates, constraint) of
      ([], Nothing)              -> []
      (_, Just set) | S.null set -> []
      ([], Just set)             -> [set]
      (xs, constraint)           ->
        concat $ map (recurIndexes constraint) xs

-- Toplevel which flushes things downward.
nuIntersect :: forall k. (Show k, Ord k)
            => [HitchhikerSet k] -> [S.Set k]
nuIntersect [] = []
--nuIntersect [a] = [a]  -- TODO: Handle this.
nuIntersect sets = output
  where
    mybNodes :: [Maybe (HitchhikerSetNode k)]
    mybNodes = map rawNode sets

    output = case (or $ map isNothing mybNodes) of
      True -> []
      False -> nuFindImpl Nothing $ map (flushDownwards hhSetTF) $ catMaybes mybNodes
