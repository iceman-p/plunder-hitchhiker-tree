{-# OPTIONS_GHC -w   #-}
module MultiIntersectV2Vector where

-- I gave up on porting this to row.

-- import           Prelude

-- import           Data.List         (findIndices)
-- import           Data.Maybe        (catMaybes, fromMaybe, isNothing)
-- import           Debug.Trace

-- import           HitchhikerSet
-- import           Impl.Tree
-- import           Impl.Types
-- import           Types

-- import           Data.Sorted
-- import           Data.Sorted.Row
-- import           Data.Sorted.Types

-- import qualified ClassyPrelude     as P
-- import qualified Data.List         as L
-- import qualified Data.Set          as S

-- -- ---------------------------------------------------------------------------

-- -- Copied with GHC.Utils.Misc for now.
-- partitionWith :: (a -> Either b c) -> [a] -> ([b], [c])
-- partitionWith _ [] = ([],[])
-- partitionWith f (x:xs) = case f x of
--                          Left  b -> (b:bs, cs)
--                          Right c -> (bs, c:cs)
--     where (bs,cs) = partitionWith f xs

-- -- ---------------------------------------------------------------------------

-- -- Given an optional (min, max) to constrain everything to,

-- checkOverlap :: Ord k => Maybe (k, k) -> Row [k] -> (Bool, Maybe k)
-- checkOverlap concreteSet ranges
--   | P.null ranges = (False, Nothing)
--   | P.null smallestList = (True, Nothing)
--   | otherwise = (matches, Just smallestMax)
--   where
--     largestMin  = maximum $ map getMin $ P.toList ranges
--     smallestList = catMaybes $ map getMax $ P.toList ranges
--     smallestMax = minimum $ smallestList

--     matches = case concreteSet of
--       Nothing -> smallestMax > largestMin
--       Just (boundingMin, boundingMax) ->
--         (smallestMax > largestMin) &&
--         (smallestMax > boundingMin) &&
--         (largestMin <= boundingMax)

--     getMin :: [k] -> k
--     getMin [k]       = k
--     getMin (min:_:_) = min

--     getMax :: [k] -> Maybe k
--     getMax [_]       = Nothing
--     getMax (_:max:_) = Just max

-- findIndexOverlap :: forall k v. (Show k, Ord k)
--                  => Maybe (k, k)
--                  -> Row [k]
--                  -> Row [v]
--                  -> Row [v]
-- findIndexOverlap constraint ranges vals =
--   -- trace ("findIndexOverlap: " ++ show constraint ++ ", indexVals: "
--   --      ++ show indexVals) $
--   P.fromList $ L.unfoldr step (ranges, vals)
--   where
--     step :: (Row [k], Row [v])
--          -> Maybe ([v], (Row [k], Row [v]))
--     step (ranges, vals)
--       | P.null ranges = Nothing
--       | P.null vals = Nothing
--       | otherwise = case checkOverlap constraint ranges of
--           (True, Nothing)          ->
--               Just (map head $ P.toList vals,
--                     (mempty, mempty))
--           (True, Just smallestMax) ->
--               Just (map head $ P.toList vals,
--                     advanceBySmallestMax smallestMax ranges vals)
--           (False, Just smallestMax) ->
--               step $ advanceBySmallestMax smallestMax ranges vals

--     advanceBySmallestMax :: k -> Row [k] -> Row [v]
--                          -> (Row [k], Row [v])
--     advanceBySmallestMax smallestMax ranges vals =
--       unzip $ map check $ zip (toList ranges) vals
--       where
--         check (r, i) = case r of
--             (_:x:_) | x == smallestMax -> ((nextRange r), (tail i))
--             _                          -> (r, i)

--         nextRange (min:max:[]) = (max:[])
--         nextRange x@(_:[])     = x
--         nextRange (_:xs)       = xs

-- partitionSetNodes :: Row (HitchhikerSetNode k)
--                   -> ( Row (TreeIndex k (HitchhikerSetNode k))
--                      , Row (ArraySet k))
-- partitionSetNodes = partitionWith match
--   where
--     match (HitchhikerSetNodeIndex ti hh)
--       | L.null hh = Left ti
--       | otherwise = error "Can't work with hitchhikers"
--     match (HitchhikerSetNodeLeaf s)     = Right s

-- findMinMax :: Ord k => ArraySet k -> (k, k)
-- findMinMax s = (ssetFindMin s, ssetFindMax s)

-- nuFindImpl :: forall k. (Show k, Ord k)
--            => Maybe (ArraySet k)
--            -> Row (HitchhikerSetNode k)
--            -> [ArraySet k]
-- nuFindImpl inputConstraint inputNodes = output
--   where
--     (idxNodes, leaves) = partitionSetNodes inputNodes

--     -- The constraint is a set which is made up of the intersection of all leaf
--     -- nodes we've seen so far. This happens when the tree depth of the
--     -- different `HitchhikerSet`s differ: a short tree's leaves keep getting
--     -- compared to the remaining trees we keep descending.
--     constraint :: Maybe (ArraySet k)
--     constraint = case (leaves, inputConstraint) of
--       (xs, Nothing)
--         | P.null xs -> Nothing
--         | otherwise -> Just $ foldl1 ssetIntersection xs
--       (xs, Just set)
--         | P.null xs -> Just set
--         | otherwise -> Just $ foldl ssetIntersection set xs

--     -- For every item in idxes, we want to turn that index into a list of lists
--     -- which we can traverse in parallel.
--     mkRangeAndVals (TreeIndex ks vals) =
-- --      V.cons (getLeftmostValue $ vals ! 0) ks
--       ( [getLeftmostValue $ vals ! 0] ++ P.toList ks
--       , P.toList vals)
--     (idxRanges, idxVals) = unzip $ map mkRangeAndVals idxNodes

--     -- Each list in this list are indexes into idxVals as parallel arrays for
--     -- the next step.
--     idxCandidates :: [[HitchhikerSetNode k]]
--     idxCandidates = case (constraint, idxRanges) of
--       -- If we had leaves and/or an input constraint which are impossible to
--       -- satisfy, just bail.
--       (Just s, _) | ssetIsEmpty s -> []
--       (const, ranges)
--         | P.null ranges      -> []
--         | otherwise ->
--             P.toList $ findIndexOverlap (fmap findMinMax const) ranges idxVals
--         -- let x =
--         -- in trace ("Result: " ++ show x) x

--     output = case (idxCandidates, constraint) of
--       ([], Nothing)              -> []
--       (_, Just set) | ssetIsEmpty set -> []
--       ([], Just set)             -> [set]
--       (xs, constraint)           ->
--         concat $ map (nuFindImpl constraint . P.fromList) xs

-- -- Toplevel which flushes things downward.
-- nuIntersect :: forall k. (Show k, Ord k)
--             => [HitchhikerSet k] -> [ArraySet k]
-- nuIntersect [] = []
-- --nuIntersect [a] = [a]  -- TODO: Handle this.
-- nuIntersect sets = output
--   where
--     mybNodes :: [Maybe (HitchhikerSetNode k)]
--     mybNodes = map rawNode sets

--     output = case (or $ map isNothing mybNodes) of
--       True -> []
--       False -> nuFindImpl Nothing $ P.fromList $ map (flushDownwards hhSetTF) $ catMaybes mybNodes
