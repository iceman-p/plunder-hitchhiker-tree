module Query.NaiveEvaluator (naiveEvaluator) where

import           ClassyPrelude

import           Query.HitchhikerDatomStore
import           Query.Rows
import           Query.Types

import           Types

import           Data.Maybe                 (fromJust)

import qualified Data.List                  as L
import qualified Data.Map                   as M
import qualified Data.Set                   as S
import qualified Data.Vector                as V

import qualified HitchhikerMap              as HM
import qualified HitchhikerSet              as HS
import qualified HitchhikerSetMap           as HSM

-- This is the naive evaluator. It is very stupid. Given a database and a list
-- of Clauses, it evaluates the query line by line, materializing everything
-- into rows immediately and then implementing all operations as full table
-- scans. It does no planning, it runs directly against unparsed clauses like
-- the planner does.
--
-- The point of this stupid evaluator is that it does nothing clever and should
-- be obviously correct with little concern for runtime performance so that
-- runs of the main implementation can be checked against the naive evaluator
-- for equivalence. The main engine which separates planning and evaluation is
-- very complicated, so we must convince ourselves it is correct by comparing
-- it against this.

-- TODO: [DataSources] instead of Database
naiveEvaluator
  :: Database -> [Rows] -> [RulePack] -> [Clause] -> [Variable]
  -> Rows
naiveEvaluator db inputs rulePacks clauses target = go inputs clauses
  where
    go inputs clauses = case (inputs, clauses) of
      ([], [])    -> error "handle empty, empty"
      ([r], [])   -> error "if r.vars and target are same, ok, otherwise err"
      (x:y:rs, _) -> go ((naiveRowJoin x y):rs) clauses
      ([r], cs)   -> evalWith r cs

    evalWith :: Rows -> [Clause] -> Rows
    evalWith r [] = projectRows target r
    evalWith r (c:cs) = case c of
      DataPattern (LC_XAZ eVar (ATTR attrText) vVar) ->
        -- TODO: OK, so we have an "attr" but we have to actually resolve that
        -- string to the attribute number in the new implementation.
        --
        let attributesMap = attributes db
            attributePropMap = attributeProps db
            Just rawEntityId = M.lookup attrText attributesMap
            Just (indexed, _, _) = M.lookup rawEntityId attributePropMap
            attrEntity = rawEntityId
            tab = HSM.mapKeysMonotonic VAL_ENTID $
              partialLookup attrEntity (aev db)
            newR = tabToRows eVar vVar tab
        in evalWith (naiveRowJoin r newR) cs

      BiPredicateExpression pLeft builtin pRight ->
        evalWith (runBuiltinPredicate builtin pLeft pRight r) cs

projectRows :: [Variable] -> Rows -> Rows
projectRows target (ROWS vars _ rows) = ROWS target target $ L.nub $ map change rows
  where
    idxes = map (\a -> fromJust $ L.elemIndex a vars) target
    change r = V.fromList $ map (r V.!) idxes

getVariable :: Variable -> [Variable] -> Vector Value -> Value
getVariable var vars row = row V.! (fromJust $ L.elemIndex var vars)

runBuiltinPredicate :: BuiltinPred -> FnArg -> FnArg -> Rows -> Rows
runBuiltinPredicate pred a b (ROWS vars _ rows) =
  ROWS vars [] $ filter (match a b) rows
  where
    match (ARG_VAR argvar) (ARG_CONST val) row =
      run pred (getVariable argvar vars row) val
    match (ARG_VAR left) (ARG_VAR right) row =
      run pred (getVariable left vars row) (getVariable right vars row)
    match (ARG_CONST val) (ARG_VAR argvar) row =
      run pred val (getVariable argvar vars row)

    run pred lhs rhs = (builtinPredToCompare pred) lhs rhs

-- Perform join by just exploding the cartesian products of tables in the most
-- naive way you can think of
naiveRowJoin :: Rows -> Rows -> Rows
naiveRowJoin (ROWS lhs _ lhRow) (ROWS rhs _ rhRow)
  | S.disjoint (S.fromList lhs) (S.fromList rhs)
      -- If the two row's symbols are disjoint, this is just a simple cartesian
      -- product with no other complications.
      = ROWS (lhs ++ rhs) [] $ cartesianProduct (V.++) lhRow rhRow
  | otherwise =
      -- We have to check if
      let (vars, overlap, leftCopy, rightCopy) = variableOverlap lhs rhs
          rows = cartesianJoin overlap leftCopy rightCopy lhRow rhRow
      in ROWS vars [] rows

cartesianProduct :: (a -> b -> c) -> [a] -> [b] -> [c]
cartesianProduct f x y = [f a b | a <- x, b <- y]

cartesianJoin :: [(Int, Int)]
              -> [Int]
              -> [Int]
              -> [Vector Value]
              -> [Vector Value]
              -> [Vector Value]
cartesianJoin overlaps leftCopy rightCopy lhs rhs =
  catMaybes $
  cartesianProduct (vectorCompare overlaps leftCopy rightCopy) lhs rhs

vectorCompare :: [(Int, Int)] -> [Int] -> [Int] -> Vector Value -> Vector Value
              -> Maybe (Vector Value)
vectorCompare checkIdxes lCopy rCopy l r = check checkIdxes
  where
    check [] = Just $ V.fromList $ (map (l V.!) lCopy ++ map (r V.!) rCopy)
    check ((x,y):is) = if (l V.! x) /= (r V.! y)
                       then Nothing
                       else check is

-- Given two lists of variables, calculate the unified variables, the overlap
-- map (two indexes that point at the same variable) and the leftCopy/rightCopy
-- lists (lists of items to copy on match).
variableOverlap :: [Variable] -> [Variable]
                -> ([Variable], [(Int, Int)], [Int], [Int])
variableOverlap leftVars rightVars =
  (vars, overlap, leftCopy, rightCopy)
  where
    overlap = findOverlap (zip [0..] leftVars) []
    leftCopy = [0..(length leftVars - 1)]
    (rightCopy, rightCopyVars) = findRight (zip [0..] rightVars) [] []
    vars = leftVars ++ rightCopyVars

    findOverlap [] overlap = reverse overlap
    findOverlap ((li,x):xs) overlap = case L.elemIndex x rightVars of
      Nothing -> findOverlap xs overlap
      Just ri -> findOverlap xs ((li,ri):overlap)

    findRight [] idxes vars = (reverse idxes, reverse vars)
    findRight ((idx,rv):ys) idxes vars =
      if hasIdxInOverlap overlap idx
      then findRight ys idxes vars
      else findRight ys (idx:idxes) (rv:vars)

    hasIdxInOverlap :: [(Int, Int)] -> Int -> Bool
    hasIdxInOverlap [] _ = False
    hasIdxInOverlap ((_, ridx):xs) ndl
      | ndl == ridx = True
      | otherwise = hasIdxInOverlap xs ndl

