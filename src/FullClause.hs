module FullClause where

import           ClassyPrelude

import           Data.List            (foldl1')
import           Data.Map             (Map)
import           Data.Set             (Set)

import           Safe                 (atMay, tailSafe)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import           HitchhikerDatomStore

import           Data.Maybe           (fromJust)

import           Data.Sorted

import qualified HitchhikerMap        as HM
import qualified HitchhikerSet        as HS
import qualified HitchhikerSetMap     as HSM

import qualified Data.List            as L
import qualified Data.Map             as M
import qualified Data.Set             as S
import qualified Data.Vector          as V

import qualified Data.Kind

-- -----------------------------------------------------------------------


-- Pre planning.
--
-- Given a list of raw clauses, how do you plan out the

-- A variable like "?e"
newtype Variable = VAR Text
  deriving (Show, Ord, Eq)

-- Binding query input or function output to the right type.
data Binding
  = B_SCALAR Symbol
  -- | B_TUPLE [Symbol]
  | B_COLLECTION Symbol
  -- | B_RELATION
  deriving (Show)

data Source
  = SourceDefault
  | SourceNamed Text
  deriving (Show)

data FnArg
  = ARG_VAR Variable
  | ARG_CONST Value
  | ARG_Source Source
  deriving (Show)

fnArgToVariable :: FnArg -> [Variable]
fnArgToVariable (ARG_VAR v) = [v]

data BuiltinPred
  = B_LT
  | B_LTE
  | B_EQ
  | B_GTE
  | B_GT
  deriving (Show)

data Pred
  = PredBuiltin BuiltinPred
  | PredFun -- TODO
  deriving (Show)

data Func = FUNC
  deriving (Show)

data Predicate = PREDICATE Pred [FnArg]
  deriving (Show)

-- Type pattern EAV for binding value, XYZ for binding symbol
data LoadClause
  = LC_EAZ EntityId Attr Variable
  -- TODO: Possible in the model, but requires iteration.
  -- | C_EYV EntityId Variable Value
  | LC_XAV Variable Attr Value

  | LC_EYZ EntityId Variable Variable
  | LC_XAZ Variable Attr Variable
  | LC_XYV Variable Variable Value
  deriving (Show)

-- All symbols bound by a clause
loadClauseBinds :: LoadClause -> Set Variable
loadClauseBinds (LC_EAZ _ _ z) = S.fromList [z]
-- C_EYV
loadClauseBinds (LC_XAV x _ _) = S.fromList [x]
loadClauseBinds (LC_EYZ _ y z) = S.fromList [y, z]
loadClauseBinds (LC_XAZ x _ z) = S.fromList [x, z]
loadClauseBinds (LC_XYV x y _) = S.fromList [x, y]

-- "Input" Clause: A new type of clause with everything in it.
data IClause
  = NotClause Source [IClause]
  | NotJoinClause Source [Variable] [IClause]
  | OrClause Source [OrClauseBody]
  | OrJoinClause -- TODO: This interacts with rule-vars in a weird way?

  -- The ExpressionClause
  | DataPattern LoadClause
  | PredicateExpression Predicate
  | FunctionExpression Func [FnArg] [Binding]
  | RuleExpression -- big question mark.
  deriving (Show)

iclauseToVars :: IClause -> Set Variable
iclauseToVars (DataPattern load)                         = loadClauseBinds load
iclauseToVars (PredicateExpression (PREDICATE x fnArgs)) =
  S.fromList $ join $ map fnArgToVariable fnArgs

-- Or and OrJoin have different rules around clauses. Ironically, the only
-- place you must explicitly state 'and' is inside an 'or'.
data OrClauseBody
  = OCB_CLAUSE IClause
  | OCB_AND_CLAUSES [IClause]
  deriving (Show)

-- So given a set of the input clauses above, how do you form a query plan?
-- DataPatterns bring data into scope. Bringing multiple patterns into scope
-- does an implicit join. But also all the rest.

-- None of this "rules passed in at run time" nonsense, rules should be compile
-- time operations, duh.
data RulePack

-- TODO: Eventually, we'll want a sort order on this like there was in the
-- original HitchhikerDataomStore version.
data Rows = ROWS [Variable] [Vector Value]
  deriving (Show)

-- -----------------------------------------------------------------------

-- This is the stupid evaluator. It is very stupid. Given a database and a list
-- of iclauses, it evaluates the query line by line, materializing everything
-- into rows immediately and then implementing all operations as full table
-- scans.
--
-- The point of the stupid evaluator is that it should be obviously correct
-- with little concern for runtime performance so that runs of the main
-- implementation can be checked against the stupid evaluator for
-- equivalence. The main engine which separates planning and evaluation is very
-- complicated, so we must convince ourselves it is correct by comparing it
-- against this.

-- TODO: [Sources] instead of Database
stupidEvaluator
  :: Database -> [Rows] -> [RulePack] -> [IClause] -> [Variable]
  -> Rows
stupidEvaluator db inputs rulePacks clauses target = go inputs clauses
  where
    go inputs clauses = case (inputs, clauses) of
      ([], [])    -> error "handle empty, empty"
      ([r], [])   -> error "if r.vars and target are same, ok, otherwise err"
      (x:y:rs, _) -> go ((stupidRowJoin x y):rs) clauses
      ([r], cs)   -> evalWith r cs

    evalWith :: Rows -> [IClause] -> Rows
    evalWith r [] = projectRows target r
    evalWith r (c:cs) = case c of
      DataPattern (LC_XAZ eVar attr vVar) ->
        let t = partialLookup (VAL_ATTR attr) (aev db)
            newR = tabToRow eVar vVar t
        in evalWith (stupidRowJoin r newR) cs

      PredicateExpression (PREDICATE (PredBuiltin builtin) [pLeft, pRight]) ->
        evalWith (runBuiltinPredicate builtin pLeft pRight r) cs

projectRows :: [Variable] -> Rows -> Rows
projectRows target (ROWS vars rows) = ROWS target $ L.nub $ map change rows
  where
    idxes = map (\a -> fromJust $ L.elemIndex a vars) target
    change r = V.fromList $ map (r V.!) idxes

getVariable :: Variable -> [Variable] -> Vector Value -> Value
getVariable var vars row = row V.! (fromJust $ L.elemIndex var vars)

runBuiltinPredicate :: BuiltinPred -> FnArg -> FnArg -> Rows -> Rows
runBuiltinPredicate pred a b (ROWS vars rows) =
  ROWS vars $ filter (match a b) rows
  where
    match (ARG_VAR argvar) (ARG_CONST val) row =
      run pred (getVariable argvar vars row) val
    match (ARG_VAR left) (ARG_VAR right) row =
      run pred (getVariable left vars row) (getVariable right vars row)
    match (ARG_CONST val) (ARG_VAR argvar) row =
      run pred val (getVariable argvar vars row)

    run B_LT lhs rhs  = lhs < rhs
    run B_LTE lhs rhs = lhs <= rhs
    run B_EQ lhs rhs  = lhs == rhs
    run B_GTE lhs rhs = lhs >= rhs
    run B_GT lhs rhs  = lhs >= rhs

tabToRow :: Variable -> Variable -> HitchhikerSetMap Value Value -> Rows
tabToRow from to setmap = ROWS [from, to] asRows
  where
    asRows = concat $ map step $ HM.toList $ HSM.toHitchhikerMap setmap

    step :: (Value, HitchhikerSet Value) -> [Vector Value]
    step (k, tops) = map (\a -> V.fromList [k, a]) $ HS.toList tops

-- Perform join by just exploding the cartesian products of tables in the most
-- naive way you can think of.
stupidRowJoin :: Rows -> Rows -> Rows
stupidRowJoin (ROWS lhs lhRow) (ROWS rhs rhRow)
  | S.disjoint (S.fromList lhs) (S.fromList rhs)
      -- If the two row's symbols are disjoint, this is just a simple cartesian
      -- product with no other complications.
      = ROWS (lhs ++ rhs) $ cartesianProduct (V.++) lhRow rhRow
  | otherwise =
      -- We have to check if
      let (vars, overlap, leftCopy, rightCopy) = variableOverlap lhs rhs
          rows = cartesianJoin overlap leftCopy rightCopy lhRow rhRow
      in ROWS vars rows

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

tlhs = [
  VAR "?e",
  VAR "?name"
  ]

lr = map V.fromList $ [
  [VAL_INT 1, VAL_STR "Bob"],
  [VAL_INT 2, VAL_STR "Sally"],
  [VAL_INT 3, VAL_STR "George"]
  ]


trhs = [
  VAR "?e",
  VAR "?title"
  ]

rr = map V.fromList $ [
  [VAL_INT 1, VAL_STR "President"],
  [VAL_INT 2, VAL_STR "Vice President"],
  [VAL_INT 3, VAL_STR "Foreign Relations"]
  ]

testStupidDisjoint = stupidRowJoin (ROWS tlhs lr) (ROWS trhs rr)

-- Actually, it's correct that join with an empty is empty, isn't it? These are
-- cartesian set operations.
testEmptyJoin = stupidRowJoin (ROWS tlhs []) (ROWS trhs rr)



db :: Database
db = foldl' add emptyDB datoms
  where
    add db (e, a, v, tx, op) =
      learn (VAL_ENTID $ ENTID e, VAL_ATTR $ ATTR a, VAL_STR v, tx, op) db

    datoms = [
      (1, ":name", "Frege", 100, True),
      (1, ":nation", "France", 100, True),
      (1, ":aka", "foo", 100, True),
      (1, ":aka", "fred", 100, True),
      (2, ":name", "Peirce", 100, True),
      (2, ":nation", "France", 100, True),
      (3, ":name", "De Morgan", 100, True),
      (3, ":nation", "English", 100, True)
      ]

fullStupidEvalTest =
  stupidEvaluator
    db
    [ROWS [VAR "?alias"] [V.fromList [VAL_STR "fred"]]]
    []
    [DataPattern (LC_XAZ (VAR "?e") (ATTR ":aka") (VAR "?alias")),
     DataPattern (LC_XAZ (VAR "?e") (ATTR ":nation") (VAR "?nation"))]
    [VAR "?nation"]

-- -----------------------------------------------------------------------

-- Mini derpibooru like db
derpdb :: Database
derpdb = foldl' add emptyDB datoms
  where
    add db (e, a, v, tx, op) =
      learn (VAL_ENTID $ ENTID e, VAL_ATTR $ ATTR a, v, tx, op) db

    datoms = [
      -- Good image
      (1, ":derp/tag", VAL_STR "twilight sparkle", 100, True),
      (1, ":derp/tag", VAL_STR "cute", 100, True),
      (1, ":derp/tag", VAL_STR "tea", 100, True),
      (1, ":derp/upvotes", VAL_INT 200, 100, True),
      (1, ":derp/id", VAL_INT 1020, 100, True),
      (1, ":derp/thumburl", VAL_STR "//cdn/1020.jpg", 100, True),

      -- Not as good image
      (2, ":derp/tag", VAL_STR "twilight sparkle", 100, True),
      (2, ":derp/tag", VAL_STR "cute", 100, True),
      (2, ":derp/tag", VAL_STR "kite", 100, True),
      (2, ":derp/upvotes", VAL_INT 31, 100, True),
      (2, ":derp/id", VAL_INT 1283, 100, True),
      (2, ":derp/thumburl", VAL_STR "//cdn/1283.jpg", 100, True),

      -- Good image about a different subject
      (3, ":derp/tag", VAL_STR "pinkie pie", 100, True),
      (3, ":derp/upvotes", VAL_INT 9000, 100, True),
      (3, ":derp/id", VAL_INT 1491, 100, True),
      (3, ":derp/thumburl", VAL_STR "//cdn/1491.jpg", 100, True),

      -- Bad image about a different subject
      (4, ":derp/tag", VAL_STR "starlight glimmer", 100, True),
      (4, ":derp/upvotes", VAL_INT 1, 100, True),
      (4, ":derp/id", VAL_INT 2041, 100, True),
      (4, ":derp/thumburl", VAL_STR "//cdn/2041.jpg", 100, True)
      ]

-- (db/q '[:find ?derpid ?thumburl
--         :in $ [?tag ...] ?amount
--         :where
--         [?e :derp/tag ?tag]
--         [?e :derp/upvotes ?upvotes]
--         [(> ?upvotes ?amount)]
--         [?e :derp/id ?derpid]
--         [?e :derp/thumburl ?thumburl]]
--        db
--        ["twilight sparkle" "cute"] 100)

fullDerpTagPlan =
  stupidEvaluator
    derpdb
    [ROWS [VAR "?tag"] [
        V.fromList [VAL_STR "twilight sparkle"],
        V.fromList [VAL_STR "cute"]],
     ROWS [VAR "?amount"] [V.fromList [VAL_INT 100]]]
    []
    [DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/tag") (VAR "?tag")),
     DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/upvotes") (VAR "?upvotes")),
     PredicateExpression (PREDICATE (PredBuiltin B_GT) [
                             ARG_VAR (VAR "?upvotes"),
                             ARG_VAR (VAR "?amount")]),
     DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")),
     DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/thumburl") (VAR "?thumburl"))]
    [VAR "?derpid", VAR "?thumburl"]

-- OK, this is technically correct. We need to have a select on this to really
-- be done (which should unify down to a single item, but without the select,
-- we're getting
{-
ROWS [VAR "?tag",VAR "?amount",VAR "?e",VAR "?upvotes",VAR "?derpid",
      VAR "?thumburl"]
     [[VAL_STR "twilight sparkle",VAL_INT 100,VAL_ENTID (ENTID 1),VAL_INT 200,VAL_INT 1020,VAL_STR "//cdn/1020.jpg"],
      [VAL_STR "cute",VAL_INT 100,VAL_ENTID (ENTID 1),VAL_INT 200,VAL_INT 1020,VAL_STR "//cdn/1020.jpg"]]
-}


-- nuMkPlan :: [Source] -> [Binding] -> [RulePack] -> [IClause] -> [Symbol]
--          -> NuPlan
-- nuMkPlan sources inputs rulePacks clauses target = undefined

-- Given a list of IClauses, how do you make an optimal graph out of them?
--
-- - Predicates should be handled as part of a join when possible.
--
-- - Function Expressions are expensive and should be handled at the last
--   possible minute because they WILL turn things into rows.
--
-- - `Not` and `Or` clauses will be pushed down to the last possible minute:
--
--     "Datomic will attempt to push the not clause down until all necessary
--     variables are bound, and will throw an exception if that is not
--     possible."
--
--     "Datomic will attempt to push the or clause down until all necessary
--     variables are bound, and will throw an exception if that is not
--     possible."
--
-- -


-- General loop:
--
-- - Pick the first runnable clause, accumulating the "rest" clauses.
--
--   - We now need to unify that with the current relations. This is when
--     predicates happen though, so we must find all predicates that
--
-- - If Are there leftover clauses


-- Predicate promotion is difficult.
--
-- - You have to deal with


-- -----------------------------------------------------------------------

-- Take this maliciously rearranged query:
--
-- (db/q '[:find ?derpid ?thumburl
--         :in $ [?tag ...] ?amount
--         :where
--         [?e :derp/tag ?tag]
--         [?e :derp/upvotes ?upvotes]
--         [?e :derp/id ?derpid]
--         [(> ?upvotes ?amount)]]
--         [?e :derp/thumburl ?thumburl]
--        db
--        ["twilight sparkle" "cute"] 100)
--
-- How do we run it optimally?
--
-- - We load [?e :derp/tag ?tag]
-- - We see a possibility of unifying with [?tag ...]
-- - We check forward for possible ?tag predicates
-- - We check forward for possible ?e/?tag predicates.
--
-- - We load [?e :derp/upvotes ?upvotes]
-- - We see a possibility of unifying with [?e ?tag]
-- - We check forward for possible ?e/?tag predicates.
-- - We check forward for possible ?e/?upvotes predicates.
--   - We check forward each possible clause, checking each one for
-- - We look forward for required symbols.
-- - We do something like

--     MkMultiTab e-tag-tab e-upvotes-tab [] [preds]
--
--  Where the new MkMultiTab takes the two input tabs, a list of the output tab
--  symbols to emit, and the predicates to run during

data NuRelScalar = RSCALAR { sym :: Symbol, val :: Value }
  deriving (Show)
data NuRelSet = RSET { sym :: Symbol, val :: HitchhikerSet Value}
  deriving (Show)
data NuRelTab = RTAB { from :: Symbol
                     , to   :: Symbol
                     , val  :: HitchhikerSetMap Value Value}
  deriving (Show)

data NuRelation
  = REL_SCALAR NuRelScalar
  | REL_SET NuRelSet
  | REL_TAB NuRelTab
  deriving (Show)

data RowLookup
  = USE_EAV
  | USE_AEV
  | USE_AVE
  | USE_VAE
  deriving (Show)

data NuPlan :: Data.Kind.Type -> Data.Kind.Type where
  InputScalar :: Variable -> Int -> NuPlan NuRelScalar
  InputSet    :: Variable -> Int -> NuPlan NuRelSet

  LoadTab :: RowLookup -> Value -> Variable -> Variable -> NuPlan NuRelTab

  TabScalarLookup :: NuPlan NuRelScalar -> NuPlan NuRelTab -> NuPlan NuRelSet
  TabSetUnionVals :: NuPlan NuRelSet -> NuPlan NuRelTab -> NuPlan NuRelSet
  TabRestrictKeys :: NuPlan NuRelTab -> NuPlan NuRelSet -> NuPlan NuRelTab
  TabKeySet :: NuPlan NuRelTab -> NuPlan NuRelSet

  SetJoin :: NuPlan NuRelSet -> NuPlan NuRelSet -> NuPlan NuRelSet
  SetScalarJoin :: NuPlan NuRelSet -> NuPlan NuRelScalar -> NuPlan NuRelSet

--  MkMultiTab :: NuPlan NuRelTab -> NuPlan NuRelTab -> NuPlan NuRelMultiTab




data NuPlanHolder
  = PH_SCALAR Variable (NuPlan NuRelScalar)
  | PH_SET Variable (NuPlan NuRelSet)
  | PH_TAB Variable Variable (NuPlan NuRelTab)

nuPlanHolderVars :: NuPlanHolder -> Set Variable
nuPlanHolderVars (PH_SCALAR a _) = S.singleton a
nuPlanHolderVars (PH_SET a _)    = S.singleton a
nuPlanHolderVars (PH_TAB a b _)  = S.fromList [a, b]



-- -- -----------------------------------------------------------------------


-- -- Given the remaining clauses, attempt to unify two plans, possibly consuming
-- -- additional future clauses and other plans.
-- nuUnify :: [IClause] -> [NuPlanHolder] -> NuPlanHolder -> NuPlanHolder
--         -> Maybe ([IClause], NuPlanHolder)
-- nuUnify clauses other lhs rhs = go lhs rhs
--   where
--     -- TODO: bothPreds cases are hard and will require the rows relation type
--     -- so punt for now. You can't implement them on top of multitab so you must
--     -- fall back to rows.
--     (restClauses, lhPreds, rhPreds, bothPreds) =
--       findPromotablePredicates other clauses lhs rhs

--     -- If a predicate is promoted to this unify, it implies it's not used "in
--     -- the future"; if the variables were used anywhere else, before or after,
--     -- it wouldn't have been promoted in the first place.
--     inFuture a = S.member a $ S.unions $ map iclauseToVars restClauses

--     -- All cases of trying to unify two plans, with promoted predicates.
--     go lhs rhs = fmap (\a -> (restClauses, a)) $ case (lhs, rhs) of
--         (PH_SET sKey sSet, PH_TAB tFrom tTo tTab) ->
--           unifyTabAndSet (tFrom, tTo, tTab) (sKey, sSet) rhPreds lhPreds []

--         -- (PH_TAB lhFrom lhTo lht, PH_SET rhSymb rhv, lp, rp, [])
--         --   | lhFrom == rhSymb && inFuture lhFrom && inFuture lhTo ->
--         --       Just $ PH_TAB

--         _ -> undefined

--     -- You're trying to unify a tab and a set. Maybe one side or both have a
--     -- predicate. (Maybe both have a predicate, but we can't handle that case
--     -- yet.)
--     unifyTabAndSet (tFrom, tTo, tTab) (sKey, sSet) []{-tPred-} []{-sPred-} []
--       --
--       -- TODO: I am NOT handling predicates yet, I need to reenable tPred/sPred
--       -- above and handle them below.
--       --
--       | tFrom == sKey && inFuture tFrom && inFuture tTo =
--           -- All keys are in the future, there can't be any predicates.
--           Just $ PH_TAB tFrom tTo $ TabRestrictKeys tTab sSet
--       | tFrom == sKey && inFuture tFrom =
--           -- OK, here we have a list of predicates
--           -- case tPred of
--           --   []
--           Just $ PH_SET tFrom $ SetJoin (TabKeySet tTab) sSet
--       | tFrom == sKey && inFuture tTo =
--           Just $ PH_SET tTo $ TabSetUnionVals sSet tTab
--       | tFrom == sKey = error "wtf is this case; both from and to not used?"
--       | otherwise = error "Handle all the lhTo cases."

--     -- ------------------------------------------------------------------------

--     unifySetAndSet :: (Variable, NuPlan NuRelSet)
--                    -> (Variable, NuPlan NuRelSet)
--                    -> [Predicate]
--                    -> Maybe NuPlanHolder
--     unifySetAndSet (lKey, lSet) (rKey, rSet) []
--       | lKey /= rKey = Nothing
--       | otherwise = Just $ PH_SET lKey $ SetJoin lSet rSet

--     unifySetAndSet (lKey, lSet) (rKey, rSet) (p:ps)
--       | lKey /= rKey = Nothing
--       | otherwise = undefined
--         -- We have set predicates here. We have to resolve the

--     -- During predicate resolving, we might need to bind the

--     resolvePredicateForSet :: Variable
--                            -> Predicate
--                            -> NuPlan NuRelSet
--                            -> NuPlan NuRelSet

--     -- We have a list of predicates. We want to maximally remove



--   -- What's the equivalent of the old inFuture in this new world? We're trying
--   -- to determine





--   -- (PH_TAB lhFrom lhTo lhTab, PH_TAB rhFrom rhTo rhTab) ->
--   --   -- let predicatesA = findPredicates lhFrom lhTo rhFrom rhTo
--   --   -- in
--   --     undefined

-- -- Given a possible join, figure out what predicates we can attach to this join
-- -- step. Predicates can either affect the left plan, the right plan, or require
-- -- data from both plans. Also returns the new list of clauses with all
-- -- promotable predicates removed.
-- --
-- -- TODO: Separating this into a lhs/rhs search might be wrong: imagine if
-- -- there's a predicate that applies to both the lhs and rhs.
-- findPromotablePredicates
--   :: [NuPlanHolder]
--   -> [IClause]
--   -> NuPlanHolder
--   -> NuPlanHolder
--   -> ([IClause], [Predicate], [Predicate], [Predicate])
-- findPromotablePredicates bound clauses lhs rhs =
--   (restClauses, lhPreds, rhPreds, bothPreds)
--   where
--     boundVars = S.unions $ map nuPlanHolderVars bound
--     lhsVars = S.union boundVars $ nuPlanHolderVars lhs
--     rhsVars = S.union boundVars $ nuPlanHolderVars rhs

--     (clauses2, lhPreds) = findSinglePred lhsVars clauses
--     (clauses3, rhPreds) = findSinglePred rhsVars clauses2
--     -- todo: double sided checks, where one symbol on each side has to be
--     -- compared. this part is separate because having a bothPreds often
--     (restClauses, bothPreds) = (clauses3, [])


--     -- vars is the variables of the item being merged.

--     findSinglePred :: Set Variable -> [IClause]
--                    -> ([IClause], [Predicate])
--     findSinglePred boundVars = go [] []
--       where
--         go :: [Predicate] -> [IClause] -> [IClause] -> ([IClause], [Predicate])
--         go outputPreds before [] = (reverse before, outputPreds)
--         go outputPreds before (clause:after) = case clause of
--           PredicateExpression p@(PREDICATE _ fnArgs)
--             | canPromotePredicate boundVars before after fnArgs ->
--                 go (p:outputPreds) before after
--             | otherwise -> go outputPreds (clause:before) after
--           _ -> go outputPreds (clause:before) after

-- -- You can promote a predicate if all its args are bound, and those arguments
-- -- aren't used by antyhing but other predicates or functions as input.
-- --
-- canPromotePredicate :: Set Variable
--                     -> [IClause]
--                     -> [IClause]
--                     -> [FnArg]
--                     -> Bool
-- canPromotePredicate boundVars beforeClause afterClause fnArg =
--   (S.intersection beforeVars argVars == S.empty) &&
--   (S.intersection afterVars argVars == S.empty) &&
--   (S.difference argVars boundVars == S.empty)
--   where
--     argVars = S.fromList $ join $ map fnArgToVariable fnArg
--     beforeVars = S.unions $ map iclauseToVars beforeClause
--     afterVars = S.unions $ map iclauseToVars afterClause


-- -- -----------------------------------------------------------------------

-- -- (db/q '[:find ?derpid ?thumburl
-- --         :in $ [?tag ...] ?amount
-- --         :where
-- --         [?e :derp/tag ?tag]
-- --         [?e :derp/upvotes ?upvotes]
-- --         [?e :derp/id ?derpid]
-- --         [(> ?upvotes ?amount)]
-- --         [?e :derp/thumburl ?thumburl]]
-- --        db
-- --        ["twilight sparkle" "cute"] 100)

-- --
-- -- TODO: What's next? We have to turn the above into a test case where a
-- -- artificially build out the steps for

-- -- Q: boundRelations should not include lhs and rhs.
-- promotableTest = findPromotablePredicates bounds clauses lhs rhs
--   where
--     -- Things bound which aren't lhs or rhs that need to be unified with.
--     bounds = [PH_SCALAR (VAR "?amount") $ InputScalar (VAR "?amount") 100]

--     -- The two join candidates that we're searching for matches for:
--     -- We have consumed and unified [?tag...] with [?e :derp/tag ?tag] at this
--     -- point.
--     lhs = PH_SET (VAR "?e")
--       $ (TabSetUnionVals
--        (InputSet (VAR "?tag") 0)
--        (LoadTab USE_AVE (VAL_ATTR (ATTR ":derp/tag")) (VAR "?tag") (VAR "?e")))

--     rhs = PH_TAB (VAR "?e") (VAR "?upvotes")
--         $ LoadTab USE_AEV (VAL_ATTR (ATTR ":derp/upvotes")) (VAR "?e")
--                   (VAR "?upvotes")

--     -- The remaining clauses to check in the
--     clauses = [
--       DataPattern $ LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid"),
--       PredicateExpression $ PREDICATE
--           (PredBuiltin B_GT)
--           [(ARG_VAR (VAR "?upvotes")), (ARG_VAR (VAR "?amount"))],
--       DataPattern $ LC_XAZ (VAR "?e") (ATTR ":derp/thumburl") (VAR "?thumburl")
--       ]

-- {-
-- -- YES! I have predicate promotion (for one sided predicates). But how do I use
-- -- this?

-- ([DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")),
--   DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/thumburl") (VAR "?thumburl"))],

--  --
--  [],
--  [PREDICATE (PredBuiltin B_GT) [ARG_VAR (VAR "?upvotes"),
--                                 ARG_VAR (VAR "?amount")]],
--  [])
-- -}

-- -- -----------------------------------------------------------------------

-- -- Let's take a step back: what does the evaluation step look like?
-- {-

-- go (SetFilterPredicate B_LTE val pset) =
--   let (RSET sym s) = go pset
--   in RSET sym $ HS.takeWhileAntitone (< val) s

-- So what does the processing look like that generates the above from
-- `(PREDICATE (PredBuiltin B_LTE) [(ARG_VAR (VAR "?upvotes")),
--                                  (ARG_VAR (VAR "?amount"))])` ?

-- Let's say we've determined that the set of ?upvotes has to be filtered by the
-- above predicate. How do we change that into

-- -}

-- -- -----------------------------------------------------------------------

-- {-

-- Maybe I've just coded myself into a corner. Some facts:

-- - I've got the

-- -


-- -}
