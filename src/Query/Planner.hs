module Query.Planner where

import           ClassyPrelude

import           Query.HitchhikerDatomStore
import           Query.PlanEvaluator
import           Query.Types

import qualified Data.Set                   as S

-- Planner try 3
--
-- This Planner can be thought of as analogous to the first planner sketch I
-- made, but with predicate handling where we only

data Direction
  = FORWARDS
  | BACKWARDS
  | LEFT_ONLY
  | RIGHT_ONLY
  | IRRELEVANT

mkPlan :: [Binding] -> [Clause] -> Set Variable -> PlanHolder
mkPlan bindingInputs clauses target =
  converge $ filter targetNeeds $ go startingInputs clauses
  where
    converge :: [PlanHolder] -> PlanHolder
    converge [r] = r
    -- TODO: Since we've filtered out any irrelevant relations, this should be
    -- turned into a Cartesian product of the remaining relations projected
    -- into the target set.
    converge inputs = error $ "Plan did not converge to one relation: "
                           <> show inputs

    targetNeeds :: PlanHolder -> Bool
    targetNeeds ph = (S.intersection (planHolderBinds ph) target) /= S.empty

    go :: [PlanHolder] -> [Clause] -> [PlanHolder]
    go ph [] = ph
    go inputs (c:cs) =
      let past = pastProvides inputs
          future = futureNeeds cs
      in case c of
        (DataPattern (LC_XAZ eSymb attr vSymb)) ->
          case direction eSymb vSymb past future of
            FORWARDS  ->
              let load = PH_TAB eSymb vSymb
                       $ LoadTab USE_AEV (VAL_ATTR attr) eSymb vSymb
              in go (joinAll (rhJoin future) (load:inputs)) cs
            BACKWARDS ->
              let load = PH_TAB vSymb eSymb
                       $ LoadTab USE_AVE (VAL_ATTR attr) vSymb eSymb
              in go (joinAll (rhJoin future) (load:inputs)) cs
            LEFT_ONLY ->
              let load = PH_SET eSymb
                       $ TabKeySet
                       $ LoadTab USE_AEV (VAL_ATTR attr) eSymb vSymb
              in go (joinAll (rhJoin future) (load:inputs)) cs
            RIGHT_ONLY ->
              let load = PH_SET vSymb
                       $ TabKeySet
                       $ LoadTab USE_AVE (VAL_ATTR attr) vSymb eSymb
              in go (joinAll (rhJoin future) (load:inputs)) cs
            IRRELEVANT -> go inputs cs
        (BiPredicateExpression pred arg1 arg2) ->
          -- We're a predicate. We look at the current state of the tree to try
          -- to transform that tree into a structure with specialized predicate
          -- supports, but otherwise
          case (arg1, arg2) of
            (ARG_VAR var1, ARG_VAR var2) ->
              -- This is the kind of complicated one because
              error "both are variables"
            (ARG_CONST const1, ARG_VAR var2) ->
              go (map (applyPredCV pred const1 var2) inputs) cs
            (ARG_VAR var1, ARG_CONST const2) -> error "compare against const"
            (ARG_CONST const1, ARG_CONST const2) ->
              error "degenerate case; i hate you."

    applyPredCV :: BuiltinPred -> Value -> Variable -> PlanHolder -> PlanHolder
    applyPredCV pred const1 var2 ph
      | not $ S.member var2 $ planHolderBinds ph = ph
      | otherwise = case ph of
          -- TODO: this isn't general, this is to support one very specific
          -- query.

          --
          -- TODO: Continue here once I've made th

--          (PH_SET s (TabKeySet tab)) -> (PH_SET s (TabKeySet $ filter

          -- TODO: Finish here!
          --
          -- (PH_SET s (SetJoin lhs rhs))
          --   | s == var2 -> PH_SET s (SetJoin
          --                           -- TODO: Can't recur like this because
          --                           -- these are the actual sets. Damn.
          --                            (applyPredCV pred const1 var2 lhs)
          --                            (applyPredCV pred const1 var2 rhs))
          _               -> undefined

    -- applyPredToPlanSet :: BuiltinPred -> Value -> Variable
    --                    -> Plan RelSet
    --                    -> Plan RelSet
    -- applyPredToPlanSet pred const1 var2 (TabKeySet ptab) =


    startingInputs = map bindToPlan $ zip [0..] bindingInputs

    bindToPlan (i, B_SCALAR symb)     = PH_SCALAR symb $ InputScalar symb i
    bindToPlan (i, B_COLLECTION symb) = PH_SET symb $ InputSet symb i

    direction :: Variable -> Variable -> Set Variable -> Set Variable
              -> Direction
    direction leftS rightS past future
      | S.member leftS future && S.member rightS future =
          -- FORWARDS is a cop out, we need to make a decision about what
          -- the best direction to go in is now.
          -- error "Both future"
          FORWARDS
      | S.member leftS past && S.member rightS future = FORWARDS
      | S.member leftS future && S.member rightS past = BACKWARDS
      | S.member leftS future = LEFT_ONLY
      | S.member rightS future = RIGHT_ONLY
      | otherwise = IRRELEVANT
--        error $ "TODO: Figure out complicated cases in direction: " <> show leftS <> " " <> show rightS <> " " <> show past <> " " <> show future

    pastProvides :: [PlanHolder] -> Set Variable
    pastProvides rs = S.unions (map planHolderBinds rs)

    futureNeeds :: [Clause] -> Set Variable
    futureNeeds cs = S.unions (target:(map clauseUses cs))

-- -----------------------------------------------------------------------

-- Given a joining function which may or may not join two elements, run all
-- permutations of all elements of with the associative function `f`. If `f`
-- returns Just, replaces both elements with the newly joined element and
-- starts over, returning only when no more joins can be made.
joinAll :: (a -> a -> Maybe a) -> [a] -> [a]
joinAll _ [] = []
joinAll _ [x] = [x]
joinAll f (x:xs) = joinOuter x xs []
  where
    joinOuter x [] prev     = reverse (x:prev)
    joinOuter x yo@(y:ys) prev = case joinInner x yo [] of
      Nothing              -> joinOuter y ys (x:prev)
      Just (new, ysMinusY) -> joinAll f (reverse (new:prev) ++ ysMinusY)

    joinInner x [] prev = Nothing
    joinInner x (y:ys) prev = case f x y of
      Nothing  -> joinInner x ys (y:prev)
      Just new -> Just (new, reverse prev ++ ys)

rhJoin :: Set Variable -> PlanHolder -> PlanHolder -> Maybe PlanHolder
rhJoin future l r = case (l, r) of
  (PH_SET lhs lhv, PH_SET rhs rhv)
    | lhs == rhs -> Just $ PH_SET lhs $ SetJoin lhv rhv
    | otherwise  -> Nothing

  (lhs@(PH_SCALAR _ _), rhs@(PH_SET _ _)) -> rhJoin future rhs lhs
  (PH_SET lhs lhv, PH_SCALAR rhs rhv)
    | lhs == rhs -> Just $ PH_SET lhs $ SetScalarJoin lhv rhv
    | otherwise -> Nothing

  (lhs@(PH_SET _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
  (PH_TAB lhFrom lhTo lht, PH_SET rhSymb rhv)
    | lhFrom == rhSymb && inFuture lhFrom && inFuture lhTo ->
        Just $ PH_TAB lhFrom lhTo $ TabRestrictKeys lht rhv
    | lhFrom == rhSymb && inFuture lhFrom ->
        Just $ PH_SET lhFrom $ SetJoin (TabKeySet lht) rhv
    | lhFrom == rhSymb && inFuture lhTo ->
        Just $ PH_SET lhTo $ TabSetUnionVals rhv lht
    | lhFrom == rhSymb -> error "wtf is this case"
    | otherwise -> error "Handle all the lhTo cases."

  (lhs@(PH_SCALAR _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
  (PH_TAB lhFrom lhTo lhTab, PH_SCALAR rhSymb rhv)
    | rhSymb == lhFrom -> Just $ PH_SET lhTo $ TabScalarLookup rhv lhTab
    | rhSymb == lhTo -> error "Handle backwards scalar table matching."
    | otherwise -> Nothing

  (PH_TAB lhFrom lhTo lht, PH_TAB rhFrom rhTo rht)
    | lhFrom == rhFrom -> Just $ PH_MULTITAB lhFrom [lhTo, rhTo]
                               $ MkMultiTab lht rht
    | otherwise -> error "Handle all the cases before adding Nothing at end"

  (lhs@(PH_SCALAR _ _), rhs@(PH_MULTITAB _ _ _)) -> rhJoin future rhs lhs
  (PH_MULTITAB keySymb valSymbs mt, PH_SCALAR rhSymb rhv)
    | keySymb == rhSymb -> error "Handle multitab/scalar same"
    | elem rhSymb valSymbs -> error "Handle multitab value scalar"
    | otherwise -> Nothing

  (PH_ROWS lSymb lSort lRows, PH_ROWS rSymb rSort rRows) -> undefined

  (a, b) -> error $ "TODO: Unhandled rhJoin case: " <> show a <> " <--> "
                 <> show b
  where
    inFuture = flip S.member future










-- This is the integrated planner.

-- So we're back here again: we have a bunch of clauses and we have to build a
-- plan out of them.


-- I'm really building a dependency graph here.

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


-- A quick chatgpt q suggests that starting from :in is how most database query
-- engines work (and only then do a second pass optimization starting from the
-- results).
--
-- This implies that last weeks' work was probably mostly ok?


-- The entire idea around finding promotable predicates is actually rather
-- difficult to handle in all cases. You need to make the

-- The above is an input graph, a sort of edge list. How do you

-- ?amount ---------------------------------------------v
-- [?tag] -> :derp/tag -> [?e ?tag] -> :derp/upvotes (pred) ->


-- The current structure of mkPlan in the old Planner.hs implementation goes
-- one clause at a time, and then tries to joinAll repeatedly. That kind of
-- works? But not really.


-- The repeated join is actually not the way to structure this because it can't
-- do middle joins.


-- Imagine you are trying to write a
--
-- What performs the unification?

-- mkPlanSketch :: [PlanHolder] -> [Clause] -> Set Variable -> [PlanHolder]
-- mkPlanSketch startingInputs clauses targets = go startingInputs clauses
--   where

--     go :: [PlanHolder] -> [Clause] -> [PlanHolder]
--     go ph [] = undefined
--     go inputs (c:cs) =
--       -- We want to build around a different pattern here. Previously, going to
--       -- the
--       case c of
--         (BiPredicateExpression pred lhs rhs) ->
--           -- We now know that we're a predicate. We have to determine from the
--           -- args what the inputs we need to bind with are.
--           undefined

-- How do you determine how to bind this? In the below, it's obvious that since
-- ?amount is a single item, it's easy to

-- Truth table:
--
-- Everything with rows becomes rows. (TODO: is that right? Are there limited
-- cases with limited need variables where it'd be more efficient to output the
-- other?)
--
-- (PH_ROWS ...) (PH_ROWS ...) -> ROWS
-- (PH_ROWS ...) (PH_SCALAR ) -> ROWS
-- (PH_ROWS ...) (PH_SET ..) -> ROWS

--
-- (PH_SET ...) (PH_SET ...) -> ROWS



-- (PH_TAB _ value) (PH_SCALAR ...) -> PH_TAB

-- -- OK, let's JUST write this case.
-- reworkTabScalar
--   :: BuiltinPred
--   -> Variable
--   -> Variable
--   -> PlanHolder -> PlanHolder -> PlanHolder
-- reworkTabScalar pred l r (PH_TAB from to planTab) (PH_SCALAR var scalar) =
--   case planTab of
--     lt@(LoadTab _ _ _ _) ->
--       -- We can't directly modify a load, so wrap it in a filter afterwards.
--       undefined
--     trk@(TabRestrictKeys _ _)
--       | l == to && r == var ->
--           (FilterValsTabRestrictKeys
--             (PrepareLHSBiPred

--       -- We want to rewrite this restriction so that


--
-- predPlan = mkPlanSketch
--   [PH_TAB (VAR "?e") (VAR "?upvotes") $ TabRestrictKeys
--    (LoadTab USE_AEV (VAL_ATTR (ATTR ":derp/upvotes")) (VAR "?e") (VAR "?upvotes"))
--    (TabSetUnionVals (InputSet (VAR "?tag") 0)
--     (LoadTab USE_AVE (VAL_ATTR (ATTR ":derp/tag")) (VAR "?tag") (VAR "?e"))),

--    PH_SCALAR (VAR "?amount") $ InputScalar (VAR "?amount") 1
--   ]
--   [BiPredicateExpression B_GT
--                          (ARG_VAR (VAR "?upvotes"))
--                          (ARG_VAR (VAR "?amount"))]
--   (S.fromList [(VAR "?e")])

-- The tree transformation that we want is the above to equal




-- What's the main loop for this? What do you do?
--
--


-- Finally, removes relations that just don't build towards the future.
