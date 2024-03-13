module Query.Planner where

import           ClassyPrelude              hiding (head)

import           Data.List                  (head)

import           Query.HitchhikerDatomStore
import           Query.PlanEvaluator
import           Query.Types

import qualified Data.Map                   as M
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

mkPlan :: [Database] -> [Binding] -> [Clause] -> [Variable] -> PlanHolder
mkPlan typeDBs bindingInputs clauses target =
  converge $ filter targetNeeds $ go startingInputs clauses
  where
    targetSet = S.fromList target

    converge :: [PlanHolder] -> PlanHolder
    converge [r] = constrainTo target r
    -- TODO: Since we've filtered out any irrelevant relations, this should be
    -- turned into a Cartesian product of the remaining relations projected
    -- into the target set.
    converge inputs = error $ "Plan did not converge to one relation: "
                           <> show inputs

    targetNeeds :: PlanHolder -> Bool
    targetNeeds ph = (S.intersection (planHolderBinds ph) targetSet) /= S.empty

    go :: [PlanHolder] -> [Clause] -> [PlanHolder]
    go ph [] = ph
    go inputs (c:cs) =
--      trace ("Step: c=" <> show c <> ", cs=" <> show cs <> ", inputs=" <> show inputs) $
      let past = pastProvides inputs
          future = futureNeeds cs
      in case c of
        (DataPattern (LC_XAZ eSymb attr@(ATTR attrText) vSymb)) ->
          let attributesMap = attributes $ head typeDBs
              attributePropMap = attributeProps $ head typeDBs
              Just rawEntityId = M.lookup attrText attributesMap
              Just (indexed, _, _) = M.lookup rawEntityId attributePropMap
              attrEntity = VAL_ENTID $ rawEntityId
          in case (direction eSymb vSymb past future, indexed) of
            (FORWARDS, _) ->
              let load = PH_TAB eSymb vSymb
                       $ LoadTab USE_AEV attrEntity eSymb vSymb
              in go (joinAll (rhJoin future) (load:inputs)) cs
            (BACKWARDS, True) ->
              let load = PH_TAB vSymb eSymb
                       $ LoadTab USE_AVE attrEntity vSymb eSymb
              in go (joinAll (rhJoin future) (load:inputs)) cs
            (BACKWARDS, False) ->
              error "Handle backwards loading without an AVE table."
            (LEFT_ONLY, _) ->
              let load = PH_SET eSymb
                       $ TabKeySet eSymb vSymb
                       $ LoadTab USE_AEV attrEntity eSymb vSymb
              in go (joinAll (rhJoin future) (load:inputs)) cs
            (RIGHT_ONLY, True) ->
              let load = PH_SET vSymb
                       $ TabKeySet vSymb eSymb
                       $ LoadTab USE_AVE attrEntity vSymb eSymb
              in go (joinAll (rhJoin future) (load:inputs)) cs
            (RIGHT_ONLY, False) ->
              error "Handle backwards loading without an AVE table"
            (IRRELEVANT, _) -> trace ("Irrelevant!?") $ go inputs cs
        (BiPredicateExpression arg1 pred arg2) ->
          -- We're a predicate. We look at the current state of the tree to try
          -- to transform that tree into a structure with specialized predicate
          -- supports, but otherwise will fallback on just making everything
          -- rows.
          case (arg1, arg2) of
            (ARG_CONST const1, ARG_VAR var2) ->
              go (map (applyPredL pred (InputConst const1) var2) inputs) cs
            (ARG_VAR var1, ARG_CONST const2) ->
              go (map (applyPredR pred var1 (InputConst const2)) inputs) cs

            (ARG_VAR var1, ARG_VAR var2)
              | var1 == var2 -> error "handle this weird case"

              | Just (var1Plan, restInputs) <- findScalarFor future var1 inputs
              , Nothing <- findScalarFor future var2 restInputs
                -> go (map (applyPredL pred var1Plan var2) inputs) cs

              | Just (var2Plan, restInputs) <- findScalarFor future var2 inputs
              , Nothing <- findScalarFor future var1 restInputs
                -> go (map (applyPredR pred var1 var2Plan) inputs) cs

              -- TODO: Both are variables where both aren't scalars.
              | otherwise -> error "both are variables"

            (ARG_CONST const1, ARG_CONST const2) ->
              error "degenerate case. i hate you."

    -- Finds and extracts a scalar plan
    findScalarFor :: Set Variable -> Variable -> [PlanHolder]
                  -> Maybe (Plan RelScalar, [PlanHolder])
    findScalarFor future var = go []
      where
        go :: [PlanHolder] -> [PlanHolder]
           -> Maybe (Plan RelScalar, [PlanHolder])
        go _ [] = Nothing
        go prev (ph@(PH_SCALAR scalarVar plan):xs)
          | var == scalarVar && inFuture var =
            Just (plan, (reverse prev) ++ [ph] ++ xs)
          | var == scalarVar =
            Just (plan, (reverse prev) ++ xs)
          | otherwise = go (ph:prev) xs
        go prev (x:xs) = go (x:prev) xs

        inFuture = flip S.member future

    applyPredL :: BuiltinPred -> Plan RelScalar -> Variable -> PlanHolder
               -> PlanHolder
    applyPredL pred const1 var2 = \case
      (PH_SET s val)
        | s == var2 -> PH_SET s $ applyPredLToPlan val
      (PH_TAB from to val)
        | from == var2 -> PH_TAB from to $ applyPredLToPlan val
        | to == var2 -> PH_TAB from to $ applyPredLToPlan val
      ph | not $ S.member var2 $ planHolderBinds ph -> ph
         | otherwise -> error $ "WRITE FALLBACK FOR: " <> show ph
      where
        applyPredLToPlan :: Plan a -> Plan a
        applyPredLToPlan = \case
          lt@(LoadTab _ _ from to)
            | var2 == from -> FilterPredTabKeys [PBP_LEFT const1 pred] lt
            | var2 == to -> FilterPredTabVals [PBP_LEFT const1 pred] lt

          sj@(SetJoin from lhs rhs)
            | var2 == from -> SetJoin from (applyPredLToPlan lhs)
                                           (applyPredLToPlan rhs)

          (TabKeySet from to tab)
            | var2 == from -> (TabKeySet from to $ applyPredLToPlan tab)

          (TabRestrictKeys from to tab set)
            | var2 == from -> TabRestrictKeys from to (applyPredLToPlan tab)
                                                      (applyPredLToPlan set)
            | var2 == to ->
              TabRestrictKeysVals from to [PBP_LEFT const1 pred] tab set

          (TabRestrictKeysVals from to ps tab set)
            | var2 == from ->
              TabRestrictKeysVals from to ps (applyPredLToPlan tab)
                                             (applyPredLToPlan set)
            | var2 == to ->
              TabRestrictKeysVals from to ((PBP_LEFT const1 pred):ps) tab set

          x -> error $ "WRITE PLAN CASE FOR " <> show x

    -- TODO: The VC case is basically undone right now and is just here to
    applyPredR :: BuiltinPred -> Variable -> Plan RelScalar -> PlanHolder
               -> PlanHolder
    applyPredR pred var1 const2 = \case
      (PH_SET s val)
        | s == var1 -> PH_SET s $ applyPredRToPlan val
      (PH_TAB from to val)
        | from == var1 -> PH_TAB from to $ applyPredRToPlan val
        | to == var1 -> PH_TAB from to $ applyPredRToPlan val
      ph | not $ S.member var1 $ planHolderBinds ph -> ph
         | otherwise -> trace ("Fallback: " <> show ph) undefined {- fallback case -}
      where
        applyPredRToPlan :: Plan a -> Plan a
        applyPredRToPlan = \case
          lt@(LoadTab _ _ from to)
            | var1 == from -> FilterPredTabKeys [PBP_RIGHT pred const2] lt
            | var1 == to -> FilterPredTabVals [PBP_RIGHT pred const2] lt

          sj@(SetJoin from lhs rhs)
            | var1 == from -> SetJoin from (applyPredRToPlan lhs)
                                           (applyPredRToPlan rhs)

          (TabKeySet from to tab)
            | var1 == from -> (TabKeySet from to $ applyPredRToPlan tab)

          (TabRestrictKeys from to tab set)
            | var1 == from -> TabRestrictKeys from to (applyPredRToPlan tab)
                                                      (applyPredRToPlan set)
            | var1 == to ->
              TabRestrictKeysVals from to [PBP_RIGHT pred const2] tab set

          (TabRestrictKeysVals from to ps tab set)
            | var1 == from ->
              TabRestrictKeysVals from to ps (applyPredRToPlan tab)
                                             (applyPredRToPlan set)
            | var1 == to ->
              TabRestrictKeysVals from to ((PBP_RIGHT pred const2):ps) tab set

    startingInputs = map bindToPlan $ zip [0..] bindingInputs

    bindToPlan (i, B_SCALAR symb)     = PH_SCALAR symb $ InputScalar symb i
    bindToPlan (i, B_COLLECTION symb) = PH_SET symb $ InputSet symb i

    direction :: Variable
              -> Variable
              -> Set Variable
              -> Set Variable
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
    futureNeeds cs = S.unions (targetSet:(map clauseUses cs))

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
rhJoin future l r = out
  where
    out = case (l, r) of
      (PH_SCALAR lhs lhv, PH_SCALAR rhs rhv)
        | lhs == rhs -> error "This has to be a set, right?"
        | otherwise -> Nothing

      (PH_SET lhs lhv, PH_SET rhs rhv)
        | lhs == rhs -> Just $ PH_SET lhs $ SetJoin lhs lhv rhv
        | otherwise  -> Nothing

      (lhs@(PH_SCALAR _ _), rhs@(PH_SET _ _)) -> rhJoin future rhs lhs
      (PH_SET lhs lhv, PH_SCALAR rhs rhv)
        | lhs == rhs -> Just $ PH_SET lhs $ SetScalarJoin lhv rhv
        | otherwise -> Nothing

      (lhs@(PH_SET _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
      (PH_TAB lhFrom lhTo lht, PH_SET rhSymb rhv)
        | lhFrom == rhSymb && inFuture lhFrom && inFuture lhTo ->
            Just $ PH_TAB lhFrom lhTo $ TabRestrictKeys lhFrom lhTo lht rhv
        | lhFrom == rhSymb && inFuture lhFrom ->
            Just $ PH_SET lhFrom $ SetJoin lhFrom (TabKeySet lhFrom lhTo lht) rhv
        | lhFrom == rhSymb && inFuture lhTo ->
            Just $ PH_SET lhTo $ TabSetUnionVals lhFrom rhv lhTo lht
        | lhFrom == rhSymb -> error "wtf is this case"
        | otherwise -> error "Handle all the lhTo cases."

      (lhs@(PH_SCALAR _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
      (PH_TAB lhFrom lhTo lhTab, PH_SCALAR rhSymb rhv)
        | rhSymb == lhFrom ->
          Just $ PH_SET lhTo $ TabScalarLookup rhSymb rhv lhTo lhTab
        | rhSymb == lhTo -> error "Handle backwards scalar table matching."
        | otherwise -> Nothing

      (PH_TAB lhFrom lhTo lht, PH_TAB rhFrom rhTo rht)
        -- TODO: Handle from==from && to==to case.
        | lhFrom == rhFrom && inFuture lhTo && inFuture rhTo ->
          Just $ PH_MULTITAB lhFrom [lhTo, rhTo] $ MkMultiTab lht rht
        | lhFrom == rhFrom && inFuture lhTo ->
          Just $ PH_TAB lhFrom lhTo
               $ TabRestrictKeys lhFrom lhTo lht
               $ TabKeySet rhFrom rhTo rht
        | lhFrom == rhFrom && inFuture rhTo ->
          Just $ PH_TAB rhFrom rhTo
               $ TabRestrictKeys rhFrom rhTo rht
               $ TabKeySet lhFrom rhTo lht
        | otherwise -> error "Handle all the cases before adding Nothing at end"

      (lhs@(PH_SCALAR _ _), rhs@(PH_MULTITAB _ _ _)) -> rhJoin future rhs lhs
      (PH_MULTITAB keySymb valSymbs mt, PH_SCALAR rhSymb rhv)
        | keySymb == rhSymb -> error "Handle multitab/scalar same"
        | elem rhSymb valSymbs -> error "Handle multitab value scalar"
        | otherwise -> Nothing

      (lhs@(PH_MULTITAB _ _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
      (PH_TAB lhFrom lhTo lht, PH_MULTITAB keySymb valSymbs mt)
        | lhFrom == keySymb && inFuture lhTo ->
            Just $ PH_MULTITAB lhFrom (lhTo:valSymbs)
                 $ AddToMultiTab lht mt
        | otherwise -> error "Handle all the cases before adding Nothing at end"


      (PH_ROWS lSymb lSort lRows, PH_ROWS rSymb rSort rRows) -> undefined

      (a, b) -> error $ "TODO: Unhandled rhJoin case: " <> show a <> " <--> "
                     <> show b

    inFuture = flip S.member future

-- constrainTo is temporary and should be renamed when we have full :find.
--
-- :find actually has a bunch of complex knobs you can turn which let it output
-- statistics like average, std.div, etc. So "just" constraining to rows is
-- wrong, and this should have a full :find
--
-- BUT FOR NOW, this will add output constraints into the resulting Plan, so
-- that in cases where we have to constrain values
constrainTo :: [Variable] -> PlanHolder -> PlanHolder
constrainTo _ p@(PH_SCALAR _ _) = p
constrainTo _ p@(PH_SET _ _) = p
constrainTo vars p@(PH_TAB from to plan)
  | S.member from varSet && S.member to varSet =
    PH_ROWS [from, to] [from, to] (TabToRows from to plan)
  | S.member from varSet =
    PH_ROWS [from] [from] (SetToRows from (TabKeySet from to plan))
  | S.member to varSet = error "Implement tab vals constrainTo"
  | otherwise = error "Useless plan"
  where
    varSet = S.fromList vars
constrainTo vars p@(PH_MULTITAB from tos mt) =
  PH_ROWS vars vars $ MultiTabToRows vars mt
