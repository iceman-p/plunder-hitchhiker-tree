module Planner where

import           ClassyPrelude

import           Data.Map                   (Map)
import           Data.Set                   (Set)

import           Safe                       (atMay, tailSafe)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import           Query.HitchhikerDatomStore
import           Query.PlanEvaluator
import           Query.Types

import           Data.Sorted



import qualified HitchhikerMap              as HM
import qualified HitchhikerSet              as HS
import qualified HitchhikerSetMap           as HSM

import qualified Data.Map                   as M
import qualified Data.Set                   as S
import qualified Data.Vector                as V

-- TAKE 2 of all the things related to planning.

-- -----------------------------------------------------------------------



-- TODO: Variables should actually only be referred to below, during mkPlan. We
-- don't actually care about symbols during the execution of the Plan (though
-- we might still keep them there for debug dumping purposes).

-- data Direction
--   = FORWARDS
--   | BACKWARDS
--   | LEFT_ONLY
--   | RIGHT_ONLY
--   | IRRELEVANT

-- mkPlan :: [Binding] -> [Clause] -> Set Variable -> PlanHolder
-- mkPlan bindingInputs clauses target =
--   converge $ filter targetNeeds $ go startingInputs clauses
--   where
--     converge :: [PlanHolder] -> PlanHolder
--     converge [r] = r
--     -- TODO: Since we've filtered out any irrelevant relations, this should be
--     -- turned into a Cartesian product of the remaining relations projected
--     -- into the target set.
--     converge inputs = error $ "Plan did not converge to one relation: "
--                            <> show inputs

--     targetNeeds :: PlanHolder -> Bool
--     targetNeeds ph = (S.intersection (planHolderBinds ph) target) /= S.empty

--     go :: [PlanHolder] -> [Clause] -> [PlanHolder]
--     go ph [] = ph
--     go inputs (c:cs) =
--       let past = pastProvides inputs
--           future = futureNeeds cs
--       in case c of
--         (DataPattern (LC_XAZ eSymb attr vSymb)) ->
--           case direction eSymb vSymb past future of
--             FORWARDS  ->
--               let load = PH_TAB eSymb vSymb
--                        $ LoadTab USE_AEV (VAL_ATTR attr) eSymb vSymb
--               in go (joinAll (rhJoin future) (load:inputs)) cs
--             BACKWARDS ->
--               let load = PH_TAB vSymb eSymb
--                        $ LoadTab USE_AVE (VAL_ATTR attr) vSymb eSymb
--               in go (joinAll (rhJoin future) (load:inputs)) cs
--             LEFT_ONLY ->
--               let load = PH_SET eSymb
--                        $ TabKeySet
--                        $ LoadTab USE_AEV (VAL_ATTR attr) eSymb vSymb
--               in go (joinAll (rhJoin future) (load:inputs)) cs
--             RIGHT_ONLY -> error "RIGHT_ONLY todo"
--             IRRELEVANT -> go inputs cs

--     startingInputs = map bindToPlan $ zip [0..] bindingInputs

--     bindToPlan (i, B_SCALAR symb)     = PH_SCALAR symb $ InputScalar symb i
--     bindToPlan (i, B_COLLECTION symb) = PH_SET symb $ InputSet symb i

--     direction :: Variable -> Variable -> Set Variable -> Set Variable -> Direction
--     direction leftS rightS past future
--       | S.member leftS future && S.member rightS future =
--           -- FORWARDS is a cop out, we need to make a decision about what
--           -- the best direction to go in is now.
--           -- error "Both future"
--           FORWARDS
--       | S.member leftS past && S.member rightS future = FORWARDS
--       | S.member leftS future && S.member rightS past = BACKWARDS
--       | S.member leftS future = LEFT_ONLY
--       | S.member rightS future = RIGHT_ONLY
--       | otherwise = IRRELEVANT
-- --        error $ "TODO: Figure out complicated cases in direction: " <> show leftS <> " " <> show rightS <> " " <> show past <> " " <> show future

--     pastProvides :: [PlanHolder] -> Set Variable
--     pastProvides rs = S.unions (map planHolderBinds rs)

--     futureNeeds :: [Clause] -> Set Variable
--     futureNeeds cs = S.unions (target:(map clauseUses cs))

-- -- -----------------------------------------------------------------------

-- -- Given a joining function which may or may not join two elements, run all
-- -- permutations of all elements of with the associative function `f`. If `f`
-- -- returns Just, replaces both elements with the newly joined element and
-- -- starts over, returning only when no more joins can be made.
-- joinAll :: (a -> a -> Maybe a) -> [a] -> [a]
-- joinAll _ [] = []
-- joinAll _ [x] = [x]
-- joinAll f (x:xs) = joinOuter x xs []
--   where
--     joinOuter x [] prev     = reverse (x:prev)
--     joinOuter x yo@(y:ys) prev = case joinInner x yo [] of
--       Nothing              -> joinOuter y ys (x:prev)
--       Just (new, ysMinusY) -> joinAll f (reverse (new:prev) ++ ysMinusY)

--     joinInner x [] prev = Nothing
--     joinInner x (y:ys) prev = case f x y of
--       Nothing  -> joinInner x ys (y:prev)
--       Just new -> Just (new, reverse prev ++ ys)

-- rhJoin :: Set Variable -> PlanHolder -> PlanHolder -> Maybe PlanHolder
-- rhJoin future l r = case (l, r) of
--   (PH_SET lhs lhv, PH_SET rhs rhv)
--     | lhs == rhs -> Just $ PH_SET lhs $ SetJoin lhv rhv
--     | otherwise  -> Nothing

--   (lhs@(PH_SCALAR _ _), rhs@(PH_SET _ _)) -> rhJoin future rhs lhs
--   (PH_SET lhs lhv, PH_SCALAR rhs rhv)
--     | lhs == rhs -> Just $ PH_SET lhs $ SetScalarJoin lhv rhv
--     | otherwise -> Nothing

--   (lhs@(PH_SET _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
--   (PH_TAB lhFrom lhTo lht, PH_SET rhSymb rhv)
--     | lhFrom == rhSymb && inFuture lhFrom && inFuture lhTo ->
--         Just $ PH_TAB lhFrom lhTo $ TabRestrictKeys lht rhv
--     | lhFrom == rhSymb && inFuture lhFrom ->
--         Just $ PH_SET lhFrom $ SetJoin (TabKeySet lht) rhv
--     | lhFrom == rhSymb && inFuture lhTo ->
--         Just $ PH_SET lhTo $ TabSetUnionVals lhFrom rhv lhTo lht
--     | lhFrom == rhSymb -> error "wtf is this case"
--     | otherwise -> error "Handle all the lhTo cases."

--   (lhs@(PH_SCALAR _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
--   (PH_TAB lhFrom lhTo lhTab, PH_SCALAR rhSymb rhv)
--     | rhSymb == lhFrom ->
--       Just $ PH_SET lhTo $ TabScalarLookup lhFrom rhv lhTo lhTab
--     | rhSymb == lhTo -> error "Handle backwards scalar table matching."
--     | otherwise -> Nothing

--   (PH_TAB lhFrom lhTo lht, PH_TAB rhFrom rhTo rht)
--     | lhFrom == rhFrom -> Just $ PH_MULTITAB lhFrom [lhTo, rhTo]
--                                $ MkMultiTab lht rht
--     | otherwise -> error "Handle all the cases before adding Nothing at end"

--   (lhs@(PH_SCALAR _ _), rhs@(PH_MULTITAB _ _ _)) -> rhJoin future rhs lhs
--   (PH_MULTITAB keySymb valSymbs mt, PH_SCALAR rhSymb rhv)
--     | keySymb == rhSymb -> error "Handle multitab/scalar same"
--     | elem rhSymb valSymbs -> error "Handle multitab value scalar"
--     | otherwise -> Nothing

--   (a, b) -> error $ "TODO: Unhandled rhJoin case: " <> show a <> " <--> "
--                  <> show b
--   where
--     inFuture = flip S.member future

-- -----------------------------------------------------------------------


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

-- derpTagPlan = mkPlan
--   [B_COLLECTION (VAR "?tag"), B_SCALAR (VAR "?amount")]
--   [DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/tag") (VAR "?tag")),
--    DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/upvotes") (VAR "?upvotes"))
-- --  ,
--    -- C_PREDICATE (whole bundle of hurt)
--    -- DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")),
--    -- DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/thumbnail") (VAR "?thumburl"))
--   ]
--   -- (S.fromList [(VAR "?derpid"), (VAR "?thumburl")])
--   (S.fromList [(VAR "?e"), (VAR "?upvotes")])
-- --  (S.fromList [(VAR "?e")])

{-

PH_MULTITAB (VAR "?e") [SYM "?thumburl",SYM "?derpid"]
  MkMultiTab
    (LoadTab USE_AEV VAL_ATTR (ATTR ":derp/thumbnail") SYM "?e" SYM "?thumburl")
    (TabRestrictKeys
      (LoadTab USE_AEV VAL_ATTR (ATTR ":derp/id") SYM "?e" SYM "?derpid")
      (SetJoin
        (TabKeySet
          (LoadTab USE_AEV VAL_ATTR (ATTR ":derp/upvotes") SYM "?e" SYM "?upvotes"))
        (TabSetUnionVals (InputSet SYM "?tag" 0) (LoadTab USE_AVE VAL_ATTR (ATTR ":derp/tag") SYM "?tag" SYM "?e"))))
ghci>

-}



-- -----------------------------------------------------------------------

-- Next major question: how do predicates show up in this language? Datomic
-- appears to treat predicates as not clauses and bundles them into the
-- previous clause. That's kind of absurd. Our planner is still really taking
-- the clauses one at a time.
--
-- How should we be thinking about rjoin? Is the massive rjoin actually a
-- design mistake here?

-- The best time for the predicate to run is during the rjoin: in the case of
-- the above query, the C_XAZ does a load from the database, and now you have to join it to the
--
-- Maybe joinAll is wrong. Instead of just taking a list, there's a certain
-- thing that needs to be joined maximally into the
--
--


-- TODO: The entire BITAB framing from the first sketch feels weird here when
-- doing a query planner: we want to know exactly which way to scan the data to
-- try to calculate

-- -- I can visually see that the tag search query above would become something
-- -- like the below, but I don't see how you write an algorithm to do this end to
-- -- end.
-- fullq db tagset amount = P_TO_ROW
--   (P_JOIN_BITAB_TAB
--     -- [?e :derp/thumburl ?thumburl]
--     (P_LOAD_TAB (LC_XAZ (VAR "?e") (ATTR ":derp/thumburl") (VAR "?thumburl")) db)

--     (P_JOIN_BITAB_SET
--       -- [?e :derp/id ?derpid]
--       (P_LOAD_TAB (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")) db)

--       -- range filter.
--       (P_SELECT_SET (VAR "?e")
--         (P_RANGE_FILTER_COL RT_GT (VAR "?upvotes") amount
--           -- ?e->?upvotes
--           (P_SORT_BY [(VAR "?upvotes")]
--             (P_TO_ROW
--               (P_JOIN_BITAB_SET
--                 -- [?e :derp/upvotes ?upvotes]
--                 (P_LOAD_TAB
--                   (LC_XAZ (VAR "?e") (ATTR ":derp/upvotes") (VAR "?upvotes")) db)
--                 -- (select ?e's which have all tags)
--                 (P_SELECT_SET (VAR "?e")
--                  (P_JOIN_BITAB_SET
--                    (P_LOAD_TAB
--                      (LC_XAZ (VAR "?e") (ATTR ":derp/tag") (VAR "?tag")) db)
--                    (P_FROM_SET tagset))))))))))

-- -----------------------------------------------------------------------

exampleADB :: Database
exampleADB = foldl' add emptyDB datoms
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

-- How do we go about making a plan? Stupid simple query:
--    (sut/q '[:find ?nation
--             :in $ ?alias
--             :where
--             [?e :aka ?alias]
--             [?e :nation ?nation]]
--           (sut/db conn1)
--           "fred")))

-- exampleAPlanOut = mkPlan
--   [B_SCALAR (VAR "?alias")]
--   [DataPattern (LC_XAZ (VAR "?e") (ATTR ":aka") (VAR "?alias")),
--    DataPattern (LC_XAZ (VAR "?e") (ATTR ":nation") (VAR "?nation"))]
--   (S.singleton (VAR "?nation"))
{-
planOutVal = PH_SET (VAR "?nation")
  TabSetUnionVals
    (TabScalarLookup
      (InputScalar SYM "?alias" 0)
      (LoadTab USE_AVE VAL_ATTR (ATTR ":aka") SYM "?alias" SYM "?e"))
    (LoadTab USE_AEV VAL_ATTR (ATTR ":nation") SYM "?e" SYM "?nation")
-}

-- exampleAOut = evalPlan [REL_SCALAR $ RSCALAR (VAR "?alias") (VAL_STR "fred")]
--                        exampleADB
--                        exampleAPlanOut
{-
relOutVal = REL_SET (
  RSET {sym = SYM "?nation",
        val = HITCHHIKERSET {config = TREECONFIG,
                             root = Just (HitchhikerSetNodeLeaf
                                          (fromListN 1 [VAL_STR "France"]))}})
-}
