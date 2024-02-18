module Planner where

import           ClassyPrelude

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

import           Data.Sorted

import qualified HitchhikerSet        as HS

import qualified Data.Map             as M
import qualified Data.Set             as S
import qualified Data.Vector          as V

import qualified Data.Kind

-- TAKE 2 of all the things related to planning.

-- A set of
data Binding
  = B_SCALAR Symbol
  -- | B_TUPLE [Symbol]
  | B_COLLECTION Symbol
  -- | B_RELATION

data RelScalar = RSCALAR { sym :: Symbol, val :: Value }
data RelSet = RSET { sym :: Symbol, val :: HitchhikerSet Value}
data RelTab = RTAB { from :: Symbol
                   , to   :: Symbol
                   , val  :: HitchhikerSetMap Value Value}

data Relation
  = REL_SCALAR RelScalar
  | REL_SET RelSet
  | REL_TAB RelTab

data Type :: Data.Kind.Type -> Data.Kind.Type where
  TScalar :: Type RelScalar
  TSet    :: Type RelSet
  TTab    :: Type RelTab

data RowLookup
  = USE_EAV
  | USE_AEV
  | USE_AVE
  | USE_VAE
  deriving (Show)

-- Plan the steps to evaluate the query. This is the intermediate form of a
-- query: it's dumpable to the console for debugging.
data Plan :: Data.Kind.Type -> Data.Kind.Type where
  InputScalar :: Symbol -> Int -> Plan RelScalar
  InputSet    :: Symbol -> Int -> Plan RelSet

  LoadTab :: RowLookup -> Value -> Symbol -> Symbol -> Plan RelTab

  TabScalarLookup :: Plan RelScalar -> Plan RelTab -> Plan RelSet
  TabSetLookup :: Plan RelSet -> Plan RelTab -> Plan RelSet
  TabRestrictKeys :: Plan RelTab -> Plan RelSet -> Plan RelTab
  TabKeySet :: Plan RelTab -> Plan RelSet

  SetJoin :: Plan RelSet -> Plan RelSet -> Plan RelSet

instance Show (Plan a) where
  show (InputScalar sym int) = "InputScalar " <> show sym <> " " <> show int
  show (InputSet sym int) = "InputSet " <> show sym <> " " <> show int
  show (LoadTab rl val from to) = "LoadTab " <> show rl <> " " <> show val <>
                                  " " <> show from <> " " <> show to
  show (TabScalarLookup a b) = "TabScalarLookup (" <> show a <> ") (" <> show b <> ")"
  show (TabSetLookup a b) = "TabSetLookup (" <> show a <> ") (" <> show b <> ")"
  show (TabRestrictKeys tab set) = "TabRestrictKeys (" <> show tab <> ") (" <>
                                   show set <> ")"
  show (TabKeySet tab) = "TabKeySet (" <> show tab <> ")"
  show (SetJoin a b) = "SetJoin (" <> show a <> ") (" <> show b <> ")"

printableLookupToFunc :: RowLookup
                      -> (Database -> EAVRows Value Value Value Int)
printableLookupToFunc USE_EAV = eav
printableLookupToFunc USE_AEV = aev
printableLookupToFunc USE_AVE = ave
printableLookupToFunc USE_VAE = vae

evalPlan :: [Relation] -> Database -> Plan a -> a
evalPlan inputs db = go
  where
    go (InputSet _ i) = case atMay inputs i of
      Just (REL_SET rs) -> rs
      _                 -> error "Bad InputSet declaration"

    go (SetJoin a b) =
      let ea = go a
          eb = go b
      in if ea.sym == eb.sym
         then RSET ea.sym $ HS.intersection ea.val eb.val
         else error "Bad plan: comparing setjoin-ing two different types"

    go (LoadTab which lookupVal from to) =
      let val = partialLookup lookupVal (printableLookupToFunc which $ db)
      in RTAB {from,to,val}

{-
    go (TabScalarLookup val tab) = lookup
  TabScalarLookup :: Plan RelScalar -> Plan RelTab -> Plan RelSet
-}


-- How do we go about making a plan? Stupid simple query:
--    (sut/q '[:find ?nation
--             :in $ ?alias
--             :where
--             [?e :aka ?alias]
--             [?e :nation ?nation]]
--           (sut/db conn1)
--           "fred")))
--
-- A: [B_SCALAR "?alias"]
-- B: [C_XAZ ?e :aka ?alias,
-- C:  C_XAZ ?e :nation ?nation]
--
-- A: InputScalar 0
-- B: TabScalarLookup <A> (LoadTab db func ...)
-- C:

-- TabScalarLookup (InputScalar 0) (LoadTab db func

planOut = mkPlan
  [B_SCALAR (SYM "?alias")]
  [C_XAZ (SYM "?e") (ATTR ":aka") (SYM "?alias"),
   C_XAZ (SYM "?e") (ATTR ":nation") (SYM "?nation")]
  (S.singleton (SYM "?nation"))

{-
planOutVal = PH_SET (SYM "?nation")
  TabSetLookup
    (TabScalarLookup
      (InputScalar SYM "?alias" 0)
      (LoadTab USE_AVE VAL_ATTR (ATTR ":aka") SYM "?alias" SYM "?e"))
    (LoadTab USE_AEV VAL_ATTR (ATTR ":nation") SYM "?e" SYM "?nation")
-}

-- All symbols bound by a clause
clauseBinds :: Clause -> Set Symbol
clauseBinds (C_EAZ _ _ z) = S.fromList [z]
-- C_EYV
clauseBinds (C_XAV x _ _) = S.fromList [x]
clauseBinds (C_EYZ _ y z) = S.fromList [y, z]
clauseBinds (C_XAZ x _ z) = S.fromList [x, z]
clauseBinds (C_XYV x y _) = S.fromList [x, y]


-- TODO: Symbols should actually only be referred to below, during mkPlan. We
-- don't actually care about symbols during the execution of the Plan (though
-- we might still keep them there for debug dumping purposes).

-- Type to hold the different possible Plans while
data PlanHolder
  = PH_SCALAR Symbol (Plan RelScalar)
  | PH_SET Symbol (Plan RelSet)
  | PH_TAB Symbol Symbol (Plan RelTab)
  deriving (Show)

planHolderBinds :: PlanHolder -> Set Symbol
planHolderBinds (PH_SCALAR s _) = S.singleton s
planHolderBinds (PH_SET s _)    = S.singleton s

data Direction
  = FORWARDS
  | BACKWARDS

mkPlan :: [Binding] -> [Clause] -> Set Symbol -> PlanHolder
mkPlan bindingInputs clauses target = go startingInputs clauses
  where
    go :: [PlanHolder] -> [Clause] -> PlanHolder
    go [r] [] = r
    go inputs [] = error "Plan did not converge to one relation"
    go inputs (c:cs) =
      let past = pastProvides inputs
          future = futureNeeds cs
      in case c of
        (C_XAZ eSymb attr vSymb) ->
          -- OK, now we need to know a few things: in which direction are we
          -- building? e->v or v->e?
          let load = case direction eSymb vSymb past future of
                       FORWARDS  -> PH_TAB eSymb vSymb
                                  $ LoadTab USE_AEV (VAL_ATTR attr) eSymb vSymb
                       BACKWARDS -> PH_TAB vSymb eSymb
                                  $ LoadTab USE_AVE (VAL_ATTR attr) vSymb eSymb
          in go (joinAll (rhJoin future) (load:inputs)) cs

    startingInputs = map bindToPlan $ zip [0..] bindingInputs

    bindToPlan (i, B_SCALAR symb)     = PH_SCALAR symb $ InputScalar symb i
    bindToPlan (i, B_COLLECTION symb) = PH_SET symb $ InputSet symb i

    direction :: Symbol -> Symbol -> Set Symbol -> Set Symbol -> Direction
    direction leftS rightS past future
      | S.member leftS past && S.member rightS future = FORWARDS
      | S.member leftS future && S.member rightS past = BACKWARDS
      | otherwise = error "TODO: Figure out complicated cases in direction"

    pastProvides :: [PlanHolder] -> Set Symbol
    pastProvides rs = S.unions (map planHolderBinds rs)

    futureNeeds :: [Clause] -> Set Symbol
    futureNeeds cs = S.unions (target:(map clauseBinds cs))

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

rhJoin :: Set Symbol -> PlanHolder -> PlanHolder -> Maybe PlanHolder
rhJoin future l r = case (l, r) of
  (PH_SET lhs lhv, PH_SET rhs rhv)
    | lhs == rhs -> Just $ PH_SET lhs $ SetJoin lhv rhv
    | otherwise  -> Nothing

  (lhs@(PH_SET _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
  (PH_TAB lhFrom lhTo lht, PH_SET rhSymb rhv)
    | lhFrom == rhSymb && inFuture lhFrom && inFuture lhTo ->
        Just $ PH_TAB lhFrom lhTo $ TabRestrictKeys lht rhv
    | lhFrom == rhSymb && inFuture lhFrom ->
        Just $ PH_SET lhFrom $ SetJoin (TabKeySet lht) rhv
    | lhFrom == rhSymb && inFuture lhTo ->
        Just $ PH_SET lhTo $ TabSetLookup rhv lht
    | lhFrom == rhSymb -> error "wtf is this case"
    | otherwise -> error "Handle all the lhTo cases."

  (lhs@(PH_SCALAR _ _), rhs@(PH_TAB _ _ _)) -> rhJoin future rhs lhs
  (PH_TAB lhFrom lhTo lhTab, PH_SCALAR rhSymb rhv)
    | rhSymb == lhFrom -> Just $ PH_SET lhTo $ TabScalarLookup rhv lhTab
    | rhSymb == lhTo -> error "Handle backwards scalar table matching."
    | otherwise -> Nothing

  (a, b) -> error $ "TODO: Unhandled rhJoin case: " <> show a <> " " <> show b
  where
    inFuture = flip S.member future


-- -----------------------------------------------------------------------


-- (db/q '[:find ?dbid ?thumburl
--         :in $ [?tag ...] ?amount
--         :where
--         [?e :derp/tag ?tag]
--         [?e :derp/upvotes ?upvotes]
--         [(> ?upvotes ?amount)]
--         [?e :derp/id ?derpid]
--         [?e :derp/thumburl ?thumburl]]
--        db
--        ["twilight sparkle" "cute"] 100)

-- Let's just hand write out each step with a justification.

-- available 0:(SET [?tag]) 1:(SCALAR ?amount)
-- clause [?e :derp/tag ?tag]
-- future [?e ?upvotes ?amount ?derpid ?thumburl]
-- outtype (SET [?e]) (SCALAR ?amount)
-- out 0:(UNION
--          JOIN_TAB_SET
--         (LOAD_AV :derp/tag db)
--     1:(SCALAR ?amount)
--
-- because ?tag isn't used in the future, the plan should realize this step can
-- consume it and use it




-- -- Executes a plan which
-- --
-- execPlan :: [Relation] -> Plan a -> Relation
-- execPlan xs (P_FROM_INPUT_SET i) = case atMay xs i of
--   Nothing -> error "Invalid plan"
--   Just r -





-- OK, technically, you should be able to pass in "clauses" in any order. But
-- that doesn't



-- TODO: The entire BITAB framing from the first sketch feels weird here when
-- doing a query planner: we want to know exactly which way to scan the data to
-- try to calculate

-- -- I can visually see that the tag search query above would become something
-- -- like the below, but I don't see how you write an algorithm to do this end to
-- -- end.
-- fullq db tagset amount = P_TO_ROW
--   (P_JOIN_BITAB_TAB
--     -- [?e :derp/thumburl ?thumburl]
--     (P_LOAD_TAB (C_XAZ (SYM "?e") (ATTR ":derp/thumburl") (SYM "?thumburl")) db)

--     (P_JOIN_BITAB_SET
--       -- [?e :derp/id ?derpid]
--       (P_LOAD_TAB (C_XAZ (SYM "?e") (ATTR ":derp/id") (SYM "?derpid")) db)

--       -- range filter.
--       (P_SELECT_SET (SYM "?e")
--         (P_RANGE_FILTER_COL RT_GT (SYM "?upvotes") amount
--           -- ?e->?upvotes
--           (P_SORT_BY [(SYM "?upvotes")]
--             (P_TO_ROW
--               (P_JOIN_BITAB_SET
--                 -- [?e :derp/upvotes ?upvotes]
--                 (P_LOAD_TAB
--                   (C_XAZ (SYM "?e") (ATTR ":derp/upvotes") (SYM "?upvotes")) db)
--                 -- (select ?e's which have all tags)
--                 (P_SELECT_SET (SYM "?e")
--                  (P_JOIN_BITAB_SET
--                    (P_LOAD_TAB
--                      (C_XAZ (SYM "?e") (ATTR ":derp/tag") (SYM "?tag")) db)
--                    (P_FROM_SET tagset))))))))))


-- OK, but how do you build the above plan in the first place?

-- mkPlan :: [Symbol] -> [Clause] -> [Symbol] -> Plan a
-- mkPlan starting clauses target = undefined





-- mkPlan :: [Binding] -> [Clause] -> [Symbol] -> Plan
-- mkPlan starting clauses target =

  -- NEXT ACTION: Build a query planner that narrows everything down.
