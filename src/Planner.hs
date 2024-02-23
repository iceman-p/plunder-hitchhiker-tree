module Planner where

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

import           Data.Sorted

import qualified HitchhikerMap        as HM
import qualified HitchhikerSet        as HS
import qualified HitchhikerSetMap     as HSM

import qualified Data.Map             as M
import qualified Data.Set             as S
import qualified Data.Vector          as V

import qualified Data.Kind

-- TAKE 2 of all the things related to planning.

mkTwoVector :: a -> a -> Vector a
mkTwoVector x y = V.fromList [x, y]

-- -----------------------------------------------------------------------

-- A set of
data Binding
  = B_SCALAR Symbol
  -- | B_TUPLE [Symbol]
  | B_COLLECTION Symbol
  -- | B_RELATION

data RelScalar = RSCALAR { sym :: Symbol, val :: Value }
  deriving (Show)
data RelSet = RSET { sym :: Symbol, val :: HitchhikerSet Value}
  deriving (Show)
data RelTab = RTAB { from :: Symbol
                   , to   :: Symbol
                   , val  :: HitchhikerSetMap Value Value}
  deriving (Show)

-- A table from one key symbol to multiple value symbols. Since so many queries
-- end with a series of lookups on a entity id, this often saves operations.
data RelMultiTab = RMTAB
  { from :: Symbol
  , to   :: [Symbol]
  , val  :: HitchhikerMap Value (Vector (HitchhikerSet Value))
  }
  deriving (Show)

data Relation
  = REL_SCALAR RelScalar
  | REL_SET RelSet
  | REL_TAB RelTab
  | REL_MULTITAB RelMultiTab
  deriving (Show)

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
  TabSetUnionVals :: Plan RelSet -> Plan RelTab -> Plan RelSet
  TabRestrictKeys :: Plan RelTab -> Plan RelSet -> Plan RelTab
  TabKeySet :: Plan RelTab -> Plan RelSet

  SetJoin :: Plan RelSet -> Plan RelSet -> Plan RelSet
  SetScalarJoin :: Plan RelSet -> Plan RelScalar -> Plan RelSet

  MkMultiTab :: Plan RelTab -> Plan RelTab -> Plan RelMultiTab

instance Show (Plan a) where
  show (InputScalar sym int) = "InputScalar " <> show sym <> " " <> show int
  show (InputSet sym int) = "InputSet " <> show sym <> " " <> show int
  show (LoadTab rl val from to) = "LoadTab " <> show rl <> " " <> show val <>
                                  " " <> show from <> " " <> show to
  show (TabScalarLookup a b) = "TabScalarLookup (" <> show a <> ") (" <> show b <> ")"
  show (TabSetUnionVals a b) = "TabSetUnionVals (" <> show a <> ") (" <> show b <> ")"
  show (TabRestrictKeys tab set) = "TabRestrictKeys (" <> show tab <> ") (" <>
                                   show set <> ")"
  show (TabKeySet tab) = "TabKeySet (" <> show tab <> ")"
  show (SetJoin a b) = "SetJoin (" <> show a <> ") (" <> show b <> ")"
  show (SetScalarJoin a b) = "SetScalarJoin (" <> show a <> ") (" <> show b <>
                             ")"
  show (MkMultiTab a b) = "MkMultiTab (" <> show a <> ") (" <> show b <> ")"

printableLookupToFunc :: RowLookup
                      -> (Database -> EAVRows Value Value Value Int)
printableLookupToFunc USE_EAV = eav
printableLookupToFunc USE_AEV = aev
printableLookupToFunc USE_AVE = ave
printableLookupToFunc USE_VAE = vae

evalPlan :: [Relation] -> Database -> PlanHolder -> Relation
evalPlan inputs db = runFromPlanHolder
  where
    runFromPlanHolder (PH_SCALAR _ s)     = REL_SCALAR $ go s
    runFromPlanHolder (PH_SET _ s)        = REL_SET $ go s
    runFromPlanHolder (PH_TAB _ _ t)      = REL_TAB $ go t
    runFromPlanHolder (PH_MULTITAB _ _ m) = REL_MULTITAB $ go m

    go :: Plan a -> a

    go (InputScalar _ i) = case atMay inputs i of
      Just (REL_SCALAR rs) -> rs
      _                    -> error "Input doesn't match plan in InputScalar"
    go (InputSet _ i) = case atMay inputs i of
      Just (REL_SET rs) -> rs
      _                 -> error "Input doesn't match plan in InputSet"

    go (LoadTab which lookupVal from to) =
      let val = partialLookup lookupVal (printableLookupToFunc which $ db)
      in RTAB {from,to,val}

    go (TabScalarLookup pscalar ptab) =
      let (RSCALAR sym val) = go pscalar
          (RTAB from to tab) = go ptab
      in RSET to $ HSM.lookup val tab

    go (TabSetUnionVals pset ptab) =
      let (RSET sym set) = go pset
          (RTAB from to tab) = go ptab

          -- TODO: This implementation isn't optimal and needs to be replaced,
          -- but is OK for bootstrapping. What we really want is to use the
          -- flattened set segments to traverse the structure of the setmap/tab.
          toSetList = map (\v -> HSM.lookup v tab) $ S.toList $ HS.toSet set
          asSet = case toSetList of
            []  -> HS.empty $ HS.getConfig set
            [x] -> x
            xs  -> foldl1' HS.union xs
      in RSET to asSet

    go (TabRestrictKeys ptab pset) =
      let (RTAB from to tab) = go ptab
          (RSET sym set) = go pset
      in RTAB from to $ HSM.restrictKeys set tab

    go (TabKeySet ptab) =
      let (RTAB from _ tab) = go ptab
      in RSET from $ HSM.toKeySet tab

    go (SetJoin pa pb) =
      let ea = go pa
          eb = go pb
      in if ea.sym == eb.sym
         then RSET ea.sym $ HS.intersection ea.val eb.val
         else error "Bad plan: comparing setjoin-ing two different types"

    go (SetScalarJoin pset pscalar) =
      let eset = go pset
          escalar = go pscalar
          config = HS.getConfig eset.val
      in RSET eset.sym $ if HS.member escalar.val eset.val
                         then HS.singleton config escalar.val
                         else HS.empty config

    go (MkMultiTab plhs prhs) =
      let elhs = go plhs
          erhs = go prhs
          hml = HSM.toHitchhikerMap elhs.val
          hmr = HSM.toHitchhikerMap erhs.val
          target = HM.intersectionWith mkTwoVector hml hmr
      in if elhs.from == erhs.from
         then RMTAB elhs.from [elhs.to, erhs.to] target
         else error "Bad plan: multi-tab join of two different key symbols"

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
  | PH_MULTITAB Symbol [Symbol] (Plan RelMultiTab)
  deriving (Show)

planHolderBinds :: PlanHolder -> Set Symbol
planHolderBinds (PH_SCALAR s _)          = S.singleton s
planHolderBinds (PH_SET s _)             = S.singleton s
planHolderBinds (PH_TAB from to _)       = S.fromList [from, to]
planHolderBinds (PH_MULTITAB from tos _) = S.fromList (from:tos)

data Direction
  = FORWARDS
  | BACKWARDS
  | LEFT_ONLY
  | RIGHT_ONLY
  | IRRELEVANT

mkPlan :: [Binding] -> [Clause] -> Set Symbol -> PlanHolder
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
        (C_XAZ eSymb attr vSymb) ->
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
            RIGHT_ONLY -> error "RIGHT_ONLY todo"
            IRRELEVANT -> go inputs cs

    startingInputs = map bindToPlan $ zip [0..] bindingInputs

    bindToPlan (i, B_SCALAR symb)     = PH_SCALAR symb $ InputScalar symb i
    bindToPlan (i, B_COLLECTION symb) = PH_SET symb $ InputSet symb i

    direction :: Symbol -> Symbol -> Set Symbol -> Set Symbol -> Direction
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

  (a, b) -> error $ "TODO: Unhandled rhJoin case: " <> show a <> " <--> "
                 <> show b
  where
    inFuture = flip S.member future

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

derpTagPlan = mkPlan
  [B_COLLECTION (SYM "?tag"), B_SCALAR (SYM "?amount")]
  [C_XAZ (SYM "?e") (ATTR ":derp/tag") (SYM "?tag"),
   C_XAZ (SYM "?e") (ATTR ":derp/upvotes") (SYM "?upvotes"),
   -- C_PREDICATE (whole bundle of hurt)
   C_XAZ (SYM "?e") (ATTR ":derp/id") (SYM "?derpid"),
   C_XAZ (SYM "?e") (ATTR ":derp/thumbnail") (SYM "?thumburl")]
  (S.fromList [(SYM "?derpid"), (SYM "?thumburl")])

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

-- Pre planning.
--
-- Given a list of raw clauses, how do you plan out the

data Source
  = SourceDefault

-- "Input" Clause: A new type of clause with everything in it.
data IClause
  = NotClause
  | NotJoinClause
  | OrClause
  | OrJoinClause

  -- ExpressionClause
  | DataPatern
  | PredicateExpression
  | FunctionExpression
  | RuleExpression








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

exampleAPlanOut = mkPlan
  [B_SCALAR (SYM "?alias")]
  [C_XAZ (SYM "?e") (ATTR ":aka") (SYM "?alias"),
   C_XAZ (SYM "?e") (ATTR ":nation") (SYM "?nation")]
  (S.singleton (SYM "?nation"))
{-
planOutVal = PH_SET (SYM "?nation")
  TabSetUnionVals
    (TabScalarLookup
      (InputScalar SYM "?alias" 0)
      (LoadTab USE_AVE VAL_ATTR (ATTR ":aka") SYM "?alias" SYM "?e"))
    (LoadTab USE_AEV VAL_ATTR (ATTR ":nation") SYM "?e" SYM "?nation")
-}

exampleAOut = evalPlan [REL_SCALAR $ RSCALAR (SYM "?alias") (VAL_STR "fred")]
                       exampleADB
                       exampleAPlanOut
{-
relOutVal = REL_SET (
  RSET {sym = SYM "?nation",
        val = HITCHHIKERSET {config = TREECONFIG,
                             root = Just (HitchhikerSetNodeLeaf
                                          (fromListN 1 [VAL_STR "France"]))}})
-}
