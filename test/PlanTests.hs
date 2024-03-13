module PlanTests where

import           ClassyPrelude

import           Control.DeepSeq
import           System.Random
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Types

import           Query.HitchhikerDatomStore
import           Query.NaiveEvaluator
import           Query.PlanEvaluator
import           Query.Planner
import           Query.Rows
import           Query.Types
import           Types

import qualified Data.Map                   as M
import qualified Data.Set                   as S

instance Arbitrary BuiltinPred where
  arbitrary = arbitraryBoundedEnum

testEmptyDB :: Database
testEmptyDB =
  learnAttribute ":t/count" False ONE VT_INT $
  learnAttribute ":t/name" False ONE VT_STR $
  emptyDB

addToDB :: Database -> [(Text, Value)] -> Database
addToDB db attrsVals = learns datoms db
  where
    datoms = map (\(t,v) -> mkDatom (attributes db) t v) attrsVals
    mkDatom m t v = (TMPREF 0, lookupName m t, v, True)
    lookupName m t = case M.lookup t m of
      Nothing    -> error "Bad test data"
      Just entid -> entid

testPairToVals :: (String, Int) -> [(Text, Value)]
testPairToVals (name, count) = [
  (":t/count", VAL_INT count),
  (":t/name", VAL_STR name)
  ]


equivalentRun :: Database -> [Binding] -> [Relation] -> [Clause] -> [Variable]
              -> Bool
equivalentRun db bindingInputs input clauses target =
  if sort planOut.values == sort naiveOut.values
  then True
  else trace ("UNEQUIVALENT:" <>
              "\nPLAN:  " <> show planOut <>
              "\nNAIVE: " <> show naiveOut) False
  where
    rowInput = map relationToRows input
    naiveOut = naiveEvaluator db rowInput [] clauses target

    plan = mkPlan [db] bindingInputs clauses target
    planOut = evalPlan input db plan


-- Given a list of numbers
prop_plan_check_filter_lhs_eq :: [(String,Int)] -> BuiltinPred -> Int -> Bool
prop_plan_check_filter_lhs_eq vals pred pivot =
  equivalentRun valDB bindings input clauses target
  where
    valDB = foldl' addToDB testEmptyDB $ map testPairToVals vals
    bindings = [B_SCALAR (VAR "?pivot")]
    input = [REL_SCALAR $ RSCALAR (VAR "?pivot") (VAL_INT pivot)]
    target = [VAR "?name", VAR "?count"]

    clauses = [
      DataPattern (LC_XAZ (VAR "?e") (ATTR ":t/count") (VAR "?count")),
      BiPredicateExpression (ARG_VAR (VAR "?pivot")) pred
                             (ARG_VAR (VAR "?count")),
      DataPattern (LC_XAZ (VAR "?e") (ATTR ":t/name") (VAR "?name"))
      ]

prop_plan_check_filter_rhs_eq :: [(String,Int)] -> BuiltinPred -> Int -> Bool
prop_plan_check_filter_rhs_eq vals pred pivot =
  equivalentRun valDB bindings input clauses target
  where
    valDB = foldl' addToDB testEmptyDB $ map testPairToVals vals
    bindings = [B_SCALAR (VAR "?pivot")]
    input = [REL_SCALAR $ RSCALAR (VAR "?pivot") (VAL_INT pivot)]
    target = [VAR "?name", VAR "?count"]

    clauses = [
      DataPattern (LC_XAZ (VAR "?e") (ATTR ":t/count") (VAR "?count")),
      BiPredicateExpression (ARG_VAR (VAR "?count")) pred
                             (ARG_VAR (VAR "?pivot")),
      DataPattern (LC_XAZ (VAR "?e") (ATTR ":t/name") (VAR "?name"))
      ]

tests :: TestTree
tests =
  testGroup "Plan" [
    testProperty "Left random set predicate" prop_plan_check_filter_lhs_eq,
    testProperty "Right random set predicate" prop_plan_check_filter_rhs_eq
    ]
