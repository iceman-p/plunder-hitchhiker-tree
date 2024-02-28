module Query.Test where

import           ClassyPrelude

import           Query.HitchhikerDatomStore
import           Query.NaiveEvaluator
import           Query.Planner
import           Query.Types

import qualified Data.Set                   as S
import qualified Data.Vector                as V

nationDemoDB :: Database
nationDemoDB = foldl' add emptyDB datoms
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

-- {-
-- fullStupidEvalTest =
--   stupidEvaluator
--     db
--     [ROWS [VAR "?alias"] [] [V.fromList [VAL_STR "fred"]]]
--     []
--     [DataPattern (LC_XAZ (VAR "?e") (ATTR ":aka") (VAR "?alias")),
--      DataPattern (LC_XAZ (VAR "?e") (ATTR ":nation") (VAR "?nation"))]
--     [VAR "?nation"]
-- -}


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

-- {-
-- fullDerpTagPlan =
--   stupidEvaluator
--     derpdb
--     [ROWS [VAR "?tag"] [] [
--         V.fromList [VAL_STR "twilight sparkle"],
--         V.fromList [VAL_STR "cute"]],
--      ROWS [VAR "?amount"] [] [V.fromList [VAL_INT 100]]]
--     []
--     [DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/tag") (VAR "?tag")),
--      DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/upvotes") (VAR "?upvotes")),
--      PredicateExpression (PREDICATE (PredBuiltin B_GT) [
--                              ARG_VAR (VAR "?upvotes"),
--                              ARG_VAR (VAR "?amount")]),
--      DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")),
--      DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/thumburl") (VAR "?thumburl"))]
--     [VAR "?derpid", VAR "?thumburl"]
-- -}



-- -----------------------------------------------------------------------

-- (db/q '[:find ?amount
--         :in $ ?min
--         :where
--         [?e :has ?amount]
--         [(> ?min ?amount)]
--        db
--        100])

-- Simple database of numbers for testing predicates
countdb :: Database
countdb = foldl' add emptyDB datoms
  where
    add db (e, a, v, tx, op) =
      learn (VAL_ENTID $ ENTID e, VAL_ATTR $ ATTR a, v, tx, op) db

    datoms = map mkRow [95..105]
    mkRow i = (1, ":has", VAL_INT i, 100, True)

countStupid = naiveEvaluator
  countdb
  [ROWS [VAR "?min"] [] [V.fromList [VAL_INT 100]]]
  []
  [DataPattern (LC_XAZ (VAR "?e") (ATTR ":has") (VAR "?amount")),
   BiPredicateExpression B_GT (ARG_VAR (VAR "?min")) (ARG_VAR (VAR "?amount"))]
  [VAR "?amount"]

-- TODO: Now replicate with the real evaluator!

countPlan = mkPlan
  [B_SCALAR (VAR "?min")]
  [DataPattern (LC_XAZ (VAR "?e") (ATTR ":has") (VAR "?amount"))]
  (S.singleton (VAR "?amount"))
