module Query.Test where

import           ClassyPrelude

import           Query.HitchhikerDatomStore
import           Query.NaiveEvaluator
import           Query.PlanEvaluator
import           Query.Planner
import           Query.Types
import           Types

import qualified Data.Map                   as M
import qualified Data.Set                   as S
import qualified Data.Vector                as V

import qualified HitchhikerSet              as HS

-- nationDemoDB :: Database
-- nationDemoDB = foldl' add emptyDB datoms
--   where
--     add db (e, a, v, tx, op) =
--       learn (ENTID e, VAL_ATTR $ ATTR a, VAL_STR v, tx, op) db

--     datoms = [
--       (1, ":name", "Frege", 100, True),
--       (1, ":nation", "France", 100, True),
--       (1, ":aka", "foo", 100, True),
--       (1, ":aka", "fred", 100, True),
--       (2, ":name", "Peirce", 100, True),
--       (2, ":nation", "France", 100, True),
--       (3, ":name", "De Morgan", 100, True),
--       (3, ":nation", "English", 100, True)
--       ]

--     dbWithAttrs =
--       learnAttribute ":name" True



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
derpdb = foldl' add dbWithAttrs records
  where
    -- We're going to make
    add :: Database -> [(Text, Value)] -> Database
    add db attrsVals =
      let datoms = map (\(t,v) -> mkDatom (attributes db) t v) attrsVals
      in learns datoms db

    mkDatom m t v = (TMPREF 0, lookupName m t, v, True)

    lookupName m t = case M.lookup t m of
      Nothing    -> error "Bad test data"
      Just entid -> entid

    records = [
      -- Good image
      [(":derp/tag", VAL_STR "twilight sparkle"),
       (":derp/tag", VAL_STR "cute"),
       (":derp/tag", VAL_STR "tea"),
       (":derp/upvotes", VAL_INT 200),
       (":derp/id", VAL_INT 1020),
       (":derp/thumburl", VAL_STR "//cdn/1020.jpg")],

      -- Not as good image
      [(":derp/tag", VAL_STR "twilight sparkle"),
       (":derp/tag", VAL_STR "cute"),
       (":derp/tag", VAL_STR "kite"),
       (":derp/upvotes", VAL_INT 31),
       (":derp/id", VAL_INT 1283),
       (":derp/thumburl", VAL_STR "//cdn/1283.jpg")],

      -- Good image about a different subject
      [(":derp/tag", VAL_STR "pinkie pie"),
       (":derp/upvotes", VAL_INT 9000),
       (":derp/id", VAL_INT 1491),
       (":derp/thumburl", VAL_STR "//cdn/1491.jpg")],

      -- Bad image about a different subject
      [(":derp/tag", VAL_STR "starlight glimmer"),
       (":derp/upvotes", VAL_INT 1),
       (":derp/id", VAL_INT 2041),
       (":derp/thumburl", VAL_STR "//cdn/2041.jpg")]
      ]

    -- database that's learned the attributes we're using
    dbWithAttrs =
      learnAttribute ":derp/tag" True MANY VT_STR $
      learnAttribute ":derp/upvotes" False ONE VT_INT $
      learnAttribute ":derp/id" True ONE VT_INT $
      learnAttribute ":derp/thumburl" False ONE VT_STR $
      emptyDB

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

fullDerpClauses = [
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/tag") (VAR "?tag")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/upvotes") (VAR "?upvotes")),
  BiPredicateExpression B_GT (ARG_VAR (VAR "?upvotes"))
                             (ARG_VAR (VAR "?amount")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/thumburl") (VAR "?thumburl"))]

fullDerpNaiveResult =
  naiveEvaluator
    derpdb
    [ROWS [VAR "?tag"] [] [
        V.fromList [VAL_STR "twilight sparkle"],
        V.fromList [VAL_STR "cute"]],
     ROWS [VAR "?amount"] [] [V.fromList [VAL_INT 100]]]
    []
    fullDerpClauses
    [VAR "?derpid", VAR "?thumburl"]

fullDerpPlan = mkPlan
  [derpdb]
  [B_COLLECTION (VAR "?tag"), B_SCALAR (VAR "?amount")]
  fullDerpClauses
  [VAR "?derpid", VAR "?thumburl"]

fullDerpOut = evalPlan
  [REL_SET $ RSET (VAR "?tag")
                  (HS.fromSet twoThreeConfig $ S.fromList $ map VAL_STR ["twilight sparkle", "cute"]),
   REL_SCALAR $ RSCALAR (VAR "?amount") (VAL_INT 100)]
  derpdb
  fullDerpPlan

-- -----------------------------------------------------------------------

-- -- (db/q '[:find ?amount
-- --         :in $ ?min
-- --         :where
-- --         [?e :has ?amount]
-- --         [(> ?min ?amount)]
-- --        db
-- --        100])

-- -- Simple database of numbers for testing predicates
-- countdb :: Database
-- countdb = foldl' add emptyDB datoms
--   where
--     add db (e, a, v, tx, op) =
--       learn (VAL_ENTID $ ENTID e, VAL_ATTR $ ATTR a, v, tx, op) db

--     datoms = map mkRow [95..105]
--     mkRow i = (1, ":has", VAL_INT i, 100, True)

-- countStupid = naiveEvaluator
--   countdb
--   [ROWS [VAR "?min"] [] [V.fromList [VAL_INT 100]]]
--   []
--   [DataPattern (LC_XAZ (VAR "?e") (ATTR ":has") (VAR "?amount")),
--    BiPredicateExpression B_GT (ARG_VAR (VAR "?min")) (ARG_VAR (VAR "?amount"))]
--   [VAR "?amount"]

-- -- TODO: Now replicate with the real evaluator!

-- -- countPlan = mkPlan
-- --   [B_SCALAR (VAR "?min")]
-- --   [DataPattern (LC_XAZ (VAR "?e") (ATTR ":has") (VAR "?amount")),
-- --    BiPredicateExpression B_GT (ARG_VAR (VAR "?min")) (ARG_VAR (VAR "?amount"))]
-- --   (S.singleton (VAR "?amount"))

-- countPlan = mkPlan
--   [B_SCALAR (VAR "?min")]
--   [DataPattern (LC_XAZ (VAR "?e") (ATTR ":has") (VAR "?amount")),
--    BiPredicateExpression B_GT (ARG_VAR (VAR "?min")) (ARG_VAR (VAR "?amount"))]
-- --   BiPredicateExpression B_GT (ARG_CONST (VAL_INT 100)) (ARG_VAR (VAR "?amount"))]
--   [VAR "?amount"]

-- countOut = evalPlan
--   [REL_SCALAR $ RSCALAR (VAR "?min") (VAL_INT 100)]
--   countdb
--   countPlan
