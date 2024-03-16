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
      -- Amazing image
      [(":derp/tag", VAL_STR "twilight sparkle"),
       (":derp/tag", VAL_STR "cute"),
       (":derp/tag", VAL_STR "tea"),
       (":derp/upvotes", VAL_INT 1500),
       (":derp/id", VAL_INT 8936),
       (":derp/thumburl", VAL_STR "//cdn/8936.jpg")],

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
       (":derp/tag", VAL_STR "twilight sparkle"),
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
--         :in $ [?tag ...] ?min ?max
--         :where
--         [?e :derp/tag ?tag]
--         [?e :derp/upvotes ?upvotes]
--         [(< ?min ?upvotes)]
--         [(< ?upvotes ?max)]
--         [?e :derp/id ?derpid]
--         [?e :derp/thumburl ?thumburl]]
--        db
--        ["twilight sparkle" "cute"] 100 500)

-- We want mid images: not too low scoring, not too high scoring:
fullDerpClauses = [
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/tag") (VAR "?tag")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/upvotes") (VAR "?upvotes")),
  BiPredicateExpression (ARG_VAR (VAR "?min")) B_LTE
                             (ARG_VAR (VAR "?upvotes")),
  BiPredicateExpression (ARG_VAR (VAR "?upvotes")) B_LTE (ARG_VAR (VAR "?max")),

  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/thumburl") (VAR "?thumburl"))]

fullDerpNaiveResult =
  naiveEvaluator
    derpdb
    [ROWS [VAR "?tag"] [] [
        V.fromList [VAL_STR "twilight sparkle"],
        V.fromList [VAL_STR "cute"]],
     ROWS [VAR "?min"] [] [V.fromList [VAL_INT 100]],
     ROWS [VAR "?max"] [] [V.fromList [VAL_INT 500]]]
    []
    fullDerpClauses
    [VAR "?derpid", VAR "?upvotes", VAR "?thumburl"]

fullDerpPlan = mkPlan
  [derpdb]
  [B_COLLECTION (VAR "?tag"), B_SCALAR (VAR "?min"), B_SCALAR (VAR "?max")]
  fullDerpClauses
  [VAR "?derpid", VAR "?upvotes", VAR "?thumburl"]

fullDerpOut = evalPlan
  [REL_SET $ RSET (VAR "?tag")
                  (HS.fromSet twoThreeConfig $ S.fromList $ map VAL_STR ["twilight sparkle", "cute"]),
   REL_SCALAR $ RSCALAR (VAR "?min") (VAL_INT 100),
   REL_SCALAR $ RSCALAR (VAR "?max") (VAL_INT 500)]
  derpdb
  (fromRight fullDerpPlan)

fromRight :: Either a b -> b
fromRight (Right a) = a
fromRight _         = error "fromRight: Left"

-- -----------------------------------------------------------------------

-- (db/q '[:find ?derpid
--         :where
--         [?e :derp/tag "twilight sparkle"]
--         (not [?e :derp/tag "starlight glimmer"])
--         [?e :derp/id ?derpid]
--        db)

notTestClauses = [
  DataPattern (LC_XAV (VAR "?e") (ATTR ":derp/tag")
                      (VAL_STR "twilight sparkle")),
  NotClause DataSourceDefault [
      DataPattern (LC_XAV (VAR "?e") (ATTR ":derp/tag")
                          (VAL_STR "starlight glimmer"))
      ],
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid"))
  ]

notTestPlan = mkPlan
  [derpdb]
  []
  notTestClauses
  [VAR "?derpid"]

notTestOut = evalPlan
  []
  derpdb
  (fromRight notTestPlan)

-- -----------------------------------------------------------------------

orTestClauses = [
  OrClause DataSourceDefault [
    OCB_CLAUSE $
      DataPattern (LC_XAV (VAR "?e") (ATTR ":derp/tag") (VAL_STR "tea")),
    OCB_CLAUSE $
      DataPattern (LC_XAV (VAR "?e") (ATTR ":derp/tag") (VAL_STR "kite"))
    ],
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid"))
  ]


orTestPlan = mkPlan
  [derpdb]
  []
  orTestClauses
  [VAR "?derpid"]

orTestOut = evalPlan
  []
  derpdb
  (fromRight orTestPlan)
