module Query.PlanEvaluator (evalPlan) where

import           ClassyPrelude

import           Data.List                  (foldl1')

import           Query.HitchhikerDatomStore
import           Query.Rows
import           Query.Types
import           Types

import           Safe                       (atMay)

import qualified HitchhikerMap              as HM
import qualified HitchhikerSet              as HS
import qualified HitchhikerSetMap           as HSM

import qualified Data.List                  as L
import qualified Data.Set                   as S
import qualified Data.Vector                as V

mkTwoVector :: a -> a -> Vector a
mkTwoVector x y = V.fromList [x, y]

pushVector :: a -> Vector a -> Vector a
pushVector = V.cons

data EvalBiPred
  = EBP_LEFT Value BuiltinPred
  | EBP_RIGHT BuiltinPred Value
  deriving (Show)


evalPlan :: [Relation] -> Database -> PlanHolder -> Rows
evalPlan inputs db = relationToRows . runFromPlanHolder
  where
    runFromPlanHolder (PH_SCALAR _ s)     = REL_SCALAR $ go s
    runFromPlanHolder (PH_SET _ s)        = REL_SET $ go s
    runFromPlanHolder (PH_TAB _ _ t)      = REL_TAB $ go t
    runFromPlanHolder (PH_ROWS _ _ r)     = REL_ROWS $ go r
    runFromPlanHolder (PH_MULTITAB _ _ m) = REL_MULTITAB $ go m

    go :: Plan a -> a

    go (InputScalar _ i) = case atMay inputs i of
      Just (REL_SCALAR rs) -> rs
      x                    -> error $ "Input doesn't match plan in InputScalar"
    go (InputSet _ i) = case atMay inputs i of
      Just (REL_SET rs) -> rs
      _                 -> error "Input doesn't match plan in InputSet"

    go (LoadSet which lookup1 lookup2 sym) =
      let val = case (which, lookup1, lookup2) of
            (USE_EAV, VAL_ENTID eid, VAL_ENTID aid) ->
              fullLookup eid aid (eav db)
            (USE_AEV, VAL_ENTID aid, VAL_ENTID eid) ->
              fullLookup aid eid (aev db)
            (USE_AVE, VAL_ENTID aid, val) ->
              HS.mapMonotonic VAL_ENTID $ fullLookup aid val (ave db)
            (USE_VAE, val, VAL_ENTID aid) ->
              HS.mapMonotonic VAL_ENTID $ fullLookup val aid (vae db)
      in RSET {sym, val}

    go (LoadTab which lookupVal from to) =
      let val = case (which, lookupVal) of
            (USE_EAV, VAL_ENTID entid) ->
              HSM.mapKeysMonotonic VAL_ENTID $ partialLookup entid (eav db)
            (USE_AEV, VAL_ENTID entid) ->
              HSM.mapKeysMonotonic VAL_ENTID $ partialLookup entid (aev db)
            (USE_AVE, VAL_ENTID entid) ->
              HSM.mapValsMonotonic VAL_ENTID $ partialLookup entid (ave db)
            (USE_VAE, val)             ->
              HSM.mapKeysMonotonic VAL_ENTID $
              HSM.mapValsMonotonic VAL_ENTID $
              partialLookup val (vae db)
      in --trace ("LoadTab " <> show lookupVal <> " " <> show from <> " " <> show to <> " " <> show val) $
         RTAB {from,to,val}

    go (SetDifference _key pl pr) =
      let (RSET sym lhs) = go pl
          (RSET _ rhs) = go pr
      in RSET sym $ HS.difference lhs rhs

    go (TabScalarLookup _from pscalar _to ptab) =
      let (RSCALAR sym val) = go pscalar
          (RTAB from to tab) = go ptab
      in RSET to $ HSM.lookup val tab

    go (TabSetUnionVals _from pset _to ptab) =
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
      in --trace ("TabSetUnionVals: " <> show asSet) $
         RSET to asSet

    go (TabRestrictKeys _from _to ptab pset) =
      let (RTAB from to tab) = go ptab
          (RSET sym set) = go pset
          restricted = HSM.restrictKeys set tab
      in --trace ("TabRestrictKeys: " <> show tab <> " " <> show set) $
         RTAB from to $ restricted

    go (TabKeySet _from _to ptab) =
      let (RTAB from _ tab) = go ptab
      in RSET from $ HSM.toKeySet tab

    go (FilterPredTabKeys ppreds ptab) =
      let (RTAB from to tab) = go ptab
          preds = map evalBiPred ppreds
      in RTAB from to $ error "TODO: Reenable FilterPredTabKeys" {- case pred of
        B_LT  -> HSM.dropWhileAntitone (val <) tab
        B_LTE -> HSM.dropWhileAntitone (val <=) tab
        B_EQ  -> undefined -- Perform lookup, make singleton
        B_GTE -> HSM.takeWhileAntitone (val >=) tab
        B_GT  -> HSM.takeWhileAntitone (val >) tab -}

    go (FilterPredTabVals ppreds ptab) =
      let (RTAB from to tab) = go ptab
          preds = map evalBiPred ppreds
      in RTAB from to $ HSM.mapMaybeWithKey
           (applyFilterFuncsToSet preds) tab

    --
    go (TabRestrictKeysVals from to ppreds ptab pset) =
      let (RTAB _ _ tab) = go ptab
          (RSET _ set) = go pset
          preds = map evalBiPred ppreds
          outtab = HSM.restrictKeysWithPred
            (applyFilterFuncsToSet preds) set tab
      in RTAB from to outtab

    go (SetJoin _key pa pb) =
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

    go (AddToMultiTab plhs prhs) =
      let elhs = go plhs
          erhs = go prhs
          hml = HSM.toHitchhikerMap elhs.val
          target = HM.intersectionWith pushVector hml erhs.val
      in if elhs.from == erhs.from
         then RMTAB elhs.from ((elhs.to):(erhs.to)) target
         else error "Bad plan: add to multi-tab of two different key symbols"

    go (SetToRows _ pset) =
      let (RSET sym set) = go pset
      in ROWS [sym] [sym] $ map V.singleton $ HS.toList set

    go (MultiTabToRows req pmtab) =
      let (RMTAB key vals mtab) = go pmtab
      in multiTabToRows req key vals mtab

    evalBiPred :: PlanBiPred -> EvalBiPred
    evalBiPred (PBP_LEFT pscalar pred)  =
      let (RSCALAR _ val) = go pscalar
      in EBP_LEFT val pred
    evalBiPred (PBP_RIGHT pred pscalar) =
      let (RSCALAR _ val) = go pscalar
      in EBP_RIGHT pred val

-- -----------------------------------------------------------------------

-- During evaluation, we may have a row, but we will want to

sortRowsBy :: [Variable] -> [Variable] -> [Vector Value] -> [Vector Value]
sortRowsBy allVars reqSortOrder rows
  | any isNothing mybRequestIdx = error "Invalid requested sort order"
  | otherwise = sortedRows
  where
    mybRequestIdx :: [Maybe Int]
    mybRequestIdx = map (flip L.elemIndex allVars) reqSortOrder

    requestIdxes = catMaybes mybRequestIdx

    sortedRows = sortBy (doSort requestIdxes) rows

    doSort :: [Int] -> Vector Value -> Vector Value -> Ordering
    doSort [] a b = EQ
    doSort (x:xs) a b = case compare a b of
      EQ -> doSort xs a b
      x  -> x

applyFilterFuncsToSet :: [EvalBiPred] -> k -> HitchhikerSet Value
                      -> Maybe (HitchhikerSet Value)
applyFilterFuncsToSet [] _ vset = if HS.null vset
                                  then Nothing
                                  else Just vset
applyFilterFuncsToSet ((EBP_LEFT s pred):ps) _key vset =
    applyFilterFuncsToSet ps _key $ splitSetLeft s pred vset
applyFilterFuncsToSet ((EBP_RIGHT pred s):ps) k vset =
    applyFilterFuncsToSet ps k $ splitSetRight vset pred s

-- Filters a set so all elements are (element `pred` k).
--
-- We can implement any check where the set is not the full or empty with one
-- of the antitone functions, but must handle
splitSetLeft :: (Show k, Ord k)
             => k -> BuiltinPred -> HitchhikerSet k -> HitchhikerSet k
splitSetLeft _ _ (HITCHHIKERSET config Nothing) = HITCHHIKERSET config Nothing
splitSetLeft k pred full@(HITCHHIKERSET config (Just top)) = case pred of
  B_LT
    -- 0 < [1, 2, 3]: all elements match
    | k < min -> full
    -- 2 < [1, 2, 3]: some elements match
    | k < max -> HS.takeWhileAntitone (k <) full
    -- 3 < [1, 2, 3]: no elements match
    | otherwise -> emptySet
  B_LTE
    -- 0 <= [1, 2, 3]: all elements match
    | k <= min -> full
    -- 3 <= [1, 2, 3]: some elements match
    | k <= max -> HS.takeWhileAntitone (k <=) full
    -- 4 <= [1, 2, 3]: no elements match
    | otherwise -> emptySet
  B_EQ ->
    -- Only return the search item in a set, if it exists.
    if HS.member k full
    then HS.singleton config k
    else emptySet
  B_GTE
    -- 4 >= [1, 2, 3]: all elements match
    | k >= max -> full
    -- 2 >= [1, 2, 3]: some elements match
    | k >= min -> HS.dropWhileAntitone (k >=) full
    -- 0 >= [1, 2, 3]: no elements match
    | otherwise -> emptySet
  B_GT
    -- 4 > [1, 2, 3]: all elements match
    | k > max -> full
    -- 2 > [1, 2, 3]: some elements match
    | k > min -> HS.dropWhileAntitone (k >) full
    -- 0 > [1, 2, 3]: no elements match
    | otherwise -> emptySet
  where
    min = HS.findMin full
    max = HS.findMax full
    emptySet = HS.empty config

splitSetRight :: (Show k, Ord k)
              => HitchhikerSet k -> BuiltinPred -> k -> HitchhikerSet k
splitSetRight (HITCHHIKERSET config Nothing) _ _ = HITCHHIKERSET config Nothing
splitSetRight full@(HITCHHIKERSET config (Just top)) pred k = case pred of
  B_LT
    -- [1, 2, 3] < 0: no elements match
    | not (min < k) -> emptySet
    -- [1, 2, 3] < 3: some elements match
    | not (max < k) -> HS.takeWhileAntitone (< k) full
    -- [1, 2, 3] < 4: all elements match
    | otherwise -> full
  B_LTE
    -- [1, 2, 3] <= 0: no elements match
    | not (max <= k) -> emptySet
    -- [1, 2, 3] <= 2: some elements match
    | not (min <= k) -> HS.takeWhileAntitone (<= k) full
    -- [1, 2, 3] <= 3: all elements match
    | otherwise -> full
  B_EQ ->
    -- Only return the search item in a set, if it exists.
    if HS.member k full
    then HS.singleton config k
    else emptySet
  B_GTE
    -- [1, 2, 3] >= 4: no elements match
    | not (max >= k) -> emptySet
    -- [1, 2, 3] >= 2: some elements match
    | not (min >= k) -> HS.dropWhileAntitone (>= k) full
    -- [1, 2, 3] >= 0: all elements match
    | otherwise -> full
  B_GT
    -- [1, 2, 3] > 4: no elements match
    | not (max > k) -> emptySet
    -- [1, 2, 3] > 2: some elements match
    | not (min > k) -> HS.dropWhileAntitone (> k) full
    -- [1, 2, 3] > 0: all elements match
    | otherwise -> full
  where
    min = HS.findMin full
    max = HS.findMax full
    emptySet = HS.empty config



-- multiTabToRows :: Variable
--                -> [Variable]
--                -> HitchhikerMap Value (Vector (HitchhikerSet Value))
--                -> Rows
-- multiTabToRows key vals hhmap = ROWS vars vars rowData
--   where
--     vars = key:vals
--     rowData = concat $ map step $ HM.toList hhmap

--     step :: (Value, Vector (HitchhikerSet Value)) -> [Vector Value]
--     step (k, tops) = V.toList $ V.sequence $
--       V.cons (V.singleton k) (map (V.fromList . HS.toList) tops)

