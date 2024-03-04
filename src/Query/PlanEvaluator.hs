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

printableLookupToFunc :: RowLookup
                      -> (Database -> EAVRows Value Value Value Int)
printableLookupToFunc USE_EAV = eav
printableLookupToFunc USE_AEV = aev
printableLookupToFunc USE_AVE = ave
printableLookupToFunc USE_VAE = vae

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

    go (LoadTab which lookupVal from to) =
      let val = partialLookup lookupVal (printableLookupToFunc which $ db)
      in RTAB {from,to,val}

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
      in RSET to asSet

    go (TabRestrictKeys _from _to ptab pset) =
      let (RTAB from to tab) = go ptab
          (RSET sym set) = go pset
      in RTAB from to $ HSM.restrictKeys set tab

    go (TabKeySet _from _to ptab) =
      let (RTAB from _ tab) = go ptab
      in RSET from $ HSM.toKeySet tab

    go (FilterPredTabKeysL pconst pred ptab) =
      let (RSCALAR _ val) = go pconst
          (RTAB from to tab) = go ptab
      in RTAB from to $ case pred of
        B_LT  -> HSM.dropWhileAntitone (val <) tab
        B_LTE -> HSM.dropWhileAntitone (val <=) tab
        B_EQ  -> undefined -- Perform lookup, make singleton
        B_GTE -> HSM.takeWhileAntitone (val >=) tab
        B_GT  -> HSM.takeWhileAntitone (val >) tab

    -- TODO: FilterPredTabKeysR, with the above flipped in direction.

    --
    go (TabRestrictKeysVals from to ppreds ptab pset) =
      let (RTAB _ _ tab) = go ptab
          (RSET _ set) = go pset
          preds = map evalBiPred ppreds

          vsetAsMaybe vset = if HS.null vset
                             then Nothing
                             else Just vset
          filterFunc [] _ vset                     = vsetAsMaybe vset
          filterFunc ((EBP_LEFT s pred):ps) k vset =
            filterFunc ps k $ case pred of
              B_LT  -> HS.takeWhileAntitone (s <) vset
              B_LTE -> HS.takeWhileAntitone (s <=) vset
              B_EQ  -> undefined
              B_GTE -> HS.dropWhileAntitone (s >=) vset
              B_GT  -> HS.dropWhileAntitone (s >) vset
          filterFunc ((EBP_RIGHT pred s):ps) k vset =
            filterFunc ps k $ case pred of
              B_LT  -> HS.dropWhileAntitone (< s) vset
              B_LTE -> HS.dropWhileAntitone (<= s) vset
              B_EQ  -> undefined
              B_GTE -> HS.takeWhileAntitone (>= s) vset
              B_GT  -> HS.takeWhileAntitone (> s) vset

          outtab = HSM.restrictKeysWithPred (filterFunc preds) set tab
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

