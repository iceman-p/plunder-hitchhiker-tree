{-# OPTIONS_GHC -Wno-partial-fields   #-}
{-# OPTIONS_GHC -Wincomplete-patterns   #-}
{-# LANGUAGE GADTs #-}
module HitchhikerDatomStore where

import           ClassyPrelude    hiding (lookup)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import           Data.Sorted

import           Query.Types

import qualified HitchhikerMap    as HM
import qualified HitchhikerSet    as HS
import qualified HitchhikerSetMap as HSM

import qualified Data.List        as L
import qualified Data.Map         as M
import qualified Data.Set         as S
import qualified Data.Vector      as V

-- What are we building here? What's the purpose? We're making a hitchhiker
-- tree variant where for any six tuple, such as [e a v tx o], we have an
-- efficient way to minimize churn in the upper levels of the tree.
--
-- You need to think of this as a way to map [e a] -> [%{v}  [tx o]], with
-- hitchhiker entries [d e a v tx o] pushed down (or in some situations used
-- for quick short circuits on cardinality once).

-- As a convention in this file, all types are of the form [e a v tx o] even
-- though the code is written so it can be reused for the ave or vea indexes,
-- too.

data EDatomRow e a v tx
  = ERowIndex (TreeIndex e (EDatomRow e a v tx))
              (Map e (ArraySet (a, v, tx, Bool)))
  | ELeaf (Map e (ADatomRow a v tx))
  deriving (Show, Generic, NFData)

-- TODO: Think about data locality. A lot. Right now VStorage's indirection
-- means that vs aren't stored next to each other when doing a simple a scan.
data ADatomRow a v tx
  = ARowIndex (TreeIndex a (ADatomRow a v tx))
              (Map a (ArraySet (v, tx, Bool)))
  | ALeaf (Map a (VStorage v tx))
  deriving (Show, Generic, NFData)

-- At the end is VStorage: a set copy of the current existing values, and a
-- separate log of transactions.
data VStorage v tx
  -- Many values are going to be a single value that doesn't change; don't
  -- allocate two hitchhiker trees to deal with them, just inline.
  = VSimple v tx
  -- We have multiple
  | VStorage (Maybe (HitchhikerSetNode v)) (HitchhikerSetMapNode tx (v, Bool))
  deriving (Show, Generic, NFData)

data EAVRows e a v tx = EAVROWS {
  config :: TreeConfig,
  root   :: Maybe (EDatomRow e a v tx)
  }
  deriving (Show)

emptyRows :: TreeConfig -> EAVRows e a v tx
emptyRows config = EAVROWS config Nothing

--
addDatom :: forall e a v tx
          . (Show e, Show a, Show v, Show tx,
             Ord e, Ord a, Ord v, Ord tx)
         => (e, a, v, tx, Bool)
         -> EAVRows e a v tx
         -> EAVRows e a v tx
addDatom (!e, !a, !v, !tx, !o) (EAVROWS config Nothing) =
  -- Initialize everything to a single
  EAVROWS config $ Just $
  ELeaf $ M.singleton e $
  ALeaf $ M.singleton a $
  vstorageSingleton (v, tx, o)

addDatom (!e, !a, !v, !tx, !o) (EAVROWS config (Just root)) =
  EAVROWS config $ Just $
  fixUp config (hhEDatomRowTF config) $
  insertRec config
            (hhEDatomRowTF config)
            (M.singleton e (ssetSingleton (a, v, tx, o)))
            root

hhEDatomRowTF :: (Show e, Show a, Show v, Show tx,
                  Ord e, Ord a, Ord v, Ord tx)
              => TreeConfig
              -> TreeFun e
                         (a, v, tx, Bool)
                         (EDatomRow e a v tx)
                         (Map e (ArraySet (a, v, tx, Bool)))
                         (Map e (ADatomRow a v tx))
hhEDatomRowTF config = TreeFun {
  mkNode = ERowIndex,
  mkLeaf = ELeaf,
  caseNode = \case
      ERowIndex a b -> Left (a, b)
      ELeaf l       -> Right l,

  -- leafMap -> hhMap -> leafMap
  leafInsert = eLeafInsert config,
  leafMerge = error "eLeafInsert only required for deletion",
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = error "Pure deletion has to be handled otherwise",

  --
  hhMerge = M.unionWith ssetUnion,
  hhLength = sum . map length . M.elems,
  hhSplit = \k m -> M.spanAntitone (< k) m,
  hhEmpty = M.empty,
  hhDelete = error "Pure deletion has to be handled otherwise"
  }

-- It's fine to leave the map larger than the ELeaf configuration because
-- `splitLeafMany` is called immediately after every `leafInsert`.
--
eLeafInsert :: forall e a v tx
             . (Show a, Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
            => TreeConfig
            -> Map e (ADatomRow a v tx)
            -> Map e (ArraySet (a, v, tx, Bool))
            -> Map e (ADatomRow a v tx)
eLeafInsert config = M.foldlWithKey insertRows
  where
    insertRows :: Map e (ADatomRow a v tx)
               -> e
               -> ArraySet (a, v, tx, Bool)
               -> Map e (ADatomRow a v tx)
    insertRows leaf key values = case M.lookup key leaf of
      Nothing -> M.insert key (buildArowLeafs config values) leaf
      Just x  -> M.adjust (arowInsertMany config $ atupleToMap values) key leaf

-- -----------------------------------------------------------------------

-- Builds a new
--
buildArowLeafs :: forall a v tx
                . (Ord a, Ord v, Ord tx, Show v, Show tx)
               => TreeConfig
               -> ArraySet (a, v, tx, Bool)
               -> ADatomRow a v tx
buildArowLeafs config = ALeaf . foldl' merge mempty
  where
    merge :: Map a (VStorage v tx)
          -> (a, v, tx, Bool)
          -> Map a (VStorage v tx)
    merge m (a, v, tx, op) = case M.lookup a m of
      Nothing -> M.insert a (vstorageSingleton (v, tx, op)) m
      Just vs -> M.insert a (vstorageInsert config vs (v, tx, op)) m

atupleToMap :: forall a v tx
             . (Ord a, Ord v, Ord tx)
            => ArraySet (a, v, tx, Bool) -> Map a (ArraySet (v, tx, Bool))
atupleToMap = foldl' add mempty
  where
    add :: Map a (ArraySet (v, tx, Bool))
        -> (a, v, tx, Bool)
        -> Map a (ArraySet (v, tx, Bool))
    add m (a, v, tx, op) = case M.lookup a m of
      Nothing -> M.insert a (ssetSingleton (v, tx, op)) m
      Just ss -> M.adjust (ssetInsert (v, tx, op)) a m


arowInsertMany :: (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
               => TreeConfig
               -> Map a (ArraySet (v, tx, Bool))
               -> ADatomRow a v tx
               -> ADatomRow a v tx
arowInsertMany config !items top =
  fixUp config (hhADatomRowTF config) $
  insertRec config (hhADatomRowTF config) items top

hhADatomRowTF :: (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
              => TreeConfig
              -> TreeFun a
                         (v, tx, Bool)
                         (ADatomRow a v tx)
                         (Map a (ArraySet (v, tx, Bool)))
                         (Map a (VStorage v tx))
hhADatomRowTF config = TreeFun {
  mkNode = ARowIndex,
  mkLeaf = ALeaf,
  caseNode = \case
      ARowIndex a b -> Left (a, b)
      ALeaf l       -> Right l,

  -- -- leafMap -> hhMap -> leafMap
  leafInsert = aLeafInsert config,
  leafMerge = error "eLeafInsert only required for deletion",
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = error "Pure deletion has to be handled otherwise",

  --
  hhMerge = M.unionWith ssetUnion,
  hhLength = sum . map length . M.elems,
  hhSplit = \k m -> M.spanAntitone (< k) m,
  hhEmpty = M.empty,
  hhDelete = error "Pure deletion has to be handled otherwise"
  }

aLeafInsert :: forall a v tx
             . (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
            => TreeConfig
            -> Map a (VStorage v tx)
            -> Map a (ArraySet (v, tx, Bool))
            -> Map a (VStorage v tx)
aLeafInsert config = M.foldlWithKey insertRows
  where
    insertRows :: Map a (VStorage v tx)
               -> a
               -> ArraySet (v, tx, Bool)
               -> Map a (VStorage v tx)
    insertRows leaf key values = case M.lookup key leaf of
      Nothing -> M.insert key (vstorageFromArray config values) leaf
      Just storage ->
          M.insert key (vstorageInsertMany config storage values) leaf


-- -----------------------------------------------------------------------

mkSingletonTxMap (v, tx, op) =
  HitchhikerSetMapNodeLeaf $
  M.singleton tx $
  NAKEDSET $ Just $ HitchhikerSetNodeLeaf $ ssetSingleton (v, op)

vstorageSingleton :: (v, tx, Bool) -> VStorage v tx
vstorageSingleton (v, tx, True)   = VSimple v tx

-- Dumb, but the model allows it.
vstorageSingleton d@(_, _, False) = VStorage Nothing $ mkSingletonTxMap d

vstorageFromArray :: (Show v, Show tx, Ord v, Ord tx)
                  => TreeConfig
                  -> ArraySet (v, tx, Bool)
                  -> VStorage v tx
vstorageFromArray config as
  | null as = error "Can't make vstorage from empty arrayset"
  | otherwise =
        let (hs, rest) = ssetSplitAt 1 as
        in foldl' (vstorageInsert config)
                  (vstorageSingleton $ ssetFindMin hs)
                  rest

vstorageInsert :: (Show v, Show tx, Ord v, Ord tx)
               => TreeConfig
               -> VStorage v tx
               -> (v, tx, Bool)
               -> VStorage v tx
vstorageInsert config (VSimple pv ptx) newFact =
  vstorageInsert config
                 (VStorage (Just $ HitchhikerSetNodeLeaf $ ssetSingleton pv)
                           (mkSingletonTxMap (pv, ptx, True)))
                 newFact

vstorageInsert config (VStorage curSet txMap) (v, tx, op) =
  VStorage valSet newTxMap
  where
    valSet = case (curSet, op) of
      (Nothing, True)   -> Just $ HitchhikerSetNodeLeaf $ ssetSingleton v
      (Nothing, False)  -> Nothing
      (Just set, True)  -> Just $ HS.insertRaw config v set
      (Just set, False) -> HS.deleteRaw config v set

    newTxMap = HSM.insertRaw config tx (v, op) txMap

vstorageInsertMany :: (Show v, Show tx, Ord v, Ord tx)
                   => TreeConfig
                   -> VStorage v tx
                   -> ArraySet (v, tx, Bool)
                   -> VStorage v tx
vstorageInsertMany config storage as =
  foldl' (vstorageInsert config) storage as

-- -----------------------------------------------------------------------

lookup :: forall e a v tx
        . (Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
       => e -> a -> EAVRows e a v tx -> HitchhikerSet v
lookup _ _ (EAVROWS config Nothing)    = HS.empty config

-- Lookup is more complicated in that we have to push matching transactions
-- down through multiple levels, to
lookup e a (EAVROWS config (Just top)) = lookInENode mempty top
  where
    lookInENode :: ArraySet (v, tx, Bool) -> EDatomRow e a v tx
                -> HitchhikerSet v
    lookInENode txs = \case
      ERowIndex index hitchhikers ->
        let newTxs = appendMatchingEHH txs hitchhikers
        in lookInENode newTxs $ findSubnodeByKey e index
      ELeaf items -> case M.lookup e items of
        Nothing     -> HS.fromArraySet config $ applyLog mempty txs
        Just anodes -> lookInANode txs anodes

    -- When recursing downwards through ERowIndex nodes, we must accumulate
    -- matching
    appendMatchingEHH :: ArraySet (v, tx, Bool)
                      -> Map e (ArraySet (a, v, tx, Bool))
                      -> ArraySet (v, tx, Bool)
    appendMatchingEHH vtx hh = case M.lookup e hh of
      Nothing   -> vtx
      Just avtx -> ssetUnion vtx $ matchAV avtx
      where
        matchAV =
          ssetMap removeA .
          ssetTakeWhileAntitone takeFun .
          ssetDropWhileAntitone dropFun
          where
            removeA (a, v, tx, o) = (v, tx, o)
            dropFun (x, _, _, _) = (x < a)
            takeFun (x, _, _, _) = (x == a)

    lookInANode :: ArraySet (v, tx, Bool) -> ADatomRow a v tx -> HitchhikerSet v
    lookInANode txs = \case
      ARowIndex index hitchhikers ->
        let newTxs = appendMatchingAHH txs hitchhikers
        in lookInANode newTxs $ findSubnodeByKey a index
      ALeaf items -> case M.lookup a items of
        Nothing       -> HS.fromArraySet config $ applyLog mempty txs
        Just vstorage -> applyToVStorage txs vstorage

    appendMatchingAHH :: ArraySet (v, tx, Bool)
                      -> Map a (ArraySet (v, tx, Bool))
                      -> ArraySet (v, tx, Bool)
    appendMatchingAHH vtx hh = case M.lookup a hh of
      Nothing    -> vtx
      Just hhVtx -> vtx <> hhVtx

    applyLog :: ArraySet v -> ArraySet (v, tx, Bool) -> ArraySet v
    applyLog = foldl' apply
      where
        apply set (v, _, True)  = ssetInsert v set
        apply set (v, _, False) = ssetDelete v set

    applyToVStorage :: ArraySet (v, tx, Bool)
                    -> VStorage v tx
                    -> HitchhikerSet v
    applyToVStorage as vs =
      case vstorageInsertMany config vs as of
        VSimple v _    -> HS.singleton config v
        VStorage top _ -> HITCHHIKERSET config top

-- -----------------------------------------------------------------------

partialLookup :: forall e a v tx
               . (Show a, Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
              => e -> EAVRows e a v tx -> HitchhikerSetMap a v
partialLookup _ (EAVROWS config Nothing)    = HSM.empty config

partialLookup e (EAVROWS config (Just top)) = lookInENode mempty top
  where
    lookInENode :: ArraySet (a, v, tx, Bool) -> EDatomRow e a v tx
                -> HitchhikerSetMap a v
    lookInENode txs = \case
      ERowIndex index hitchhikers ->
        let newTxs = appendMatchingEHH txs hitchhikers
        in lookInENode newTxs $ findSubnodeByKey e index
      ELeaf items -> case M.lookup e items of
        Nothing     -> error "TODO: shut up ghcid, come back later"
          -- undefined --HS.fromArraySet config $ applyLog mempty txs
        Just anodes -> aNodeTo (atupleToMap txs) anodes

    -- When recursing downwards through ERowIndex nodes, we must accumulate
    -- matching
    appendMatchingEHH :: ArraySet (a, v, tx, Bool)
                      -> Map e (ArraySet (a, v, tx, Bool))
                      -> ArraySet (a, v, tx, Bool)
    appendMatchingEHH vtx hh = case M.lookup e hh of
      Nothing   -> vtx
      Just avtx -> ssetUnion vtx avtx

    -- What are we doing here?
    aNodeTo :: Map a (ArraySet (v, tx, Bool))
            -> ADatomRow a v tx
            -> HitchhikerSetMap a v
    aNodeTo txs anodes =
      let arow = flushDownwards (hhADatomRowTF config)
               $ arowInsertMany config txs anodes

          translate = \case
            ARowIndex tree hh ->
              HitchhikerSetMapNodeIndex (mapIndex translate tree) mempty
            ALeaf leafMap ->
              HitchhikerSetMapNodeLeaf $ M.mapMaybe translateLeaf leafMap

          translateLeaf = \case
            VSimple x _ ->
              Just $ NAKEDSET (Just (HitchhikerSetNodeLeaf $ ssetSingleton x))
            VStorage Nothing _ -> Nothing
            VStorage x _ -> Just $ NAKEDSET x

      in HITCHHIKERSETMAP config $ Just $ translate arow

-- -----------------------------------------------------------------------

type EAVStore = EAVRows Int Int Value Int

data Database = DATABASE {
  -- TODO: More restricted types.
  eav :: EAVRows Value Value Value Int,
  aev :: EAVRows Value Value Value Int,
  ave :: EAVRows Value Value Value Int,
  vae :: EAVRows Value Value Value Int

  -- eav :: EAVRows Int Attr Value Int,
  -- aev :: EAVRows Attr Int Value Int,
  -- ave :: EAVRows Attr Value Int Int,
  -- vae :: EAVRows Value Attr Int Int
  }
  deriving (Show)

emptyDB :: Database
emptyDB = DATABASE {
  eav = emptyRows twoThreeConfig,
  aev = emptyRows twoThreeConfig,
  ave = emptyRows twoThreeConfig,
  vae = emptyRows twoThreeConfig
  }

learn :: (Value, Value, Value, Int, Bool)
      -> Database
      -> Database
learn (e, a, v, tx, op) DATABASE{..} = DATABASE {
  eav = addDatom (e, a, v, tx, op) eav,
  aev = addDatom (a, e, v, tx, op) aev,
  ave = addDatom (a, v, e, tx, op) ave,
  vae = addDatom (v, a, e, tx, op) vae
  }

-- -----------------------------------------------------------------------

-- If you just start over, what does a top down unification engine look like
-- here?
--
--    (sut/q '[:find ?nation
--             :in $ ?alias
--             :where
--             [?e :aka ?alias]
--             [?e :nation ?nation]]
--           (sut/db conn1)
--           "fred")))
--
-- What's going on here?
--
--   R_ONECONST "?alias" "fred"
--   LOOKUP_VEA

-- For every clause below that's a mapping, we have to provide both forwards
-- and backwards search sets. The "backwards" searches are costlier full scans
-- of a partial index.

-- [e a ?z]  -> EAV(e, a)                 -> VSET "?z" (HHSet v)
-- [e ?y v]  -> EAV(e, <iterate>, v)      -> VSET "?y" (HHSet a)
-- [?x a v]  -> (AVE(a, v) or VAE(v, a))  -> VSET "?x" (HHSet e)
-- [e ?y ?z] -> EAV(e), EAV(e, <backwards scan>)
--              -> VMAP "?y" "?z" (HHSetMap a v) (HHSetMap v a)
-- [?x a ?z] -> AEV(a), (AVE(a) if it exists, VAE(v, <iterate>) if ref,
--              -> VMAP "?z" "?x" (HHSetMap v e) (HHSetMap e v)
-- [?x ?y v] -> VAE(v) if exists,


{-


-- -----------------------------------------------------------------------

-- What's the relational algebra thing we're going to implement here?
--
-- A relationship table

data Relation
  -- Initial relation value, always returns other value on join.
  = RNONE
  -- No relations because disjoint.
  | RDISJOINT
  -- Simple one set relation.
  | RSET Variable (HitchhikerSet Value)
  -- Bidirectional relation straight from the database. Created in the first
  -- clause when we don't know what direction we'll be searching. Always build
  -- it so the first symbol to the second symbol is the preferred search
  -- direction.
  | RBIDIR Variable
           Variable
           (HitchhikerSetMap Value Value)
           (HitchhikerSetMap Value Value)

  -- A list of rows. Joining with this any other type produces a row. The first
  -- list of symbols is the order in the order of each row.
  | RROW [Variable] [Variable] (Vector (Vector Value))

  -- A table from one symbol to possibly multiple symbols. ?a -> [?b ?c]
  | RTAB Variable [Variable] (HitchhikerMap Value (Vector (HitchhikerSet Value)))

  deriving (Show)

-- -----------------------------------------------------------------------

-- We've hit a case where we can't join between two representations without
-- iterating anyway, so build a full RROW table.
tableToRows :: Variable
            -> [Variable]
            -> HitchhikerMap Value (Vector (HitchhikerSet Value))
            -> Relation
tableToRows key vals hhmap = RROW symbols sortOrder rowData
  where
    symbols = key:vals
    sortOrder = symbols
    rowData = V.concat $ map step $ HM.toList hhmap

    step :: (Value, Vector (HitchhikerSet Value)) -> Vector (Vector Value)
    step (k, tops) = V.sequence $
      V.cons (V.singleton k) (map (V.fromList . HS.toList) tops)

ensureSortedBy :: [Variable] -> [Variable] -> [Variable] -> Vector (Vector Value)
               -> ([Variable], Vector (Vector Value))
ensureSortedBy request allSymb curSortedBy rows
  | request `isPrefixOf` curSortedBy = (curSortedBy, rows)
  | any isNothing mybRequestIdx = error "Invalid request"
  | otherwise = (request, sortedRows)
  where
    mybRequestIdx :: [Maybe Int]
    mybRequestIdx = map (flip L.elemIndex allSymb) request

    requestIdxes = catMaybes mybRequestIdx

    sortedRows = sortBy (doSort requestIdxes) rows

    doSort :: [Int] -> Vector Value -> Vector Value -> Ordering
    doSort [] a b = EQ
    doSort (x:xs) a b = case compare a b of
      EQ -> doSort xs a b
      x  -> x


-- Map Int [Vector [Int]]
--
-- 'e' => {'a' 'b' 'c'} {1 2 3}
-- 'f' => {'b' 'c' 'd'} {4 5 6}

-- 'e' 'a' 1
-- 'e' 'a' 2
-- 'e' 'a' 3

-- -- OK, this here is a good first draft of the Cartesian product, but the types
-- -- aren't right, we aren't

-- d :: Map Int (Vector (Vector Int)) -> [(Int, Vector (Vector Int))]
-- d = M.toList

-- doprod :: (Int, Vector (Vector Int)) -> Vector (Vector Int)
-- doprod (key, vals) = V.sequence (V.cons (V.singleton key) vals)

-- dojoin :: Map Int (Vector (Vector Int)) -> Vector (Vector Int)
-- dojoin m = concat $ map doprod $ M.toList m

-- dd = dojoin $ M.fromList [
--   (1, V.fromList [
--         V.fromList [2, 3, 4],
--         V.fromList [5, 6, 7] ]),
--   (8, V.fromList [
--         V.fromList [9, 10, 11],
--         V.fromList [12, 13]])]

-- -- TODO: When I come back, my next task is to implement this, and then use it
-- -- to implement rowData in tableToRows above. That should give me a table which
-- -- means we can then write tab/bidi join below.
-- --
-- dhm :: HitchhikerMap Value (Vector (HitchhikerSet Value))
--     -> Vector (Vector Value)
-- dhm = V.concat . map step . HM.toList
--   where
--     step :: (Value, Vector (HitchhikerSet Value)) -> Vector (Vector Value)
--     step (k, tops) = V.sequence (V.cons (V.singleton k) (map (V.fromList . HS.toList) tops))

-- ddhm = dhm $ HM.insertMany x $ HM.empty twoThreeConfig
--   where
--     x = M.fromList [
--       (VAL_INT 1, V.fromList [
--           HS.fromSet twoThreeConfig $ S.fromList $ map VAL_INT [2, 3, 4],
--           HS.fromSet twoThreeConfig $ S.fromList $ map VAL_INT[5, 6, 7] ]),
--       (VAL_INT 8, V.fromList [
--           HS.fromSet twoThreeConfig $ S.fromList $ map VAL_INT [9, 10, 11],
--           HS.fromSet twoThreeConfig $ S.fromList $ map VAL_INT [12, 13] ] ) ]

-- -----------------------------------------------------------------------

rjoin :: Relation -> Relation -> Relation
rjoin RNONE r = r
rjoin l RNONE = l

rjoin RDISJOINT RDISJOINT = RDISJOINT
rjoin RDISJOINT _      = RDISJOINT
rjoin _ RDISJOINT      = RDISJOINT

rjoin (RSET lhs lhv) (RSET rhs rhv)
  | lhs /= rhs = RDISJOINT
  | otherwise = let intersect = HS.intersection lhv rhv
                in if HS.null intersect
                   then RDISJOINT
                   else RSET lhs intersect

rjoin l@(RBIDIR _ _ _ _) r@(RSET _ _) = rjoin r l
rjoin (RSET lhs lhv) (RBIDIR x y xtoy ytox)
  | lhs == x = RTAB x [y] $
               HM.mapMaybe (Just . V.singleton) $
               HSM.toHitchhikerMap $
               HSM.restrictKeys lhv xtoy
  | lhs == y = RTAB y [x] $
               HM.mapMaybe (Just . V.singleton) $
               HSM.toHitchhikerMap $
               HSM.restrictKeys lhv ytox
  | otherwise = RDISJOINT

rjoin l@(RTAB _ _ _) r@(RSET _ _) = rjoin r l
rjoin (RSET lhs lhv) (RTAB keyS valS hhm)
  | lhs == keyS = RTAB keyS valS $ HM.restrictKeys lhv hhm
  | elem lhs valS = error "Must materialize RTAB to RROW"
  | otherwise = RDISJOINT

rjoin l@(RTAB _ _ _) r@(RBIDIR _ _ _ _) = rjoin r l
rjoin bidir@(RBIDIR x y xtoy ytox) (RTAB keyS valS hhm)
  | keyS == x = RTAB x (y:valS)
              $ HM.intersectionWith V.cons (HSM.toHitchhikerMap xtoy) hhm
  | keyS == y = RTAB y (x:valS)
              $ HM.intersectionWith V.cons (HSM.toHitchhikerMap ytox) hhm
  | (elem x valS) || (elem y valS) = rjoin bidir $ tableToRows keyS valS hhm
  | otherwise = error $ "Implement other RBIDIR RTAB cases"

rjoin bidir@(RBIDIR x y xtoy ytox) inrow@(RROW all inSortedBy inRows)
  | (elem x all) && (elem y all) = error "full join case"
  | (elem x all) =
    addYToAllRows x y (HSM.toHitchhikerMap xtoy) (all, inSortedBy, inRows)
  | (elem y all) =
    addYToAllRows y x (HSM.toHitchhikerMap ytox) (all, inSortedBy, inRows)
  | otherwise = RDISJOINT

rjoin a b = error $ "Unimplemented join between " <> show a <> " and " <> show b

data NextMatchType
  -- Iterate between the index into the rows and the mapping to find where we
  -- have to do a merge.
  = SearchVal Int [(Value, [Value])]
  -- The row in the
  | MatchedPrefix Int [Value] [(Value, [Value])]

-- Given a map of x to y and a bunch of rows, where x is an element of the rows
-- but y is not, join all values of y to all rows that
addYToAllRows :: Variable
              -> Variable
              -> HitchhikerMap Value (HitchhikerSet Value)
              -> ([Variable], [Variable], Vector (Vector Value))
              -> Relation
addYToAllRows x y maps (all, inSortedBy, inRows) = RROW newAll newSorted newRows
  where
    newAll = all ++ [y]
    (newSorted, sortedRows) = ensureSortedBy [x] all inSortedBy inRows
    newRows = case toMaplist maps of
      [] -> V.empty
      xs -> V.unfoldr nextMatch $ SearchVal 0 xs

    -- Doing this the super naive way under the assumption from the
    -- intersection experiments that showed that doing any fancy allocation
    -- kills performance.
    toMaplist :: HitchhikerMap Value (HitchhikerSet Value)
              -> [(Value, [Value])]
    toMaplist (HITCHHIKERMAP _ Nothing) = []
    toMaplist (HITCHHIKERMAP _ (Just a))
      = map (mapSnd HS.toList) $
        join $
        map M.toList $
        getLeafList HM.hhMapTF a

    --
    vecLen = V.length sortedRows
    -- In this dummy example, we don't worry about the position that x from the
    -- lhs is in the target vector.
    xPos = case L.elemIndex x all of
      Nothing -> error "Invalid symbol name in addYToAllRows"
      Just i  -> i

    nextMatch :: NextMatchType -> Maybe (Vector Value, NextMatchType)
    nextMatch = \case
      SearchVal _ [] -> Nothing
      SearchVal idx orig@((key, vals):xs)
        | idx == vecLen -> Nothing
        | otherwise ->
            case compare ((sortedRows V.! idx) V.! xPos) key of
              LT -> nextMatch $ SearchVal (idx + 1) orig
              EQ -> nextMatch $ MatchedPrefix idx vals orig
              GT -> nextMatch $ SearchVal idx xs

      MatchedPrefix idx [] mapping -> nextMatch $ SearchVal (idx + 1) mapping
      MatchedPrefix idx (x:xs) mapping -> Just (V.snoc (sortedRows V.! idx) x,
                                                MatchedPrefix idx xs mapping)

mapSnd :: (b -> c) -> (a, b) -> (a, c)
mapSnd fun (a, b) = (a, fun b)

loadClause :: Clause -> Database -> Relation

loadClause (C_XAV eSymb attr val) DATABASE{..} =
  RSET eSymb $ lookup val (VAL_ATTR attr) vae

-- C_EYV

loadClause (C_EAZ entity attr vSymb) DATABASE{..} =
  RSET vSymb $ lookup (VAL_ENTID entity) (VAL_ATTR attr) eav

loadClause (C_EYZ entityId aSymb vSymb) DATABASE{..} =
  RBIDIR aSymb vSymb av va
  where
    av = partialLookup (VAL_ENTID entityId) eav
    va = error "Implement backwards entity value->attribute lookup"

loadClause (C_XAZ eSymb attr vSymb) db =
  RBIDIR eSymb vSymb ev ve
  where
    ev = partialLookup (VAL_ATTR attr) (aev db)
    ve = partialLookup (VAL_ATTR attr) (ave db)

-- loadClause (C_XAZ eSymb attr vSymb) DATABASE{..} rset@(RSET rSymb rSet)
--   | rSymb == eSymb =
--       rjoin rset $ RTAB [eSymb, vSymb] $ partialLookup (VAL_ATTR attr) aev
--   | rSymb == vSymb =
--       rjoin rset $ RTAB [vSymb, eSymb] $ partialLookup (VAL_ATTR attr) ave
--   | otherwise =
--       RDIS

loadClause _ _ = error "shut up ghcid, come back here later"

-- [{:name "Frege", :db/id -1, :nation "France",
--   :aka  ["foo" "fred"]}
--  {:name "Peirce", :db/id -2, :nation "france"}
--  {:name "De Morgan", :db/id -3, :nation "English"}])]
--
--    (sut/q '[:find ?nation
--             :in $ ?alias
--             :where
--             [?e :aka ?alias]
--             [?e :nation ?nation]]
--           (sut/db conn1)
--           "fred")))

db :: Database
db = foldl' add emptyDB datoms
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

out :: Relation
out =
  (rjoin
    (loadClause (C_XAZ (SYM "?e") (ATTR ":nation") (SYM "?nation")) db)
    (rjoin
      (loadClause (C_XAZ (SYM "?e") (ATTR ":aka") (SYM "?alias")) db)
      (RSET (SYM "?alias") (HS.singleton twoThreeConfig (VAL_STR "fred")))))

--    (sut/q '[:find ?nation
--             :in $ ?alias
--             :where
--             [?e :aka ?alias]
--             [?e :nation ?nation]]
--           (sut/db conn1)
--           "fred")))

{-

(db/q '[:find ?dbid ?thumburl
        :in $ [?tag ...] ?amount
        :where
        [?e :derp/tag ?tag]
        [?e :derp/upvotes ?upvotes]
        [(> ?upvotes amount)]
        [?e :derp/id ?derpid]
        [?e :derp/thumurl ?thumburl]]
       db
       ["twilight sparkle" "cute"] 100)

What does a query planner do with the above? There's three sections to the
above:

- Union all the ?tag to form an ?e set. Grab AVE, lookup each ?tag for th


-}


-- -----------------------------------------------------------------------


-- A set of
data Binding
  = B_SCALAR Variable
  -- | B_TUPLE [Variable]
  | B_COLLECTION Variable
  -- | B_RELATION

-- Query planner:
--
-- Working towards a target.

data RangeType
  = RT_LT
  | RT_LTE
  | RT_EQ
  | RT_GTE
  | RT_GT

data DType
  = D_ROW
  | D_TAB
  | D_BITAB
  | D_SET

--
data Plan a where
  P_FROM_SET :: Set a -> Plan D_SET

  P_LOAD_TAB :: Clause -> Database -> Plan D_BITAB

  P_SELECT_SET :: Variable -> Plan a -> Plan D_SET

  P_JOIN_BITAB_BITAB :: Plan D_BITAB -> Plan D_BITAB -> Plan D_TAB
  P_JOIN_BITAB_TAB :: Plan D_BITAB -> Plan D_TAB -> Plan D_TAB
  P_JOIN_BITAB_SET :: Plan D_BITAB -> Plan D_SET -> Plan D_TAB
  P_JOIN_TAB_TAB :: Plan D_TAB -> Plan D_TAB -> Plan D_TAB
  P_JOIN_TAB_ROW :: Plan D_TAB -> Plan D_ROW -> Plan D_ROW
  P_JOIN_SET_SET :: Plan D_SET -> Plan D_SET -> Plan D_SET

  P_TO_ROW :: Plan x -> Plan D_ROW

  P_SORT_BY :: [Variable] -> Plan D_ROW -> Plan D_ROW
  P_RANGE_FILTER_COL :: RangeType -> Variable -> a -> Plan D_ROW -> Plan D_ROW

  -- = P_TARGET [Variable]

-- What's the query plan for

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

-- We want to

-- available (SET [?tag]) (SCALAR ?amount)
-- clause [?e :derp/tag ?tag]
-- future [?e ?upvotes ?amount ?derpid ?thumburl]
-- outtype (SET [?e]) (SCALAR ?amount)
-- out ( (LOAD_AV :derp/tag db)
--
-- because ?tag isn't used in the future, the plan should


-- The entire BITAB framing from the first sketch feels weird here when doing a
-- query planner: we want to know exactly which way to scan the data to try to
-- calculate

fullq db tagset amount = P_TO_ROW
  (P_JOIN_BITAB_TAB
    -- [?e :derp/thumburl ?thumburl]
    (P_LOAD_TAB (C_XAZ (SYM "?e") (ATTR ":derp/thumburl") (SYM "?thumburl")) db)

    (P_JOIN_BITAB_SET
      -- [?e :derp/id ?derpid]
      (P_LOAD_TAB (C_XAZ (SYM "?e") (ATTR ":derp/id") (SYM "?derpid")) db)

      -- range filter.
      (P_SELECT_SET (SYM "?e")
        (P_RANGE_FILTER_COL RT_GT (SYM "?upvotes") amount
          -- ?e->?upvotes
          (P_SORT_BY [(SYM "?upvotes")]
            (P_TO_ROW
              (P_JOIN_BITAB_SET
                -- [?e :derp/upvotes ?upvotes]
                (P_LOAD_TAB
                  (C_XAZ (SYM "?e") (ATTR ":derp/upvotes") (SYM "?upvotes")) db)
                -- (select ?e's which have all tags)
                (P_SELECT_SET (SYM "?e")
                 (P_JOIN_BITAB_SET
                   (P_LOAD_TAB
                     (C_XAZ (SYM "?e") (ATTR ":derp/tag") (SYM "?tag")) db)
                   (P_FROM_SET tagset))))))))))


        -- -- (select restrict the upvotes to a set: type is now `Set ?e`.)
        -- (P_SELECT_SET (SYM "?e")
        --   -- [(> ?upvotes ?amount)]
        --   (P_RANGE_FILTER RT_GT (SYM "?e") amount
        --    -- [?e :derp/upvotes ?upvotes]
        --    (P_LOAD_TAB (C_XAZ (SYM "?e") (ATTR ":derp/upvotes") (SYM "?upvotes")) db)))))

  -- (P_SELECT_SET (SYM "?e")
  --  (P_JOIN_BITAB_SET
  --    (P_LOAD (C_XAZ (SYM "?e") (ATTR ":derp/tag") (SYM "?tag")) db)
  --    (P_FROM_SET tagset)))

-- -- bad query, only for testing
-- xxx db = P_TO_ROW
--   (P_JOIN_BITAB_BITAB
--     (P_LOAD (C_XAZ (SYM "?e") (ATTR ":derp/thumburl") (SYM "?thumburl")) db)
--     (P_LOAD (C_XAZ (SYM "?e") (ATTR ":derp/id") (SYM "?derpid")) db))




-- OK, but how do you build the above plan in the first place?


-- mkPlan :: [Binding] -> [Clause] -> [Variable] -> Plan
-- mkPlan starting clauses target =

  -- NEXT ACTION: Build a query planner that narrows everything down.


-}
