{-# OPTIONS_GHC -Wno-partial-fields   #-}
{-# OPTIONS_GHC -Wincomplete-patterns   #-}
module HitchhikerDatomStore where

import           ClassyPrelude    hiding (lookup)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import           Data.Sorted

import qualified HitchhikerMap    as HM
import qualified HitchhikerSet    as HS
import qualified HitchhikerSetMap as HSM

import qualified Data.Map         as M
import qualified Data.Set         as S

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
               . (Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
              => e -> EAVRows e a v tx -> HitchhikerSetMap a v
partialLookup _ (EAVROWS config Nothing)    = HSM.empty config

partialLookup e (EAVROWS config (Just top)) = undefined
-- TODO: OK, what has to happen here?
--
-- First step: we have to flush hitchhikers that match e downwards to the a
-- row.
--
-- Second step: we have to lazily generate a HitchhikerSetMap from the
-- combination of the ADatomRow and VStorage.


-- -----------------------------------------------------------------------

newtype EntityId = ENTID Int
  deriving (Show, Eq, Ord)

-- The attribute type
newtype Attr = ATTR Text
  deriving (Show, Eq, Ord)

data Value
  = VAL_ATTR Attr
  | VAL_ENTID EntityId
  | VAL_INT Int
  | VAL_STR String
  deriving (Show, Eq, Ord)

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
  | RSET Symbol (HitchhikerSet Value)
  -- Bidirectional relation. Created in the first clause when we don't know
  -- what direction we'll be searching.
  | RBIDIR Symbol
           Symbol
           (HitchhikerSetMap Value Value)
           (HitchhikerSetMap Value Value)
  -- A table. A table is a list of the column symbol names, and then a mapping
  -- from
  | RTAB [Symbol] TableElem
  deriving (Show)

data TableElem
  = TableEnd Symbol Symbol (HitchhikerSetMap Value Value)
  | TableCont Symbol (HitchhikerSetMap Value TableElem)
  deriving (Show)

-- A variable reference like ?name
data Symbol = SYM Text
  deriving (Show, Ord, Eq)

-- Type pattern EAV for binding value, XYZ for binding symbol
data Clause
  = C_EAZ EntityId Attr Symbol
  -- TODO: Possible in the model, but requires iteration.
  -- | C_EYV EntityId Symbol Value
  | C_XAV Symbol Attr Value

  | C_EYZ EntityId Symbol Symbol
  | C_XAZ Symbol Attr Symbol
  | C_XYV Symbol Symbol Value

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
  | lhs == x = RTAB [x, y] (TableEnd x y $ hhSetMapRestrictKeys lhv xtoy)
  | lhs == y = RTAB [y, x] (TableEnd y x $ hhSetMapRestrictKeys lhv ytox)
  | otherwise = RDISJOINT

rjoin l@(RTAB _ _) r@(RSET _ _) = rjoin r l
rjoin (RSET lhs lhv) (RTAB rhs rht)
  | not $ elem lhs rhs = RDISJOINT
  | otherwise = RTAB rhs (filteredTable rht)
  where
    filteredTable (TableEnd key val hhSetMap)
      | lhs == key = TableEnd key val $ hhSetMapRestrictKeys lhv hhSetMap
      | otherwise = TableEnd key val $ hhSetMapRestrictVals lhv hhSetMap
    filteredTable (TableCont key hhSetMap)
      | lhs == key = TableCont key $ hhSetMapRestrictKeys lhv hhSetMap
      | otherwise = TableCont key $ hhSetMapMapVals filteredTable hhSetMap

rjoin (RTAB lhs lht) (RTAB rhs rht) = undefined
-- What are the issues on tab to tab joining?
--
-- We'll need to
--
-- [?one ?two] [?two ?three] -> [?one ?two ?three]  ; easy
-- [?one ?two ?three] [?two ?four] ->

--    (sut/q '[:find ?nation
--             :in $ ?alias
--             :where
--             [?e :aka ?alias]
--             [?e :nation ?nation]]
--
-- [?alias] [?e :aka ?alias] -> [?alias ?e]
-- [?alias ?e] [?e :nation ?nation] -> [?alias ?e ?nation]


-- TODO: Implement these
hhSetMapRestrictKeys keySet setMap = undefined
hhSetMapRestrictVals valSet setMap = undefined

hhSetMapMapVals :: (v -> v) -> HitchhikerSetMap k v -> HitchhikerSetMap k v
hhSetMapMapVals = undefined


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

loadClause (C_XAZ eSymb attr vSymb) DATABASE{..} =
  RBIDIR eSymb vSymb ev ve
  where
    ev = partialLookup (VAL_ATTR attr) aev
    ve = partialLookup (VAL_ATTR attr) ave


-- loadClause (C_XAZ eSymb attr vSymb) DATABASE{..} rset@(RSET rSymb rSet)
--   | rSymb == eSymb =
--       rjoin rset $ RTAB [eSymb, vSymb] $ partialLookup (VAL_ATTR attr) aev
--   | rSymb == vSymb =
--       rjoin rset $ RTAB [vSymb, eSymb] $ partialLookup (VAL_ATTR attr) ave
--   | otherwise =
--       RDIS




--   | C_EYZ EntityId Symbol Symbol


-- Equivalent to `hash-join` in query.clj. This does a single
-- hashJoin :: Relation -> Relation -> Relation

-- hashJoin (RAW_TUPLES lNames lVals) (RAW_TUPLES rNames rVals)  =
--   let lKeep = keysSet lNames
--       rKeep = S.difference (keysSet rNames) lKeep
--       oNames =
--   in RAW_TUPLES oNames oVals




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
out = rjoin
    (loadClause (C_XAZ (SYM "?e") (ATTR ":aka") (SYM "?alias")) db)
    (RSET (SYM "?alias") (HS.singleton twoThreeConfig (VAL_STR "fred")))




inAlias :: Relation
inAlias = RSET (SYM "?alias") (HS.singleton twoThreeConfig (VAL_STR "fred"))

-- inAlias :: Relation
-- inAlias = RAW_TUPLES (M.singleton (SYM "?alias") 0) [[VAL_STR "fred"]]

-- eAlias :: Relation
-- eAlias = RAW_TUPLES (M.fromList [(SYM "?e", 0), (SYM "?alias", 1)]) [
--   [VAL_INT 100, VAL_STR "foo"],
--   [VAL_INT 100, VAL_STR "fred"]]

-- eNation :: Relation
-- eNation = RAW_TUPLES (M.fromList [(SYM "?e", 0), (SYM "?nation", 1)]) [
--   [VAL_INT 100, VAL_STR "France"],
--   [VAL_INT 101, VAL_STR "france"],
--   [VAL_INT 102, VAL_STR "English"]]

-- res = hashJoin eNation $ hashJoin inAlias eAlias

-- data Relation
--   -- A single
--   = R_ONECONST Symbol Value
--   | R_
