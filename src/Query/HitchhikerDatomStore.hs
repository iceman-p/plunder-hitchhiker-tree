{-# OPTIONS_GHC -Wno-partial-fields   #-}
{-# OPTIONS_GHC -Wincomplete-patterns   #-}
{-# LANGUAGE GADTs      #-}
{-# LANGUAGE Strict     #-}
{-# LANGUAGE StrictData #-}
module Query.HitchhikerDatomStore where

import           ClassyPrelude    hiding (lookup)

import           Impl.Index
import           Impl.Leaf
import           Impl.Strict
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
import qualified Data.Map.Strict  as M
import qualified Data.Set         as S


emptyRows :: TreeConfig -> EAVRows e a v tx
emptyRows config = EAVROWS config SNothing

--
addDatom :: forall e a v tx
          . (Show e, Show a, Show v, Show tx,
             Ord e, Ord a, Ord v, Ord tx)
         => DatomEAV e a v tx
         -> EAVRows e a v tx
         -> EAVRows e a v tx
addDatom (DAT_EAV e a v tx o) (EAVROWS config SNothing) =
  -- Initialize everything to a single
  EAVROWS config $ SJust $
  ELeaf $ M.singleton e $!
  ALeaf $ M.singleton a $!
  vstorageSingleton $ DAT_TXV tx v o

addDatom datom (EAVROWS config (SJust root)) =
  EAVROWS config $ SJust $
  fixUp config (hhEDatomRowTF config) $
  insertRec config
            (hhEDatomRowTF config)
            (singletonCL datom)
            root

-- Given a list of datoms, build out a
addDatoms :: forall e a v tx
          . (Show e, Show a, Show v, Show tx,
             Ord e, Ord a, Ord v, Ord tx)
         => [DatomEAV e a v tx]
         -> EAVRows e a v tx
         -> EAVRows e a v tx
addDatoms ds (EAVROWS config SNothing) =
  EAVROWS config $ SJust $ fixUp config (hhEDatomRowTF config) $
  splitLeafMany (hhEDatomRowTF config)
                (maxLeafItems config)
                (datomsToTree config ds)
addDatoms ds (EAVROWS config (SJust top)) =
  EAVROWS config $ SJust $
  fixUp config (hhEDatomRowTF config) $
  insertRec config (hhEDatomRowTF config) (toCountList ds) top


-- Given a list of individual datoms, put all of them in a hitchhiker
-- compatible representation _first_ so we don't go through the a bunch of
-- times.
datomsToTree :: forall e a v tx
              . (Show e, Show a, Show v, Show tx, Ord a, Ord e, Ord v, Ord tx)
             => TreeConfig
             -> [DatomEAV e a v tx] -> Map e (ADatomRow a v tx)
datomsToTree _ [] = mempty
datomsToTree config ds = go mempty ds
  where
    go :: Map e (ADatomRow a v tx) -> [DatomEAV e a v tx]
       -> Map e (ADatomRow a v tx)
    go !acc []                         = acc
    go !acc (d@(DAT_EAV e _ _ _ _):ds) = go (M.alter (injectE d) e acc) ds

    injectE x@(DAT_EAV _ a v tx op) Nothing =
      Just $! ALeaf $! M.singleton a $! vstorageSingleton $ DAT_TXV tx v op
    injectE (DAT_EAV _ a v tx op) (Just !adatoms) =
      Just $! arowInsertMany config [(DAT_AV a v tx op)] adatoms


hhEDatomRowTF :: (Show e, Show a, Show v, Show tx,
                  Ord e, Ord a, Ord v, Ord tx)
              => TreeConfig
              -> TreeFun e
                         (a, v, tx, Bool)
                         (EDatomRow e a v tx)
                         (CountList (DatomEAV e a v tx))
                         (Map e (ADatomRow a v tx))
hhEDatomRowTF config = TreeFun {
  mkNode = ERowIndex,
  mkLeaf = ELeaf,
  caseNode = \case
      ERowIndex a b -> CaseIndex a b
      ELeaf l       -> CaseLeaf l,

  -- leafMap -> hhMap -> leafMap
  leafInsert = edatomLeafInsert config,
  leafMerge = \a b -> error "eLeafInsert only required for deletion",
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = \a b -> error "Pure deletion has to be handled otherwise",

  hhMerge = countListMerge,
  hhLength = countListSize,
  hhWholeSplit = edatomHHWholeSplit,
  hhEmpty = emptyCL,
  hhDelete = \a b -> error "Pure deletion has to be handled otherwise"
  }

-- -----------------------------------------------------------------------

edatomLeafInsert :: (Show e, Show a, Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
                 => TreeConfig
                 -> Map e (ADatomRow a v tx)
                 -> (CountList (DatomEAV e a v tx))
                 -> Map e (ADatomRow a v tx)
edatomLeafInsert config map (COUNTLIST _ insertions) =
  go map $ reverse insertions
  where
    go !map []                             = map
    go !map (tuple@(DAT_EAV e _ _ _ _):is) = go (M.alter (merge tuple) e map) is

    merge (DAT_EAV e a v tx op) c =
--      trace ("edatom insert " <> show orig) $
      case c of
        Nothing  -> Just $ ALeaf $ M.singleton a $
                    vstorageSingleton (DAT_TXV tx v op)
        -- TODO: Once you've confirmed that the new edatom list based
        -- implementation works, move on at this join point to make adatom work.
        Just !ad -> Just $ arowInsertMany config [DAT_AV a v tx op] ad

edatomHHWholeSplit :: (Ord e, Ord a, Ord v, Ord tx)
                   => [e] -> (CountList (DatomEAV e a v tx))
                   -> [CountList (DatomEAV e a v tx)]
edatomHHWholeSplit = doWholeSplit altk
  where
    altk k (DAT_EAV e _ _ _ _) = e < k

unwrapHSM :: HitchhikerSetMap k v -> HitchhikerSetMapNode k v
unwrapHSM (HITCHHIKERSETMAP _ (Just x)) = x

arowInsertMany :: (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
               => TreeConfig
               -> [DatomAV a v tx]
               -> ADatomRow a v tx
               -> ADatomRow a v tx
arowInsertMany config !items top =
  fixUp config (hhADatomRowTF config) $
  insertRec config (hhADatomRowTF config) (toCountList items) top

hhADatomRowTF :: (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
              => TreeConfig
              -> TreeFun a
                         (v, tx, Bool)
                         (ADatomRow a v tx)
                         (CountList (DatomAV a v tx))
                         (Map a (VStorage v tx))
hhADatomRowTF config = TreeFun {
  mkNode = ARowIndex,
  mkLeaf = ALeaf,
  caseNode = \case
      ARowIndex a b -> CaseIndex a b
      ALeaf l       -> CaseLeaf l,

  -- -- leafMap -> hhMap -> leafMap
  leafInsert = adatomLeafInsert config, --   M.unionWith (mergeVStorage config),
  leafMerge = \a b -> error "eLeafInsert only required for deletion",
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = \a b -> error "Pure deletion has to be handled otherwise",

  hhMerge = countListMerge,
  hhLength = countListSize,
  hhWholeSplit = adatomHHWholeSplit,
  hhEmpty = emptyCL,
  hhDelete = \a b -> error "Pure deletion has to be handled otherwise"
  }

adatomLeafInsert :: (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
                 => TreeConfig
                 -> Map a (VStorage v tx)
                 -> (CountList (DatomAV a v tx))
                 -> Map a (VStorage v tx)
adatomLeafInsert config map (COUNTLIST _ insertions) =
--  trace ("adatomLeafInsert " <> show insertions) $
  go map $ reverse insertions
  where
    go !map []                          = map
    go !map (tuple@(DAT_AV a _ _ _):is) = go (M.alter (merge tuple) a map) is

    merge (DAT_AV _ v tx op) = \case
      Nothing ->
        Just $! vstorageSingleton $ DAT_TXV tx v op
      -- TODO: Once you've confirmed that the new edatom list based
      -- implementation works, move on at this join point to make adatom work.
      Just !vs -> Just $! vstorageInsertMany config vs [(DAT_TXV tx v op)]

adatomHHWholeSplit :: (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
                   => [a] -> CountList (DatomAV a v tx)
                   -> [CountList (DatomAV a v tx)]
adatomHHWholeSplit = doWholeSplit altk
  where
    altk k (DAT_AV a _ _ _) = a < k

-- -----------------------------------------------------------------------

mkSingletonTxMap !tuple@(DAT_TXV tx _ _) = TxHistory tx [tuple]

insertTxMap :: TxHistory v tx -> DatomTxV v tx -> TxHistory v tx
insertTxMap (TxHistory origTx txs) !newTx = TxHistory origTx (newTx:txs)

vstorageSingleton :: DatomTxV v tx -> VStorage v tx
vstorageSingleton (DAT_TXV tx v True) = VSimple v tx

-- Dumb, but the model allows it.
vstorageSingleton d@(DAT_TXV _ _ False)     =
  VStorage SNothing $ mkSingletonTxMap d

vstorageInsert :: (Show v, Show tx, Ord v, Ord tx)
               => TreeConfig
               -> VStorage v tx
               -> DatomTxV v tx
               -> VStorage v tx
vstorageInsert config (VSimple pv ptx) newFact =
  let !singpv = ssetSingleton pv
  in vstorageInsert config
                    (VStorage (SJust $ HitchhikerSetNodeLeaf $ singpv)
                              (mkSingletonTxMap newFact))
                    newFact

vstorageInsert config (VStorage !curSet !txMap) !newTx =
  VStorage newSet newTxMap
  where
    newSet = insertToValSet config curSet newTx
    newTxMap = insertTxMap txMap newTx

vstorageInsertMany :: (Show v, Show tx, Ord v, Ord tx)
                   => TreeConfig
                   -> VStorage v tx
                   -> [DatomTxV v tx]
                   -> VStorage v tx
vstorageInsertMany config !storage !as =
  foldl' (vstorageInsert config) storage as

insertToValSet :: (Show v, Ord v)
               => TreeConfig
               -> StrictMaybe (HitchhikerSetNode v)
               -> DatomTxV v tx
               -> StrictMaybe (HitchhikerSetNode v)
insertToValSet config curSet (DAT_TXV tx v op) = case (curSet, op) of
      (SNothing, True)    -> SJust $ HitchhikerSetNodeLeaf $ ssetSingleton v
      (SNothing, False)   -> SNothing
      (SJust !set, True)  -> SJust $ HS.insertRaw config v set
      (SJust !set, False) -> HS.deleteRaw config v set

-- -----------------------------------------------------------------------

fullLookup :: forall e a v tx
        . (Show e, Show a, Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
       => e -> a -> EAVRows e a v tx -> HitchhikerSet v
fullLookup _ _ (EAVROWS config SNothing)    = HS.empty config
fullLookup e a rows@(EAVROWS config (SJust top)) =
  HSM.lookup a $ partialLookup e rows

-- -----------------------------------------------------------------------

partialLookup :: forall e a v tx
               . (Show e, Show a, Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
              => e -> EAVRows e a v tx -> HitchhikerSetMap a v
partialLookup _ (EAVROWS config SNothing)    = HSM.empty config

partialLookup e (EAVROWS config (SJust top)) = lookInENode mempty top
  where
    lookInENode :: [DatomAV a v tx]
                -> EDatomRow e a v tx
                -> HitchhikerSetMap a v
    lookInENode hh = \case
      ERowIndex index hitchhikers ->
        lookInENode (hh <> matchHitchhikers hitchhikers) $
        findSubnodeByKey e index
      ELeaf items ->
        case M.lookup e items of
          Nothing
            | null hh -> HSM.empty config
            | otherwise -> aNodeTo $ arowInsertMany config hh $ ALeaf mempty
          Just anodes
            | null hh -> aNodeTo anodes
            | otherwise -> aNodeTo $
                arowInsertMany config hh anodes

    matchHitchhikers :: CountList (DatomEAV e a v tx) -> [DatomAV a v tx]
    matchHitchhikers (COUNTLIST _ rows) = go rows
      where
        go [] = []
        go ((DAT_EAV cure a v t op):ds) = if e == cure
                                          then (DAT_AV a v t op):(go ds)
                                          else go ds

    aNodeTo :: ADatomRow a v tx -> HitchhikerSetMap a v
    aNodeTo anodes = HITCHHIKERSETMAP config $ Just $ translate arow
      where
        arow = flushDownwards (hhADatomRowTF config) anodes
        translate = \case
          ARowIndex tree hh ->
            HitchhikerSetMapNodeIndex (mapIndex translate tree) mempty
          ALeaf leafMap ->
            HitchhikerSetMapNodeLeaf $ M.mapMaybe translateLeaf leafMap

        translateLeaf = \case
          VSimple x _ ->
            let !sj = SJust $ HitchhikerSetNodeLeaf $ ssetSingleton x
            in Just $ NAKEDSET sj
          VStorage SNothing _ -> Nothing
          VStorage !x _ -> Just $ NAKEDSET x


-- -----------------------------------------------------------------------

emptyDB :: Database
emptyDB = learns prelude startingVal
  where
    baseTx = 2^16 + 1

    -- Keep this synchronized with `attributes` below!
    prelude = map (\(a,b,c) -> (ENTREF $ ENTID a, ENTID b, c, True)) $ [
      (1, 1, VAL_STR ":db/ident"),
      (1, 2, VAL_ENTID $ ENTID 3), -- cardinality one
      (1, 5, VAL_ENTID $ ENTID 8), -- type string
      -- TODO: uniqueness must be set on ident when I add :db/unique.
      (1, 9, VAL_INT 1), -- should be unique instead of indexed

      -- Cardinality and its enumerations
      (2, 1, VAL_STR ":db/cardinality"),
      (2, 2, VAL_ENTID $ ENTID 3),
      (2, 5, VAL_ENTID $ ENTID 6),
      (3, 1, VAL_STR ":db.cardinality/one"),
      (4, 1, VAL_STR ":db.cardinality/many"),

      -- Value type and its enumerations
      (5, 1, VAL_STR ":db/valueType"),
      (5, 3, VAL_ENTID $ ENTID 3),
      (5, 5, VAL_ENTID $ ENTID 6),
      (6, 1, VAL_STR ":db.type/ref"),
      (7, 1, VAL_STR ":db.type/int"),
      (8, 1, VAL_STR ":db.type/string"),

      -- Index flag (does this value go in the AVE table?)
      (9, 1, VAL_STR ":db/index"),
      (9, 5, VAL_ENTID $ ENTID 7) -- should be bool
      ]

    -- Same data as above, but in the quick lookup format so we can actually
    -- store the above correctly.
    attributes = M.fromList [
      (":db/ident", ENTID 1),
      (":db/cardinality", ENTID 2),
      (":db/valueType", ENTID 5),
      (":db/index", ENTID 9)
      ]
    attributeProps = M.fromList [
      (ENTID 1, (True, ONE, VT_STR)),
      (ENTID 2, (False, ONE, VT_ENTITY)),
      (ENTID 5, (False, ONE, VT_ENTITY)),
      (ENTID 9, (False, ONE, VT_INT))
      ]

    startingVal = DATABASE {
      nextTransaction = baseTx,
      nextEntity = 10,
      attributes,
      attributeProps,

      eav = emptyRows largeConfig,
      aev = emptyRows largeConfig,
      ave = emptyRows largeConfig,
      vae = emptyRows largeConfig
      }

-- Technically, we should be fishing attribute changes out of general `learn`
-- calls but I don't want to write that data right now.
learnAttribute :: Text -> Bool -> Cardinality -> ValueType -> Database
               -> Database
learnAttribute name indexed cardinality vtype db =
  let entity = nextEntity db
  in learns [
    (ENTREF $ ENTID entity, ENTID 1, VAL_STR name, True),
    (ENTREF $ ENTID entity, ENTID 2,
     VAL_ENTID $ ENTID $ case cardinality of { ONE -> 3; MANY -> 4 }, True),
    (ENTREF $ ENTID entity, ENTID 9, VAL_INT $ fromEnum indexed, True)
    ] $
    db { nextEntity = (nextEntity db) + 1,
         nextTransaction = (nextTransaction db) + 1,
         attributes = M.insert name (ENTID entity) (attributes db),
         attributeProps =
           M.insert (ENTID entity) (indexed, cardinality, vtype)
                    (attributeProps db)
       }

-- TODO: We have to make learn and learns allocate their own transaction
-- numbers at this point.

data EntityRef
  = TMPREF Int  -- Resolved to a new id
  | ENTREF EntityId

-- learns :: [(EntityRef, EntityId, Value, Bool)]
--        -> Database
--        -> Database

-- Given some input
resolve :: Int
        -> Int
        -> [(EntityRef, EntityId, Value, Bool)]
        -> (Int, [(EntityId, EntityId, Value, Int, Bool)])
resolve nextEid tx input = pick $ foldl' go (mempty, nextEid, []) input
  where
    pick (refmap, nextEid, m) = (nextEid, reverse m)

    go (refmap, nextEid, entities) (TMPREF i, a, v, op)
      | Just entity <- M.lookup i refmap =
          (refmap, nextEid, (ENTID entity, a, v, tx, op):entities)
      | otherwise = ( M.insert i nextEid refmap
                    , nextEid + 1
                    , (ENTID nextEid, a, v, tx, op):entities)
    go (refmap, nextEid, entities) (ENTREF i, a, v, op)
      = (refmap, nextEid, (i, a, v, tx, op):entities)

-- TODO: Just redo this entire thing to make it easier to ingest data. Each
-- thing we add here makes it less likely to really work.
learns :: [(EntityRef, EntityId, Value, Bool)]
       -> Database
       -> Database
learns rawDatoms db@DATABASE{..} =
  let tx = nextTransaction
      (newNextEid, resolvedDatoms) = resolve nextEntity tx rawDatoms
  in db {
    nextTransaction = nextTransaction + 1,
    nextEntity = newNextEid,
    eav = addDatoms (map eavToEav resolvedDatoms) eav,
    aev = addDatoms (map eavToAev resolvedDatoms) aev,
    ave = addDatoms (mapMaybe (eavToAve attributeProps) resolvedDatoms) ave,
    vae = addDatoms (mapMaybe (eavToVae attributeProps) resolvedDatoms) vae
  }

eavToEav (e, a, v, tx, op) = DAT_EAV e a v tx op

eavToAev (e, a, v, tx, op) = DAT_EAV a e v tx op

eavToAve attributes (e, a, v, tx, op)
  | Just (indexed, _, _) <- M.lookup a attributes
  , indexed
    = Just (DAT_EAV a v e tx op)
  | otherwise = Nothing

eavToVae attributes (e, a, v, tx, op)
  | Just (_, _, valType) <- M.lookup a attributes
  , valType == VT_ENTITY
    = Just (DAT_EAV v a e tx op)
  | otherwise = Nothing
