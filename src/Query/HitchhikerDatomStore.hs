{-# OPTIONS_GHC -Wno-partial-fields   #-}
{-# OPTIONS_GHC -Wincomplete-patterns   #-}
{-# LANGUAGE GADTs #-}
module Query.HitchhikerDatomStore where

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
  vstorageSingleton (tx, v, o)

addDatom (!e, !a, !v, !tx, !o) (EAVROWS config (Just root)) =
  EAVROWS config $ Just $
  fixUp config (hhEDatomRowTF config) $
  insertRec config
            (hhEDatomRowTF config)
            (M.singleton e (ALeaf $ M.singleton a
                            (if o
                             then VSimple v tx
                             else VStorage Nothing historySet)))
            root
  where
    historySet =
      HitchhikerSetMapNodeLeaf $
      M.singleton tx (HSM.strip $ HS.singleton config (v, o))

-- Given a list of datoms, build out a
addDatoms :: forall e a v tx
          . (Show e, Show a, Show v, Show tx,
             Ord e, Ord a, Ord v, Ord tx)
         => [(e, a, v, tx, Bool)]
         -> EAVRows e a v tx
         -> EAVRows e a v tx
addDatoms ds (EAVROWS config Nothing) =
  EAVROWS config $ Just $ fixUp config (hhEDatomRowTF config) $
  splitLeafMany (hhEDatomRowTF config)
                (maxLeafItems config)
                (datomsToTree config ds)
addDatoms ds (EAVROWS config (Just top)) =
  EAVROWS config $ Just $
  fixUp config (hhEDatomRowTF config) $
  insertRec config (hhEDatomRowTF config) (datomsToTree config ds) top


-- Given a list of individual datoms, put all of them in a hitchhiker
-- compatible representation _first_ so we don't go through the a bunch of
-- times.
datomsToTree :: forall e a v tx
              . (Show e, Show a, Show v, Show tx, Ord a, Ord e, Ord v, Ord tx)
             => TreeConfig
             -> [(e, a, v, tx, Bool)] -> Map e (ADatomRow a v tx)
datomsToTree _ [] = mempty
datomsToTree config ds = go mempty ds
  where
    go :: Map e (ADatomRow a v tx) -> [(e, a, v, tx, Bool)]
       -> Map e (ADatomRow a v tx)
    go acc []                     = acc
    go acc (d@(e, _, _, _, _):ds) = go (M.alter (injectE d) e acc) ds

    injectE :: (e, a, v, tx, Bool) -> Maybe (ADatomRow a v tx)
            -> Maybe (ADatomRow a v tx)
    injectE (_, a, v, tx, op) Nothing =
      Just $ ALeaf $ M.singleton a $ vstorageSingleton (tx, v, op)
    injectE (_, a, v, tx, op) (Just adatoms) =
      Just $ arowInsertMany config
                            (M.singleton a $ vstorageSingleton (tx, v, op))
                            adatoms


hhEDatomRowTF :: (Show e, Show a, Show v, Show tx,
                  Ord e, Ord a, Ord v, Ord tx)
              => TreeConfig
              -> TreeFun e
                         (a, v, tx, Bool)
                         (EDatomRow e a v tx)
                         (Map e (ADatomRow a v tx))
                         (Map e (ADatomRow a v tx))
hhEDatomRowTF config = TreeFun {
  mkNode = ERowIndex,
  mkLeaf = ELeaf,
  caseNode = \case
      ERowIndex a b -> Left (a, b)
      ELeaf l       -> Right l,

  -- leafMap -> hhMap -> leafMap
  leafInsert = M.unionWith (mergeADatomRows config),
  leafMerge = error "eLeafInsert only required for deletion",
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = error "Pure deletion has to be handled otherwise",

  hhMerge = M.unionWith (mergeADatomRows config),
  -- If we just continue to collect attributes until we make some pins, that's
  -- actually fine.
  --
  -- TODO: The point of hhLength is to determine how much "stuff" we can store
  -- in a pin. We should actually recursively descend into ADatomRow, but stop
  -- recursive descent when we encounter a pin.
  hhLength = M.size,
  hhSplit = \k m -> M.spanAntitone (< k) m,
  hhEmpty = M.empty,
  hhDelete = error "Pure deletion has to be handled otherwise"
  }

-- -----------------------------------------------------------------------

mergeADatomRows :: (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
                => TreeConfig
                -> ADatomRow a v tx -> ADatomRow a v tx -> ADatomRow a v tx
mergeADatomRows config (ALeaf leftMap) (ALeaf rightMap) =
--  trace ("MERGE : " <> show leftMap <> ", " <> show rightMap) $
  fixUp config (hhADatomRowTF config) $
  splitLeafMany (hhADatomRowTF config) (maxLeafItems config) $
  M.unionWith (mergeVStorage config) leftMap rightMap

mergeADatomRows config main@(ARowIndex _ _) (ALeaf leaf)
  | M.null leaf = main
  | otherwise = arowInsertMany config leaf main

mergeADatomRows config (ALeaf leaf) main@(ARowIndex _ _)
  | M.null leaf = main
  | otherwise = arowInsertMany config leaf main

mergeADatomRows config left right =
  -- This is maybe bad. We probably want to have some sort of merge union.
  foldl' (\a m -> arowInsertMany config m a) left $
  getLeafList (hhADatomRowTF config) right

-- mergeADatomRows config (ARowIndex tree

unwrapHSM :: HitchhikerSetMap k v -> HitchhikerSetMapNode k v
unwrapHSM (HITCHHIKERSETMAP _ (Just x)) = x

mergeVStorage :: (Show v, Show tx, Ord v, Ord tx)
              => TreeConfig -> VStorage v tx -> VStorage v tx -> VStorage v tx
mergeVStorage config (VSimple lv ltx) (VSimple rv rtx) =
  VStorage (Just (HitchhikerSetNodeLeaf $ ssetFromList [lv, rv]))
           (unwrapHSM $
            HSM.insertMany [(ltx, (lv, True)), (rtx, (rv, True))] $
            HSM.empty config)

mergeVStorage config (VSimple lv ltx) vs@(VStorage _ _) =
  vstorageInsert config vs (ltx, lv, True)
mergeVStorage config vs@(VStorage _ _) (VSimple rv rtx) =
  vstorageInsert config vs (rtx, rv, True)

mergeVStorage config l@(VStorage ln lh) r@(VStorage rn rh)
  -- If one set of history happens purely after the other, replay the newer
  -- transactions on top of the older ones.
  | lhMax < rhMin = vstorageInsertMany config l $ toTxList rh
  | rhMax < lhMin = vstorageInsertMany config r $ toTxList lh
  | otherwise = error "Handle the hard overlapping case"
  where
    lhMin = HSM.findMinKey $ mkNode lh
    lhMax = HSM.findMaxKey $ mkNode lh
    rhMin = HSM.findMinKey $ mkNode rh
    rhMax = HSM.findMaxKey $ mkNode rh

    toTxList = map change . HSM.toList . mkNode
    change (a, (b, c)) = (a, b, c)
    mkNode x = HITCHHIKERSETMAP config (Just x)

arowInsertMany :: (Show a, Show v, Show tx, Ord a, Ord v, Ord tx)
               => TreeConfig
               -> Map a (VStorage v tx)
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
                         (Map a (VStorage v tx))
                         (Map a (VStorage v tx))
hhADatomRowTF config = TreeFun {
  mkNode = ARowIndex,
  mkLeaf = ALeaf,
  caseNode = \case
      ARowIndex a b -> Left (a, b)
      ALeaf l       -> Right l,

  -- -- leafMap -> hhMap -> leafMap
  leafInsert = M.unionWith (mergeVStorage config),
  leafMerge = error "eLeafInsert only required for deletion",
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = error "Pure deletion has to be handled otherwise",

  -- M.unionWith ssetUnion,
  hhMerge = M.unionWith (mergeVStorage config),
  -- TODO: Figure out policy around splitting here.
  hhLength =  M.size, -- sum . map length . M.elems,
  hhSplit = \k m -> M.spanAntitone (< k) m,
  hhEmpty = M.empty,
  hhDelete = error "Pure deletion has to be handled otherwise"
  }

-- -----------------------------------------------------------------------

mkSingletonTxMap (tx, v, op) =
  HitchhikerSetMapNodeLeaf $
  M.singleton tx $
  NAKEDSET $ Just $ HitchhikerSetNodeLeaf $ ssetSingleton (v, op)

vstorageSingleton :: (tx, v, Bool) -> VStorage v tx
vstorageSingleton (tx, v, True)   = VSimple v tx

-- Dumb, but the model allows it.
vstorageSingleton d@(_, _, False) = VStorage Nothing $ mkSingletonTxMap d

vstorageInsert :: (Show v, Show tx, Ord v, Ord tx)
               => TreeConfig
               -> VStorage v tx
               -> (tx, v, Bool)
               -> VStorage v tx
vstorageInsert config (VSimple pv ptx) newFact =
  vstorageInsert config
                 (VStorage (Just $ HitchhikerSetNodeLeaf $ ssetSingleton pv)
                           (mkSingletonTxMap (ptx, pv, True)))
                 newFact

vstorageInsert config (VStorage curSet txMap) (tx, v, op) =
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
                   -> [(tx, v, Bool)]
                   -> VStorage v tx
vstorageInsertMany config storage as =
  foldl' (vstorageInsert config) storage as

-- -----------------------------------------------------------------------

-- lookup :: forall e a v tx
--         . (Show a, Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
--        => e -> a -> EAVRows e a v tx -> HitchhikerSet v
-- lookup _ _ (EAVROWS config Nothing)    = HS.empty config

-- -- Lookup is more complicated in that we have to push matching transactions
-- -- down through multiple levels, to
-- lookup e a (EAVROWS config (Just top)) = lookInENode (ALeaf mempty) top
--   where
--     lookInENode :: ADatomRow a v tx -> EDatomRow e a v tx
--                 -> HitchhikerSet v
--     lookInENode hh = \case
--       ERowIndex index hitchhikers ->
--         lookInENode (mergeADatomRows config hh $ matchHitchhikers hitchhikers) $
--           findSubnodeByKey e index
--       ELeaf items -> case M.lookup e items of
--         Nothing     -> undefined -- HS.fromArraySet config $ applyLog mempty txs
--         Just anodes -> lookInANode hh anodes

--     matchHitchhikers :: Map e (ADatomRow a v tx) -> ADatomRow a v tx
--     matchHitchhikers hh = fromMaybe (ALeaf mempty) (M.lookup e hh)

--     lookInANode :: ADatomRow a v tx -> ADatomRow a v tx -> HitchhikerSet v
--     lookInANode = undefined

--     -- -- When recursing downwards through ERowIndex nodes, we must accumulate
--     -- -- matching
--     -- appendMatchingEHH :: ArraySet (v, tx, Bool)
--     --                   -> Map e (ArraySet (a, v, tx, Bool))
--     --                   -> ArraySet (v, tx, Bool)
--     -- appendMatchingEHH vtx hh = case M.lookup e hh of
--     --   Nothing   -> vtx
--     --   Just avtx -> ssetUnion vtx $ matchAV avtx
--     --   where
--     --     matchAV =
--     --       ssetMap removeA .
--     --       ssetTakeWhileAntitone takeFun .
--     --       ssetDropWhileAntitone dropFun
--     --       where
--     --         removeA (a, v, tx, o) = (v, tx, o)
--     --         dropFun (x, _, _, _) = (x < a)
--     --         takeFun (x, _, _, _) = (x == a)

--     -- lookInANode :: ArraySet (v, tx, Bool) -> ADatomRow a v tx -> HitchhikerSet v
--     -- lookInANode txs = \case
--     --   ARowIndex index hitchhikers ->
--     --     let newTxs = appendMatchingAHH txs hitchhikers
--     --     in lookInANode newTxs $ findSubnodeByKey a index
--     --   ALeaf items -> case M.lookup a items of
--     --     Nothing       -> HS.fromArraySet config $ applyLog mempty txs
--     --     Just vstorage -> applyToVStorage txs vstorage

--     -- appendMatchingAHH :: ArraySet (v, tx, Bool)
--     --                   -> Map a (ArraySet (v, tx, Bool))
--     --                   -> ArraySet (v, tx, Bool)
--     -- appendMatchingAHH vtx hh = case M.lookup a hh of
--     --   Nothing    -> vtx
--     --   Just hhVtx -> vtx <> hhVtx

--     -- applyLog :: ArraySet v -> ArraySet (v, tx, Bool) -> ArraySet v
--     -- applyLog = foldl' apply
--     --   where
--     --     apply set (v, _, True)  = ssetInsert v set
--     --     apply set (v, _, False) = ssetDelete v set

--     -- applyToVStorage :: ArraySet (v, tx, Bool)
--     --                 -> VStorage v tx
--     --                 -> HitchhikerSet v
--     -- applyToVStorage as vs =
--     --   case vstorageInsertMany config vs as of
--     --     VSimple v _    -> HS.singleton config v
--     --     VStorage top _ -> HITCHHIKERSET config top

-- -----------------------------------------------------------------------

-- OK, with the new data structures, how do we rewrite partialLookup? Because
-- we now need something with

partialLookup :: forall e a v tx
               . (Show e, Show a, Show v, Show tx, Ord e, Ord a, Ord v, Ord tx)
              => e -> EAVRows e a v tx -> HitchhikerSetMap a v
partialLookup _ (EAVROWS config Nothing)    = HSM.empty config

partialLookup e (EAVROWS config (Just top)) =
  -- trace ("LOOKUP " <> show e <> " in " <> show top) $
  lookInENode (ALeaf mempty) top
  where
    lookInENode :: ADatomRow a v tx -> EDatomRow e a v tx
                -> HitchhikerSetMap a v
    lookInENode hh = \case
      ERowIndex index hitchhikers ->
        -- trace ("HH KEYS: " <> (show $ M.keys hitchhikers)) $
        lookInENode (mergeADatomRows config hh $ matchHitchhikers hitchhikers) $
        findSubnodeByKey e index
      ELeaf items ->
        trace ("ITEMS: " <> (show $ M.keys items)) $
        case (M.lookup e items, hh) of
          (Nothing, ALeaf leaf)
            | M.null leaf -> trace ("Nothing, ALeaf null") $ HSM.empty config
          (Nothing, hh) -> trace ("hh") $ aNodeTo hh
          (Just anodes, ALeaf leaf)
            | M.null leaf -> trace ("just anodes") $ aNodeTo anodes
          (Just anodes, hh) -> trace ("full merge") $ aNodeTo $ mergeADatomRows config hh anodes

    matchHitchhikers :: Map e (ADatomRow a v tx) -> ADatomRow a v tx
    matchHitchhikers hh = fromMaybe (ALeaf mempty) (M.lookup e hh)

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
            Just $ NAKEDSET (Just (HitchhikerSetNodeLeaf $ ssetSingleton x))
          VStorage Nothing _ -> Nothing
          VStorage x _ -> Just $ NAKEDSET x

-- -----------------------------------------------------------------------

emptyDB :: Database
emptyDB = DATABASE {
  eav = emptyRows largeConfig,
  aev = emptyRows largeConfig,
  ave = emptyRows largeConfig,
  vae = emptyRows largeConfig
  }

learn :: (Value, Value, Value, Int, Bool)
      -> Database
      -> Database
learn (e, a, v, tx, op) db@DATABASE{..} = db {
  eav = addDatom (e, a, v, tx, op) eav,
  aev = addDatom (a, e, v, tx, op) aev,
  ave = addDatom (a, v, e, tx, op) ave,
  vae = addDatom (v, a, e, tx, op) vae
  }

learns :: [(Value, Value, Value, Int, Bool)]
       -> Database
       -> Database
learns ds db@DATABASE{..} = db {
  eav = addDatoms ds eav,
  aev = addDatoms (map eavToAev ds) aev,
  ave = addDatoms (map eavToAve ds) ave,
  vae = addDatoms (map eavToVae ds) vae
  }

eavToAev (e, a, v, tx, op) = (a, e, v, tx, op)
eavToAve (e, a, v, tx, op) = (a, v, e, tx, op)
eavToVae (e, a, v, tx, op) = (v, a, e, tx, op)
