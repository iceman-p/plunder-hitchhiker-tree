{-# OPTIONS_GHC -Wno-partial-fields   #-}
module HitchhikerDatomStore where

import           ClassyPrelude

import           Data.Map                     (Map)
import           Data.Set                     (Set)

import           Types

import           Impl.Tree
import           Impl.Types

import           Data.Sorted

import qualified HitchhikerMap                as HM
import qualified HitchhikerSet                as HS

import qualified Data.Map                     as M
import qualified Data.Vector                  as V
import qualified Data.Vector.Algorithms.Merge as VA

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

data ADatomRow a v tx
  = ARowIndex (TreeIndex a (ADatomRow a v tx))
              (Map a (ArraySet (v, tx, Bool)))
  | ALeaf (Map a (VStorage v tx))
  deriving (Show, Generic, NFData)

-- At the end is VStorage: a set copy of the current existing values, and a
-- separate log of transactions.
data VStorage v tx
  = VStorage (Maybe (HitchhikerSetNode v)) (HitchhikerMapNode tx (v, Bool))
  deriving (Show, Generic, NFData)

data EAVRows e a v tx = EAVROWS {
  config :: TreeConfig,
  root   :: Maybe (EDatomRow e a v tx)
  }
  deriving (Show)

empty :: TreeConfig -> EAVRows e a v tx
empty config = EAVROWS config Nothing

data Cardinality
  = MANY
  | ONCE
  deriving (Show, Eq)

lookup :: Ord k => Cardinality -> k -> k -> EAVRows e a v tx -> ArraySet k
lookup _ _ _ (EAVROWS _ Nothing) = mempty


--
addDatom :: forall e a v tx
          . (Show e, Show a, Show v, Show tx,
             Ord e, Ord a, Ord v, Ord tx)
         => e
         -> a
         -> v
         -> tx
         -> Bool
         -> EAVRows e a v tx
         -> EAVRows e a v tx
addDatom !e !a !v !tx !o (EAVROWS config Nothing) =
  -- Initialize everything to a single
  EAVROWS config $ Just $
  ELeaf $ M.singleton e $
  ALeaf $ M.singleton a $
  vstorageSingleton (v, tx, o)

addDatom !e !a !v !tx !o (EAVROWS config (Just root)) =
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
      ELeaf l -> Right l,

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
      ALeaf l -> Right l,

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

vstorageSingleton :: (v, tx, Bool) -> VStorage v tx
vstorageSingleton (v, tx, op) = VStorage valSet txMap
  where
    txMap = HitchhikerMapNodeLeaf $ M.singleton tx (v, op)
    valSet = case op of
      True  -> Just $ HitchhikerSetNodeLeaf $ ssetSingleton v
      False -> Nothing

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
vstorageInsert config (VStorage curSet txMap) (v, tx, op) =
  VStorage valSet newTxMap
  where
    valSet = case (curSet, op) of
      (Nothing, True)   -> Just $ HitchhikerSetNodeLeaf $ ssetSingleton v
      (Nothing, False)  -> Nothing
      (Just set, True)  -> Just $ HS.insertRaw config v set
      (Just set, False) -> HS.deleteRaw config v set

    newTxMap = HM.insertRaw config tx (v, op) txMap

vstorageInsertMany :: (Show v, Show tx, Ord v, Ord tx)
                   => TreeConfig
                   -> VStorage v tx
                   -> ArraySet (v, tx, Bool)
                   -> VStorage v tx
vstorageInsertMany config storage as =
  foldl' (vstorageInsert config) storage as

-- -----------------------------------------------------------------------

data Value
  = VAL_STR String
  | VAL_INT Int

type EAVStore = EAVRows Int Int Value Int

