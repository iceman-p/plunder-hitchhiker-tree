module HitchhikerSet where

import           Data.Map      (Map)
import           Data.Maybe
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Debug.Trace

import           Impl.Tree
import           Index
import           Leaf
import           Types
import           Utils

import qualified Data.Foldable as F
import qualified Data.Map      as M
import qualified Data.Sequence as Q
import qualified Data.Set      as S

empty :: TreeConfig -> HitchhikerSet k
empty config = HITCHHIKERSET config Nothing

singleton :: TreeConfig -> k -> HitchhikerSet k
singleton config k
  = HITCHHIKERSET config (Just $ HitchhikerSetNodeLeaf $ Q.singleton k)

fromSeq :: (Show k, Ord k) => TreeConfig -> Seq k -> HitchhikerSet k
fromSeq config ks = foldl (flip insert) (empty config) ks

toSet :: (Show k, Ord k) => HitchhikerSet k -> S.Set k
toSet (HITCHHIKERSET config Nothing) = S.empty
toSet (HITCHHIKERSET config (Just root)) = collect root
  where
    collect = \case
      HitchhikerSetNodeIndex (Index _ nodes) hh ->
        foldl (<>) (mkSet hh) $ fmap collect nodes
      HitchhikerSetNodeLeaf l -> mkSet l

    mkSet = S.fromList . F.toList

insert :: (Show k, Ord k) => k -> HitchhikerSet k -> HitchhikerSet k
insert !k !(HITCHHIKERSET config (Just root)) = HITCHHIKERSET config $ Just $
  fixUp config hhSetTF $ insertRec config hhSetTF (Q.singleton k) root

insert !k (HITCHHIKERSET config Nothing)
  = HITCHHIKERSET config (Just $ HitchhikerSetNodeLeaf $ Q.singleton k)

-- -----------------------------------------------------------------------

hhSetTF :: Ord k => TreeFun k (HitchhikerSetNode k) k k
hhSetTF = TreeFun {
  mkIndex = HitchhikerSetNodeIndex,
  mkLeaf = HitchhikerSetNodeLeaf,
  caseNode = \case
      HitchhikerSetNodeIndex a b -> Left (a, b)
      HitchhikerSetNodeLeaf l    -> Right l,
  leafMerge = foldl $ \items k -> qSortedInsert k items,
  hhMerge = foldl $ \items k -> qSortedInsert k items,
  leafKey = id,
  hhKey = id
  }

-- -----------------------------------------------------------------------

member :: Ord k => k -> HitchhikerSet k -> Bool
member key (HITCHHIKERSET _ Nothing) = False
member key (HITCHHIKERSET _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerSetNodeIndex index hitchhikers ->
        case findInSetHitchhikers key hitchhikers of
          True  -> True
          False -> lookInNode $ findSubnodeByKey key index
      HitchhikerSetNodeLeaf items -> findInSetLeaves key items

findInSetHitchhikers :: Eq k => k -> Seq k -> Bool
findInSetHitchhikers key hh = isJust $ Q.findIndexR (== key) hh

findInSetLeaves :: Eq k => k -> Seq k -> Bool
findInSetLeaves key leaves = isJust $ Q.findIndexL (== key) leaves
