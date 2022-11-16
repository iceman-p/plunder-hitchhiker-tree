module HitchhikerSet ( empty
                     , null
                     , singleton
                     , fromSet
                     , toSet
                     , insert
                     , insertMany
                     , member
                     , intersection
                     ) where

import           ClassyPrelude hiding (empty, intersection, member, null,
                                singleton)

import           Data.Set      (Set)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import qualified Data.Foldable as F
import qualified Data.Map      as M
import qualified Data.Set      as S

empty :: TreeConfig -> HitchhikerSet k
empty config = HITCHHIKERSET config Nothing

null :: HitchhikerSet k -> Bool
null (HITCHHIKERSET config tree) = not $ isJust tree

singleton :: TreeConfig -> k -> HitchhikerSet k
singleton config k
  = HITCHHIKERSET config (Just $ HitchhikerSetNodeLeaf $ S.singleton k)

fromSet :: (Show k, Ord k) => TreeConfig -> Set k -> HitchhikerSet k
fromSet config ks
  | S.null ks = empty config
  | otherwise = insertMany ks $ empty config

toSet :: (Show k, Ord k) => HitchhikerSet k -> S.Set k
toSet (HITCHHIKERSET config Nothing) = S.empty
toSet (HITCHHIKERSET config (Just root)) = collect root
  where
    collect = \case
      HitchhikerSetNodeIndex (TreeIndex _ nodes) hh ->
        foldl' (<>) (mkSet hh) $ fmap collect nodes
      HitchhikerSetNodeLeaf l -> mkSet l

    mkSet = S.fromList . F.toList

insert :: (Show k, Ord k) => k -> HitchhikerSet k -> HitchhikerSet k
insert !k !(HITCHHIKERSET config (Just root)) = HITCHHIKERSET config $ Just $
  fixUp config hhSetTF $ insertRec config hhSetTF (S.singleton k) root

insert !k (HITCHHIKERSET config Nothing)
  = HITCHHIKERSET config (Just $ HitchhikerSetNodeLeaf $ S.singleton k)


insertMany :: (Show k, Ord k) => Set k -> HitchhikerSet k -> HitchhikerSet k
insertMany !items !(HITCHHIKERSET config Nothing) =
  HITCHHIKERSET config $ Just $
  fixUp config hhSetTF $
  splitLeafMany hhSetTF (maxLeafItems config) items

insertMany !items !(HITCHHIKERSET config (Just top)) =
  HITCHHIKERSET config $ Just $
  fixUp config hhSetTF $
  insertRec config hhSetTF items top

-- -----------------------------------------------------------------------

hhSetTF :: Ord k => TreeFun k (HitchhikerSetNode k) (Set k) (Set k)
hhSetTF = TreeFun {
  mkNode = HitchhikerSetNodeIndex,
  mkLeaf = HitchhikerSetNodeLeaf,
  caseNode = \case
      HitchhikerSetNodeIndex a b -> Left (a, b)
      HitchhikerSetNodeLeaf l    -> Right l,

  leafMerge = S.union,
  leafLength = S.size,
  leafSplitAt = S.splitAt,
  leafFirstKey = S.findMin,
  leafEmpty = S.empty,

  hhMerge = S.union,
  hhLength = S.size,
  hhSplit = splitImpl,
  hhEmpty = S.empty
  }

splitImpl :: Ord k => k -> Set k -> (Set k, Set k)
splitImpl k s = S.spanAntitone (< k) s

-- -----------------------------------------------------------------------

member :: Ord k => k -> HitchhikerSet k -> Bool
member key (HITCHHIKERSET _ Nothing) = False
member key (HITCHHIKERSET _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerSetNodeIndex index hitchhikers ->
        case S.member key hitchhikers of
          True  -> True
          False -> lookInNode $ findSubnodeByKey key index
      HitchhikerSetNodeLeaf items -> S.member key items

-- -----------------------------------------------------------------------

-- set merge: step one

intersection :: (Show k, Ord k)
             => HitchhikerSet k -> HitchhikerSet k -> HitchhikerSet k
intersection n@(HITCHHIKERSET _ Nothing) _ = n
intersection _ n@(HITCHHIKERSET _ Nothing) = n
intersection (HITCHHIKERSET conf (Just a)) (HITCHHIKERSET _ (Just b)) =
  fromSet conf $ S.intersection as bs
  where
    as = S.unions $ flushDownwards hhSetTF a
    bs = S.unions $ flushDownwards hhSetTF b
