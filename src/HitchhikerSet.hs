module HitchhikerSet ( empty
                     , null
                     , singleton
                     , fromSet
                     , toSet
                     , insert
                     , insertMany
                     , delete
                     , member
                     , union
                     , intersection
                     ) where

import           ClassyPrelude hiding (delete, empty, intersection, member,
                                null, singleton, union)

import           Data.Set      (Set)

import           Data.Vector   ((!))
import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import qualified Data.Foldable as F
import qualified Data.List     as L
import qualified Data.Map      as M
import qualified Data.Set      as S
import qualified Data.Vector   as V

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


delete :: (Show k, Ord k)
       => k -> HitchhikerSet k -> HitchhikerSet k
delete _ !(HITCHHIKERSET config Nothing) = HITCHHIKERSET config Nothing
delete !k !(HITCHHIKERSET config (Just root)) =
  case deleteRec config hhSetTF k Nothing root of
    HitchhikerSetNodeIndex index hitchhikers
      | Just childNode <- fromSingletonIndex index ->
          if S.null hitchhikers then HITCHHIKERSET config (Just childNode)
          else insertMany hitchhikers $ HITCHHIKERSET config (Just childNode)
    HitchhikerSetNodeLeaf items
      | S.null items -> HITCHHIKERSET config Nothing
    newRootNode -> HITCHHIKERSET config (Just newRootNode)

-- -----------------------------------------------------------------------

hhSetTF :: Ord k => TreeFun k v (HitchhikerSetNode k) (Set k) (Set k)
hhSetTF = TreeFun {
  mkNode = HitchhikerSetNodeIndex,
  mkLeaf = HitchhikerSetNodeLeaf,
  caseNode = \case
      HitchhikerSetNodeIndex a b -> Left (a, b)
      HitchhikerSetNodeLeaf l    -> Right l,

  leafInsert = S.union,
  leafMerge = (<>),
  leafLength = S.size,
  leafSplitAt = S.splitAt,
  leafFirstKey = S.findMin,
  leafEmpty = S.empty,
  leafDelete = \k mybV s -> case mybV of
      Nothing -> S.delete k s
      Just _  -> error "Can't delete specific values in set",

  hhMerge = S.union,
  hhLength = S.size,
  hhSplit = splitImpl,
  hhEmpty = S.empty,
  hhDelete = \k mybV s -> case mybV of
      Nothing -> S.delete k s
      Just _  -> error "Can't delete specific values in set"
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

-- union here is super basic and kinda inefficient. these just union the leaves
-- into Data.Set values and perform intersection and union on those. a real
-- implementation should instead operate on the hitchhiker set tree itself.

union :: (Show k, Ord k)
      => HitchhikerSet k -> HitchhikerSet k -> HitchhikerSet k
union n@(HITCHHIKERSET _ Nothing) _ = n
union _ n@(HITCHHIKERSET _ Nothing) = n
union (HITCHHIKERSET conf (Just a)) (HITCHHIKERSET _ (Just b)) =
  fromSet conf $ S.union as bs
  where
    as = S.unions $ getLeafList hhSetTF a
    bs = S.unions $ getLeafList hhSetTF b

-- -----------------------------------------------------------------------

data AdvanceOp
  = AdvanceLeft
  | AdvanceRight
  | AdvanceBoth    -- actually possible!

-- This is also wrong. Those hitchhiker values? Actually need to be checked.
getLeftmostValue :: HitchhikerSetNode k -> k
getLeftmostValue (HitchhikerSetNodeLeaf s)       = S.findMin s
getLeftmostValue (HitchhikerSetNodeIndex (TreeIndex _ vals) hh)
  | not $ S.null hh = error "Hitchhikers in set intersection"
  | otherwise = getLeftmostValue $ V.head vals

intersection :: forall k
                . (Show k, Ord k)
               => HitchhikerSet k -> HitchhikerSet k -> HitchhikerSet k
intersection n@(HITCHHIKERSET _ Nothing) _ = n
intersection _ n@(HITCHHIKERSET _ Nothing) = n
intersection (HITCHHIKERSET conf (Just a)) (HITCHHIKERSET _ (Just b)) =
  HITCHHIKERSET conf $ build
                     $ consolidate (maxLeafItems conf)
                     $ find (flushDownwards hhSetTF a)
                            (flushDownwards hhSetTF b)
  where
    build :: [Set k] -> Maybe (HitchhikerSetNode k)
    build [] = Nothing
    build s = let vals = V.fromList $ map HitchhikerSetNodeLeaf s
                  keys = V.fromList $ map S.findMin (L.tail s)
              in Just $ fixUp conf hhSetTF $ TreeIndex keys vals

    consolidate :: Int -> [Set k] -> [Set k]
    consolidate _ [] = []
    consolidate _ [x]
      | S.null x = []
      -- TODO: Check size of x is maxSize or smaller.
      | otherwise = [x]
    consolidate maxSize (x:y:xs) =
      case compare (S.size x) maxSize of
        LT -> consolidate maxSize ((x <> y):xs)
        EQ -> x:(consolidate maxSize (y:xs))
        GT -> let (head, tail) = (S.splitAt maxSize x)
              in head:(consolidate maxSize (tail:y:xs))

    find :: HitchhikerSetNode k
         -> HitchhikerSetNode k
         -> [Set k]
    find (HitchhikerSetNodeLeaf a) (HitchhikerSetNodeLeaf b) =
      [S.intersection a b]

    find (HitchhikerSetNodeLeaf leaves) sn@(HitchhikerSetNodeIndex _ _) =
      checkSetAgainst leaves sn

    find sn@(HitchhikerSetNodeIndex _ _) (HitchhikerSetNodeLeaf leaves) =
      checkSetAgainst leaves sn

    find (HitchhikerSetNodeIndex treeA _) (HitchhikerSetNodeIndex treeB _) =
      let (TreeIndex aIdxKeys aVals) = treeA
          (TreeIndex bIdxKeys bVals) = treeB
          aKeys = [getLeftmostValue $ aVals ! 0] ++ V.toList aIdxKeys
          bKeys = [getLeftmostValue $ bVals ! 0] ++ V.toList bIdxKeys
      in join $ L.unfoldr step (aKeys, V.toList aVals,
                                bKeys, V.toList bVals)
      where
        step :: ([k], [HitchhikerSetNode k], [k], [HitchhikerSetNode k])
             -> Maybe ( [Set k]
                      , ([k], [HitchhikerSetNode k],
                         [k], [HitchhikerSetNode k]))
        step ([], [],  _,  _)           = Nothing
        step (_,   _, [], [])           = Nothing

        -- Both range to the end.
        step ([amin], [a], [bmin], [b]) = Just (find a b, ([], [], [], []))

        -- Right range is at the end.
        step ((amin:amax:arest), (a:as), [bmin], [b])
          | bmin <= amax = Just (find a b, (amax:arest, as, [bmin], [b]))
          | otherwise    = Just ([],       (amax:arest, as, [bmin], [b]))

        -- Left range is at the end.
        step ([amin], [a], (bmin:bmax:brest), (b:bs))
          | amin <= bmax = Just (find a b, ([amin], [a], bmax:brest, bs))
          | otherwise    = Just ([],       ([amin], [a], bmax:brest, bs))

        -- Both ranges.
        step ((amin:amax:arest), (a:as), (bmin:bmax:brest), (b:bs)) =
          let (overlaps, advanceOp) = checkOverlap amin amax bmin bmax
              stepResult = if overlaps then find a b else []
              advances = case advanceOp of
                AdvanceLeft  -> (amax:arest, as, bmin:bmax:brest, b:bs)
                AdvanceRight -> (amin:amax:arest, a:as, bmax:brest, bs)
                AdvanceBoth  -> (amax:arest, as, bmax:brest, bs)
          in Just $ (stepResult, advances)

        checkOverlap :: k -> k -> k -> k -> (Bool, AdvanceOp)
        checkOverlap aMin aMax bMin bMax = (overlap, advance)
          where
            overlap = aMin < bMax && bMin < aMax
            advance = if | aMax == bMax -> AdvanceBoth
                         | aMax > bMax  -> AdvanceRight
                         | otherwise    -> AdvanceLeft

    -- One side of the operation is a set, no need to keep track of ranges
    -- anymore.
    checkSetAgainst :: Set k -> HitchhikerSetNode k -> [Set k]
    checkSetAgainst a (HitchhikerSetNodeLeaf b) = [S.intersection a b]
    checkSetAgainst leaves (HitchhikerSetNodeIndex treeIdx _) =
      scanSet leaves treeIdx
      where
        scanSet set idx = join $ flip L.unfoldr (set, Just idx) $ \case
          (_, Nothing)      -> Nothing
          (leaves, Just remainingIdx@(TreeIndex keys vals))
            | S.null leaves -> Nothing
            | otherwise     ->
                let ((tryLeaves, restLeaves), subNode, restIndex) =
                      case length vals of
                        0 -> error "Invalid"
                        1 -> ((leaves, S.empty), vals ! 0, Nothing)
                        _ -> (splitImpl (keys ! 0) leaves,
                              vals ! 0,
                              Just $ TreeIndex (V.tail keys) (V.tail vals))
                    result = checkSetAgainst tryLeaves subNode
                in Just (result, (restLeaves, restIndex))
