{-# OPTIONS_GHC -w   #-}
module HitchhikerSet ( empty
                     , getConfig
                     , null
                     , rawNode
                     , singleton
                     , fromSet
                     , fromArraySet
                     , toSet
                     , weightEstimate
                     , insert
                     , insertMany
                     , insertRaw
                     , delete
                     , deleteRaw
                     , member
                     , union
                     , intersection
                     , getLeftmostValue
                     , consolidate
                     , hhSetTF
                     , HitchhikerSet.toList
                     ) where

import           ClassyPrelude   hiding (delete, empty, intersection, member,
                                  null, singleton, union)

import           Data.Set        (Set)

import           Data.Vector     ((!))
import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import           Data.Sorted
import           Data.Sorted.Set

import qualified Data.Foldable   as F
import qualified Data.List       as L
import qualified Data.Map        as M
import qualified Data.Set        as S
import qualified Data.Vector     as V

empty :: TreeConfig -> HitchhikerSet k
empty config = HITCHHIKERSET config Nothing

getConfig :: HitchhikerSet k -> TreeConfig
getConfig (HITCHHIKERSET config _) = config

null :: HitchhikerSet k -> Bool
null (HITCHHIKERSET config tree) = not $ isJust tree

rawNode :: HitchhikerSet k -> Maybe (HitchhikerSetNode k)
rawNode (HITCHHIKERSET _ tree) = tree

singleton :: TreeConfig -> k -> HitchhikerSet k
singleton config k
  = HITCHHIKERSET config (Just $ HitchhikerSetNodeLeaf $ ssetSingleton k)

fromSet :: (Show k, Ord k) => TreeConfig -> Set k -> HitchhikerSet k
fromSet config ks
  | S.null ks = empty config
  | otherwise = insertMany (ssetFromList $ S.toList ks) $ empty config

fromArraySet :: (Show k, Ord k) => TreeConfig -> ArraySet k -> HitchhikerSet k
fromArraySet config ks
  | ssetIsEmpty ks = empty config
  | otherwise = insertMany ks $ empty config

toSet :: (Show k, Ord k) => HitchhikerSet k -> Set k
toSet (HITCHHIKERSET config Nothing) = S.empty
toSet (HITCHHIKERSET config (Just root)) = collect root
  where
    collect = \case
      HitchhikerSetNodeIndex (TreeIndex _ nodes) hh ->
        foldl' (<>) (mkSet hh) $ fmap collect nodes
      HitchhikerSetNodeLeaf l -> mkSet l

    mkSet = S.fromList . F.toList

weightEstimate :: (Ord k) => HitchhikerSet k -> Int
weightEstimate (HITCHHIKERSET config Nothing)     = 0
weightEstimate (HITCHHIKERSET config (Just root)) =
  treeWeightEstimate hhSetTF root

depth :: (Ord k) => HitchhikerSet k -> Int
depth (HITCHHIKERSET config Nothing)     = 0
depth (HITCHHIKERSET config (Just root)) = treeDepth hhSetTF root

insert :: (Show k, Ord k) => k -> HitchhikerSet k -> HitchhikerSet k
insert !k !(HITCHHIKERSET config (Just root)) =
  HITCHHIKERSET config $ Just $ insertRaw config k root

insert !k (HITCHHIKERSET config Nothing)
  = HITCHHIKERSET config (Just $ HitchhikerSetNodeLeaf $ ssetSingleton k)

insertRaw :: (Show k, Ord k)
          => TreeConfig -> k -> HitchhikerSetNode k
          -> HitchhikerSetNode k
insertRaw config !k root =
  fixUp config hhSetTF $ insertRec config hhSetTF (ssetSingleton k) root


insertMany :: (Show k, Ord k) => ArraySet k -> HitchhikerSet k -> HitchhikerSet k
insertMany !items !(HITCHHIKERSET config Nothing) =
  HITCHHIKERSET config $ Just $
  fixUp config hhSetTF $
  splitLeafMany hhSetTF (maxLeafItems config) items

insertMany !items !(HITCHHIKERSET config (Just top)) =
  HITCHHIKERSET config $ Just $
  fixUp config hhSetTF $
  insertRec config hhSetTF items top

insertManyRaw :: (Show k, Ord k)
              => TreeConfig
              -> ArraySet k
              -> HitchhikerSetNode k
              -> HitchhikerSetNode k
insertManyRaw config !items top =
  fixUp config hhSetTF $
  insertRec config hhSetTF items top

delete :: (Show k, Ord k)
       => k -> HitchhikerSet k -> HitchhikerSet k
delete _ !(HITCHHIKERSET config Nothing) = HITCHHIKERSET config Nothing
delete !k !(HITCHHIKERSET config (Just root)) =
  HITCHHIKERSET config $ deleteRaw config k root

deleteRaw :: (Show k, Ord k)
          => TreeConfig -> k -> HitchhikerSetNode k
          -> Maybe (HitchhikerSetNode k)
deleteRaw config !k root =
  case deleteRec config hhSetTF k Nothing root of
    HitchhikerSetNodeIndex index hitchhikers
      | Just childNode <- fromSingletonIndex index ->
          if ssetIsEmpty hitchhikers then Just childNode
          else Just $ insertManyRaw config hitchhikers childNode
    HitchhikerSetNodeLeaf items
      | ssetIsEmpty items -> Nothing
    newRootNode -> Just newRootNode

-- -----------------------------------------------------------------------

hhSetTF :: Ord k => TreeFun k v (HitchhikerSetNode k) (ArraySet k) (ArraySet k)
hhSetTF = TreeFun {
  mkNode = HitchhikerSetNodeIndex,
  mkLeaf = HitchhikerSetNodeLeaf,
  caseNode = \case
      HitchhikerSetNodeIndex a b -> Left (a, b)
      HitchhikerSetNodeLeaf l    -> Right l,

  leafInsert = ssetUnion,
  leafMerge = (<>),
  leafLength = ssetSize,
  leafSplitAt = ssetSplitAt,
  leafFirstKey = ssetFindMin,
  leafEmpty = mempty,
  leafDelete = \k mybV s -> case mybV of
      Nothing -> ssetDelete k s
      Just _  -> error "Can't delete specific values in set",

  hhMerge = ssetUnion,
  hhLength = ssetSize,
  hhSplit = splitImpl,
  hhEmpty = mempty,
  hhDelete = \k mybV s -> case mybV of
      Nothing -> ssetDelete k s
      Just _  -> error "Can't delete specific values in set"
  }

splitImpl :: Ord k => k -> ArraySet k -> (ArraySet k, ArraySet k)
splitImpl k s = ssetSpanAntitone (< k) s

-- -----------------------------------------------------------------------

member :: Ord k => k -> HitchhikerSet k -> Bool
member key (HITCHHIKERSET _ Nothing) = False
member key (HITCHHIKERSET _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerSetNodeIndex index hitchhikers ->
        case ssetMember key hitchhikers of
          True  -> True
          False -> lookInNode $ findSubnodeByKey key index
      HitchhikerSetNodeLeaf items -> ssetMember key items

-- -----------------------------------------------------------------------

-- union here is super basic and kinda inefficient. these just union the leaves
-- into Data.Set values and perform intersection and union on those. a real
-- implementation should instead operate on the hitchhiker set tree itself.

union :: (Show k, Ord k)
      => HitchhikerSet k -> HitchhikerSet k -> HitchhikerSet k
union n@(HITCHHIKERSET _ Nothing) _ = n
union _ n@(HITCHHIKERSET _ Nothing) = n
union (HITCHHIKERSET conf (Just a)) (HITCHHIKERSET _ (Just b)) =
  fromArraySet conf $ ssetUnion as bs
  where
    as = foldl' ssetUnion mempty $ getLeafList hhSetTF a
    bs = foldl' ssetUnion mempty $ getLeafList hhSetTF b

-- -----------------------------------------------------------------------

data AdvanceOp
  = AdvanceLeft
  | AdvanceRight
  | AdvanceBoth    -- actually possible!

-- This is also wrong. Those hitchhiker values? Actually need to be checked.
getLeftmostValue :: HitchhikerSetNode k -> k
getLeftmostValue (HitchhikerSetNodeLeaf s)       = ssetFindMin s
getLeftmostValue (HitchhikerSetNodeIndex (TreeIndex _ vals) hh)
  | not $ ssetIsEmpty hh = error "Hitchhikers in set intersection"
  | otherwise = getLeftmostValue $ V.head vals

consolidate :: Ord k => Int -> [ArraySet k] -> [ArraySet k]
consolidate _ [] = []
consolidate _ [x]
  | ssetIsEmpty x = []
  -- TODO: Check size of x is maxSize or smaller.
  | otherwise = [x]
consolidate maxSize (x:y:xs) =
  case compare (ssetSize x) maxSize of
    LT -> consolidate maxSize ((x <> y):xs)
    EQ -> x:(consolidate maxSize (y:xs))
    GT -> let (head, tail) = (ssetSplitAt maxSize x)
          in head:(consolidate maxSize (tail:y:xs))

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
    build :: [ArraySet k] -> Maybe (HitchhikerSetNode k)
    build [] = Nothing
    build s = let vals = V.fromList $ map HitchhikerSetNodeLeaf s
                  keys = V.fromList $ map ssetFindMin (L.tail s)
              in Just $ fixUp conf hhSetTF $ TreeIndex keys vals

    find :: HitchhikerSetNode k
         -> HitchhikerSetNode k
         -> [ArraySet k]
    find (HitchhikerSetNodeLeaf a) (HitchhikerSetNodeLeaf b) =
      [ssetIntersection a b]

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
             -> Maybe ( [ArraySet k]
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
    checkSetAgainst :: ArraySet k -> HitchhikerSetNode k -> [ArraySet k]
    checkSetAgainst a (HitchhikerSetNodeLeaf b) = [ssetIntersection a b]
    checkSetAgainst leaves (HitchhikerSetNodeIndex treeIdx _) =
      scanSet leaves treeIdx
      where
        scanSet set idx = join $ flip L.unfoldr (set, Just idx) $ \case
          (_, Nothing)      -> Nothing
          (leaves, Just remainingIdx@(TreeIndex keys vals))
            | ssetIsEmpty leaves -> Nothing
            | otherwise     ->
                let ((tryLeaves, restLeaves), subNode, restIndex) =
                      case length vals of
                        0 -> error "Invalid"
                        1 -> ((leaves, mempty), vals ! 0, Nothing)
                        _ -> (splitImpl (keys ! 0) leaves,
                              vals ! 0,
                              Just $ TreeIndex (V.tail keys) (V.tail vals))
                    result = checkSetAgainst tryLeaves subNode
                in Just (result, (restLeaves, restIndex))

toList :: (Show k, Ord k) => HitchhikerSet k -> [k]
toList (HITCHHIKERSET _ Nothing) = []
toList (HITCHHIKERSET _ (Just a)) = concat $ map ssetToAscList $ getLeafList hhSetTF a

