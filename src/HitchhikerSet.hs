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
                     , HitchhikerSet.takeWhileAntitone
                     , HitchhikerSet.dropWhileAntitone
                     , HitchhikerSet.findMin
                     , HitchhikerSet.findMax
                     , HitchhikerSet.mapMonotonic
                     , HitchhikerSet.difference
                     ) where

import           ClassyPrelude   hiding (delete, empty, intersection, member,
                                  null, singleton, union)

import           Data.Set        (Set)
import           Safe            (tailSafe)

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
  | otherwise = insertMany (S.toList ks) $ empty config

fromArraySet :: (Show k, Ord k) => TreeConfig -> ArraySet k -> HitchhikerSet k
fromArraySet config ks
  | ssetIsEmpty ks = empty config
  | otherwise = insertMany (ssetToAscList ks) $ empty config

-- TODO: The runtime of this seems stupidly heavyweight.
toSet :: (Show k, Ord k) => HitchhikerSet k -> Set k
toSet (HITCHHIKERSET config Nothing) = S.empty
toSet (HITCHHIKERSET config (Just root)) = collect root
  where
    collect = \case
      HitchhikerSetNodeIndex (TreeIndex _ nodes) hh ->
        foldl' (<>) (S.fromList $ snd hh) $ fmap collect nodes
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
  fixUp config hhSetTF $ insertRec config hhSetTF (1, [k]) root

insertMany :: (Show k, Ord k)
           => [k] -> HitchhikerSet k -> HitchhikerSet k
insertMany !items hhset@(HITCHHIKERSET config Nothing)
  | L.null items = hhset
  | otherwise = HITCHHIKERSET config $ Just $
                fixUp config hhSetTF $
                splitLeafMany hhSetTF (maxLeafItems config) $ ssetFromList items

insertMany !items hhset@(HITCHHIKERSET config (Just top))
  | L.null items = hhset
  | otherwise = HITCHHIKERSET config $ Just $
                fixUp config hhSetTF $
                insertRec config hhSetTF (length items, items) top

insertManyRaw :: (Show k, Ord k)
              => TreeConfig
              -> [k]
              -> HitchhikerSetNode k
              -> HitchhikerSetNode k
insertManyRaw config !items top =
  fixUp config hhSetTF $
  insertRec config hhSetTF (length items, items) top

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
          if L.null hitchhikers then Just childNode
          else Just $ insertManyRaw config (snd hitchhikers) childNode
    HitchhikerSetNodeLeaf items
      | ssetIsEmpty items -> Nothing
    newRootNode -> Just newRootNode

-- -----------------------------------------------------------------------

hhSetTF :: Ord k => TreeFun k v (HitchhikerSetNode k) (Int, [k]) (ArraySet k)
hhSetTF = TreeFun {
  mkNode = HitchhikerSetNodeIndex,
  mkLeaf = HitchhikerSetNodeLeaf,
  caseNode = \case
      HitchhikerSetNodeIndex a b -> Left (a, b)
      HitchhikerSetNodeLeaf l    -> Right l,

  leafInsert = hhSetLeafInsert,
  leafMerge = (<>),
  leafLength = ssetSize,
  leafSplitAt = ssetSplitAt,
  leafFirstKey = ssetFindMin,
  leafEmpty = mempty,
  leafDelete = \k mybV s -> case mybV of
      Nothing -> ssetDelete k s
      Just _  -> error "Can't delete specific values in set",

  hhMerge = countListMerge,
  hhLength = fst,
  hhWholeSplit = setHHWholeSplit,
  hhEmpty = (0, []),
  hhDelete = \k mybV (_, s) -> case mybV of
      Nothing -> let nu = filter (/= k) s
                 in (length nu, nu)
      Just _  -> error "Can't delete specific values in set"
  }

hhSetLeafInsert :: Ord k => ArraySet k -> (Int, [k]) -> ArraySet k
hhSetLeafInsert as (_, items) = ssetUnion as $ ssetFromList items

setHHWholeSplit :: Ord k
                => [k] -> (Int, [k])
                -> [(Int, [k])]
setHHWholeSplit = doWholeSplit altk
  where
    altk k x = x < k


-- -----------------------------------------------------------------------

member :: Ord k => k -> HitchhikerSet k -> Bool
member key (HITCHHIKERSET _ Nothing) = False
member key (HITCHHIKERSET _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerSetNodeIndex index hitchhikers ->
        case elem key (snd hitchhikers) of
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
  | not $ L.null hh = error "Hitchhikers in set intersection"
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
                        _ -> (ssetSpanAntitone (< (keys ! 0)) leaves,
                              vals ! 0,
                              Just $ TreeIndex (V.tail keys) (V.tail vals))
                    result = checkSetAgainst tryLeaves subNode
                in Just (result, (restLeaves, restIndex))

toList :: (Show k, Ord k) => HitchhikerSet k -> [k]
toList (HITCHHIKERSET _ Nothing) = []
toList (HITCHHIKERSET _ (Just a)) = concat $ map ssetToAscList $ getLeafList hhSetTF a

takeWhileAntitone :: forall k
                   . (Show k, Ord k)
                  => (k -> Bool)
                  -> HitchhikerSet k
                  -> HitchhikerSet k
takeWhileAntitone fun hsm@(HITCHHIKERSET _ Nothing) = hsm
takeWhileAntitone fun (HITCHHIKERSET config (Just top)) =
  (HITCHHIKERSET config newTop)
  where
    newTop = case hsmTakeWhile $ flushDownwards hhSetTF top of
      HitchhikerSetNodeLeaf l | ssetIsEmpty l -> Nothing
      x                                       -> Just x

    hsmTakeWhile = \case
      HitchhikerSetNodeIndex (TreeIndex keys vals) _ ->
        -- The antitone function is run for every value in the index. While
        -- the function holds, we don't need to recur into the subnodes.
        let nuKeys = takeWhile fun keys
            tVals = take (length nuKeys + 1) vals
            lastItem = length tVals - 1
            nuVals = tVals V.//
                     [(lastItem, hsmTakeWhile (tVals V.! lastItem))]
        in if V.null nuKeys
           then hsmTakeWhile $ vals V.! 0
           else HitchhikerSetNodeIndex (TreeIndex nuKeys nuVals) (0, [])

      HitchhikerSetNodeLeaf l ->
        HitchhikerSetNodeLeaf $ ssetTakeWhileAntitone fun l

-- TODO: Doesn't deal with balancing at the ends at all.
dropWhileAntitone :: forall k
                   . (Show k, Ord k)
                  => (k -> Bool)
                  -> HitchhikerSet k
                  -> HitchhikerSet k
dropWhileAntitone fun hsm@(HITCHHIKERSET _ Nothing)     = hsm
dropWhileAntitone fun (HITCHHIKERSET config (Just top)) =
  HITCHHIKERSET config newTop
  where
    newTop = case hsmDropWhile $ flushDownwards hhSetTF top of
      HitchhikerSetNodeLeaf l | ssetIsEmpty l -> Nothing
      x                                       -> Just x

    hsmDropWhile = \case
      HitchhikerSetNodeIndex (TreeIndex keys vals) _ ->
        -- The antitone function is run for every value in the index. While
        -- the function holds, we don't need to recur into the subnodes.
        if V.null nuKeys
        then hsmDropWhile $ nuVals V.! 0
        else HitchhikerSetNodeIndex (TreeIndex nuKeys nuVals) (0, [])
        where
          nuKeys = dropWhile fun keys
          dropCount = length keys - length nuKeys
          tVals = drop dropCount vals
          nuVals = case dropCount of
            0 -> vals V.// [(0, hsmDropWhile (vals V.! 0))]
            x -> tVals V.// [(0, hsmDropWhile (tVals V.! 0))]

      HitchhikerSetNodeLeaf l ->
        HitchhikerSetNodeLeaf $ ssetDropWhileAntitone fun l

findMin :: forall k
         . (Show k, Ord k)
        => HitchhikerSet k
        -> k
findMin (HITCHHIKERSET _ Nothing) = error "HitchhikerSet.findMin: empty set"
findMin (HITCHHIKERSET _ (Just top)) = go $ flushDownwards hhSetTF top
  where
    go = \case
      HitchhikerSetNodeIndex (TreeIndex _ vals) _ -> go $ V.head vals
      HitchhikerSetNodeLeaf l -> ssetFindMin l

findMax :: forall k
         . (Show k, Ord k)
        => HitchhikerSet k
        -> k
findMax (HITCHHIKERSET _ Nothing) = error "HitchhikerSet.findMax: empty set"
findMax (HITCHHIKERSET _ (Just top)) = go $ flushDownwards hhSetTF top
  where
    go = \case
      HitchhikerSetNodeIndex (TreeIndex _ vals) _ -> go $ V.last vals
      HitchhikerSetNodeLeaf l -> ssetFindMax l

mapMonotonic :: forall k a
              . (Show k, Ord k, Show a, Ord a)
             => (k -> a)
             -> HitchhikerSet k
             -> HitchhikerSet a
mapMonotonic func (HITCHHIKERSET config Nothing) =
  HITCHHIKERSET config Nothing
mapMonotonic func (HITCHHIKERSET config (Just top)) =
  HITCHHIKERSET config (Just $ go top)
  where
    go :: HitchhikerSetNode k -> HitchhikerSetNode a
    go = \case
      HitchhikerSetNodeIndex (TreeIndex keys vals) (i, hh) ->
        HitchhikerSetNodeIndex (TreeIndex mkeys mvals) (i, mhh)
        where
          mkeys = map func keys
          mvals = map go vals
          mhh = map func hh
      HitchhikerSetNodeLeaf l ->
        -- TODO: Make a ssetMapMonotonic instead of just ssetMap.
        HitchhikerSetNodeLeaf $ ssetMap func l


fromLeafSets :: (Show k, Ord k)
            => TreeConfig
            -> [ArraySet k]
            -> HitchhikerSet k
fromLeafSets config [] = HitchhikerSet.empty config
fromLeafSets config [m] = fromArraySet config m
fromLeafSets config rawSets = HITCHHIKERSET config $ Just node
  where
    node = fixUp config hhSetTF treeIndex
    treeIndex = indexFromList idxV vals
    idxV = V.fromList $ tailSafe $ map ssetFindMin rawSets
    vals = V.fromList $ map HitchhikerSetNodeLeaf rawSets

difference :: forall k
                . (Show k, Ord k)
               => HitchhikerSet k -> HitchhikerSet k -> HitchhikerSet k
difference l@(HITCHHIKERSET _ Nothing) _ = l
difference l (HITCHHIKERSET _ Nothing)   = l
difference (HITCHHIKERSET config (Just a)) (HITCHHIKERSET _ (Just b)) =
  fromLeafSets config $ setlistSetlistDifference as bs
  where
    as = getLeafList hhSetTF a
    bs = getLeafList hhSetTF b

