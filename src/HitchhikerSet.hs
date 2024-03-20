{-# LANGUAGE Strict     #-}
{-# LANGUAGE StrictData #-}
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

import           ClassyPrelude        hiding (delete, empty, intersection,
                                       member, null, singleton, union)

import           Data.Primitive.Array
import           Data.Set             (Set)
import           Safe                 (tailSafe)

import           Impl.Index
import           Impl.Leaf
import           Impl.Strict
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import           Data.Sorted
import           Data.Sorted.Row
import           Data.Sorted.Set
import           Data.Sorted.Types

import qualified ClassyPrelude        as P

import qualified Data.Foldable        as F
import qualified Data.List            as L
import qualified Data.Map.Strict      as M
import qualified Data.Set             as S

empty :: TreeConfig -> HitchhikerSet k
empty config = HITCHHIKERSET config SNothing

getConfig :: HitchhikerSet k -> TreeConfig
getConfig (HITCHHIKERSET config _) = config

null :: HitchhikerSet k -> Bool
null (HITCHHIKERSET config tree) = not $ sIsJust tree

rawNode :: HitchhikerSet k -> StrictMaybe (HitchhikerSetNode k)
rawNode (HITCHHIKERSET _ tree) = tree

singleton :: TreeConfig -> k -> HitchhikerSet k
singleton config k
  = HITCHHIKERSET config (SJust $ HitchhikerSetNodeLeaf $ ssetSingleton k)

fromSet :: (Show k, Ord k) => TreeConfig -> S.Set k -> HitchhikerSet k
fromSet config ks
  | S.null ks = empty config
  | otherwise = insertMany (S.toList ks) $ empty config

fromArraySet :: (Show k, Ord k) => TreeConfig -> ArraySet k -> HitchhikerSet k
fromArraySet config ks
  | ssetIsEmpty ks = empty config
  | otherwise = insertMany (ssetToAscList ks) $ empty config

-- TODO: The runtime of this seems stupidly heavyweight.
toSet :: (Show k, Ord k) => HitchhikerSet k -> S.Set k
toSet (HITCHHIKERSET config SNothing) = S.empty
toSet (HITCHHIKERSET config (SJust !root)) = collect root
  where
    collect = \case
      HitchhikerSetNodeIndex (TreeIndex _ nodes) (COUNTLIST i hh) ->
        foldl' (<>) (S.fromList hh) $ fmap collect nodes
      HitchhikerSetNodeLeaf l -> mkSet l

    mkSet = S.fromList . F.toList

weightEstimate :: (Ord k) => HitchhikerSet k -> Int
weightEstimate (HITCHHIKERSET config SNothing)     = 0
weightEstimate (HITCHHIKERSET config (SJust !root)) =
  treeWeightEstimate hhSetTF root

depth :: (Ord k) => HitchhikerSet k -> Int
depth (HITCHHIKERSET config SNothing)      = 0
depth (HITCHHIKERSET config (SJust !root)) = treeDepth hhSetTF root

insert :: (Show k, Ord k) => k -> HitchhikerSet k -> HitchhikerSet k
insert !k !(HITCHHIKERSET config (SJust !root)) =
  HITCHHIKERSET config $ SJust $ insertRaw config k root

insert !k (HITCHHIKERSET config SNothing)
  = HITCHHIKERSET config (SJust $ HitchhikerSetNodeLeaf $ ssetSingleton k)

insertRaw :: (Show k, Ord k)
          => TreeConfig -> k -> HitchhikerSetNode k
          -> HitchhikerSetNode k
insertRaw config !k !root =
  fixUp config hhSetTF $ insertRec config hhSetTF (singletonCL k) root

insertMany :: (Show k, Ord k)
           => [k] -> HitchhikerSet k -> HitchhikerSet k
insertMany !items hhset@(HITCHHIKERSET config SNothing)
  | L.null items = hhset
  | otherwise = HITCHHIKERSET config $ SJust $
                fixUp config hhSetTF $
                splitLeafMany hhSetTF (maxLeafItems config) $ ssetFromList items

insertMany !items hhset@(HITCHHIKERSET config (SJust !top))
  | L.null items = hhset
  | otherwise = HITCHHIKERSET config $ SJust $
                fixUp config hhSetTF $
                insertRec config hhSetTF (toCountList items) top

insertManyRaw :: (Show k, Ord k)
              => TreeConfig
              -> [k]
              -> HitchhikerSetNode k
              -> HitchhikerSetNode k
insertManyRaw config !items !top =
  fixUp config hhSetTF $
  insertRec config hhSetTF (toCountList items) top

delete :: (Show k, Ord k)
       => k -> HitchhikerSet k -> HitchhikerSet k
delete _ !(HITCHHIKERSET config SNothing) = HITCHHIKERSET config SNothing
delete !k !(HITCHHIKERSET config (SJust root)) =
  HITCHHIKERSET config $ deleteRaw config k root

deleteRaw :: (Show k, Ord k)
          => TreeConfig -> k -> HitchhikerSetNode k
          -> StrictMaybe (HitchhikerSetNode k)
deleteRaw config !k root =
  case deleteRec config hhSetTF k Nothing root of
    HitchhikerSetNodeIndex index (COUNTLIST i hitchhikers)
      | SJust childNode <- fromSingletonIndex index ->
          if L.null hitchhikers then SJust childNode
          else SJust $ insertManyRaw config hitchhikers childNode
    HitchhikerSetNodeLeaf items
      | ssetIsEmpty items -> SNothing
    newRootNode -> SJust newRootNode

-- -----------------------------------------------------------------------

hhSetTF :: Ord k => TreeFun k v (HitchhikerSetNode k) (CountList k) (ArraySet k)
hhSetTF = TreeFun {
  mkNode = HitchhikerSetNodeIndex,
  mkLeaf = HitchhikerSetNodeLeaf,
  caseNode = \case
      HitchhikerSetNodeIndex a b -> CaseIndex a b
      HitchhikerSetNodeLeaf l    -> CaseLeaf l,

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
  hhLength = countListSize,
  hhWholeSplit = setHHWholeSplit,
  hhEmpty = emptyCL,
  hhDelete = \k mybV (COUNTLIST _ s) -> case mybV of
      Nothing -> let nu = filter (/= k) s
                 in toCountList nu
      Just _  -> error "Can't delete specific values in set"
  }

hhSetLeafInsert :: Ord k => ArraySet k -> CountList k -> ArraySet k
hhSetLeafInsert as (COUNTLIST _ items) =
  let !x = ssetUnion as $ ssetFromList items
  in x

setHHWholeSplit :: Ord k
                => [k] -> CountList k
                -> [CountList k]
setHHWholeSplit = doWholeSplit altk
  where
    altk k x = x < k


-- -----------------------------------------------------------------------

member :: Ord k => k -> HitchhikerSet k -> Bool
member key (HITCHHIKERSET _ SNothing) = False
member key (HITCHHIKERSET _ (SJust top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerSetNodeIndex index (COUNTLIST _ hitchhikers) ->
        case elem key hitchhikers of
          True  -> True
          False -> lookInNode $ findSubnodeByKey key index
      HitchhikerSetNodeLeaf items -> ssetMember key items

-- -----------------------------------------------------------------------

-- union here is super basic and kinda inefficient. these just union the leaves
-- into Data.Set values and perform intersection and union on those. a real
-- implementation should instead operate on the hitchhiker set tree itself.

union :: (Show k, Ord k)
      => HitchhikerSet k -> HitchhikerSet k -> HitchhikerSet k
union n@(HITCHHIKERSET _ SNothing) _ = n
union _ n@(HITCHHIKERSET _ SNothing) = n
union (HITCHHIKERSET conf (SJust a)) (HITCHHIKERSET _ (SJust b)) =
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
  | not $ countListEmpty hh = error "Hitchhikers in set intersection"
  | otherwise = getLeftmostValue $ unsafeHead vals

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
intersection n@(HITCHHIKERSET _ SNothing) _ = n
intersection _ n@(HITCHHIKERSET _ SNothing) = n
intersection (HITCHHIKERSET conf (SJust a)) (HITCHHIKERSET _ (SJust b)) =
  HITCHHIKERSET conf $ build
                     $ consolidate (maxLeafItems conf)
                     $ find (flushDownwards hhSetTF a)
                            (flushDownwards hhSetTF b)
  where
    build :: [ArraySet k] -> StrictMaybe (HitchhikerSetNode k)
    build [] = SNothing
    build s = let vals = P.fromList $ map HitchhikerSetNodeLeaf s
                  keys = P.fromList $ map ssetFindMin (L.tail s)
              in SJust $ fixUp conf hhSetTF $ TreeIndex keys vals

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
          aKeys = [getLeftmostValue $ aVals ! 0] ++ P.toList aIdxKeys
          bKeys = [getLeftmostValue $ bVals ! 0] ++ P.toList bIdxKeys
      in join $ L.unfoldr step (aKeys, P.toList aVals,
                                bKeys, P.toList bVals)
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
                              Just $ TreeIndex (unsafeTail keys) (unsafeTail vals))
                    result = checkSetAgainst tryLeaves subNode
                in Just (result, (restLeaves, restIndex))

toList :: (Show k, Ord k) => HitchhikerSet k -> [k]
toList (HITCHHIKERSET _ SNothing) = []
toList (HITCHHIKERSET _ (SJust a)) =
  concat $ map ssetToAscList $ getLeafList hhSetTF a

takeWhileAntitone :: forall k
                   . (Show k, Ord k)
                  => (k -> Bool)
                  -> HitchhikerSet k
                  -> HitchhikerSet k
takeWhileAntitone fun hsm@(HITCHHIKERSET _ SNothing) = hsm
takeWhileAntitone fun (HITCHHIKERSET config (SJust top)) =
  (HITCHHIKERSET config newTop)
  where
    newTop = case hsmTakeWhile $ flushDownwards hhSetTF top of
      HitchhikerSetNodeLeaf l | ssetIsEmpty l -> SNothing
      x                                       -> SJust x

    hsmTakeWhile = \case
      HitchhikerSetNodeIndex (TreeIndex keys vals) _ ->
        -- The antitone function is run for every value in the index. While
        -- the function holds, we don't need to recur into the subnodes.
        let nuKeys = takeWhile fun keys
            tVals = take (length nuKeys + 1) vals
            lastItem = length tVals - 1
            nuVals = rowPut lastItem (hsmTakeWhile (tVals ! lastItem)) tVals
        in if P.null nuKeys
           then hsmTakeWhile $ vals ! 0
           else HitchhikerSetNodeIndex (TreeIndex nuKeys nuVals) emptyCL

      HitchhikerSetNodeLeaf l ->
        HitchhikerSetNodeLeaf $ ssetTakeWhileAntitone fun l

-- TODO: Doesn't deal with balancing at the ends at all.
dropWhileAntitone :: forall k
                   . (Show k, Ord k)
                  => (k -> Bool)
                  -> HitchhikerSet k
                  -> HitchhikerSet k
dropWhileAntitone fun hsm@(HITCHHIKERSET _ SNothing)     = hsm
dropWhileAntitone fun (HITCHHIKERSET config (SJust top)) =
  HITCHHIKERSET config newTop
  where
    newTop = case hsmDropWhile $ flushDownwards hhSetTF top of
      HitchhikerSetNodeLeaf l | ssetIsEmpty l -> SNothing
      x                                       -> SJust x

    hsmDropWhile = \case
      HitchhikerSetNodeIndex (TreeIndex keys vals) _ ->
        -- The antitone function is run for every value in the index. While
        -- the function holds, we don't need to recur into the subnodes.
        if P.null nuKeys
        then hsmDropWhile $ nuVals ! 0
        else HitchhikerSetNodeIndex (TreeIndex nuKeys nuVals) emptyCL
        where
          nuKeys = dropWhile fun keys
          dropCount = length keys - length nuKeys
          tVals = drop dropCount vals
          nuVals = case dropCount of
            0 -> rowPut 0 (hsmDropWhile (vals ! 0)) vals
            x -> rowPut 0 (hsmDropWhile (tVals ! 0)) tVals

      HitchhikerSetNodeLeaf l ->
        HitchhikerSetNodeLeaf $ ssetDropWhileAntitone fun l

findMin :: forall k
         . (Show k, Ord k)
        => HitchhikerSet k
        -> k
findMin (HITCHHIKERSET _ SNothing) = error "HitchhikerSet.findMin: empty set"
findMin (HITCHHIKERSET _ (SJust top)) = go $ flushDownwards hhSetTF top
  where
    go = \case
      HitchhikerSetNodeIndex (TreeIndex _ vals) _ -> go $ unsafeHead vals
      HitchhikerSetNodeLeaf l -> ssetFindMin l

findMax :: forall k
         . (Show k, Ord k)
        => HitchhikerSet k
        -> k
findMax (HITCHHIKERSET _ SNothing) = error "HitchhikerSet.findMax: empty set"
findMax (HITCHHIKERSET _ (SJust top)) = go $ flushDownwards hhSetTF top
  where
    go = \case
      HitchhikerSetNodeIndex (TreeIndex _ vals) _ ->
        let !wid = sizeofArray vals
        in go $ (vals ! (wid - 1))
      HitchhikerSetNodeLeaf l -> ssetFindMax l

mapMonotonic :: forall k a
              . (Show k, Ord k, Show a, Ord a)
             => (k -> a)
             -> HitchhikerSet k
             -> HitchhikerSet a
mapMonotonic func (HITCHHIKERSET config SNothing) =
  HITCHHIKERSET config SNothing
mapMonotonic func (HITCHHIKERSET config (SJust top)) =
  HITCHHIKERSET config (SJust $ go top)
  where
    go :: HitchhikerSetNode k -> HitchhikerSetNode a
    go = \case
      HitchhikerSetNodeIndex (TreeIndex keys vals) (COUNTLIST i hh) ->
        HitchhikerSetNodeIndex (TreeIndex mkeys mvals) (COUNTLIST i mhh)
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
fromLeafSets config rawSets = HITCHHIKERSET config $ SJust node
  where
    node = fixUp config hhSetTF treeIndex
    treeIndex = indexFromList idxV vals
    idxV = P.fromList $ tailSafe $ map ssetFindMin rawSets
    vals = P.fromList $ map HitchhikerSetNodeLeaf rawSets

difference :: forall k
                . (Show k, Ord k)
               => HitchhikerSet k -> HitchhikerSet k -> HitchhikerSet k
difference l@(HITCHHIKERSET _ SNothing) _ = l
difference l (HITCHHIKERSET _ SNothing)   = l
difference (HITCHHIKERSET config (SJust a)) (HITCHHIKERSET _ (SJust b)) =
  fromLeafSets config $ setlistSetlistDifference as bs
  where
    as = getLeafList hhSetTF a
    bs = getLeafList hhSetTF b

