module HitchhikerSetMap where

import           ClassyPrelude

import           Data.Map.Strict   (Map)
import           Data.Set          (Set)

import           Safe              (tailSafe)

import           Data.Sorted.Row
import           Data.Sorted.Types

import           Impl.Index
import           Impl.Leaf
import           Impl.Strict
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import           Data.Sorted

import qualified ClassyPrelude     as P

import qualified HitchhikerSet     as HS

import qualified Data.Map.Strict   as M
import qualified Data.Set          as S

empty :: TreeConfig -> HitchhikerSetMap k v
empty config = HITCHHIKERSETMAP config Nothing

-- When doing mappings over hitchhiker map sets, we sometimes want to switch
-- the representation over to actual sets so that we can map the entire key
-- value.
toHitchhikerMap :: (Show k, Show v, Ord k, Ord v)
                => HitchhikerSetMap k v -> HitchhikerMap k (HitchhikerSet v)
toHitchhikerMap (HITCHHIKERSETMAP config Nothing) =
  (HITCHHIKERMAP config Nothing)

toHitchhikerMap (HITCHHIKERSETMAP config (Just top)) =
  (HITCHHIKERMAP config newTop)
  where
    newTop = Just $ translate $ flushDownwards (hhSetMapTF config) top

    translate = \case
      HitchhikerSetMapNodeIndex idx _ ->
        HitchhikerMapNodeIndex (mapIndex translate idx) M.empty
      HitchhikerSetMapNodeLeaf l ->
        HitchhikerMapNodeLeaf (map toHHSet l)

    toHHSet (NAKEDSET n) = HITCHHIKERSET config n


-- Takes a list of leaves and make a valid
fromLeafMaps :: (Show k, Show v, Ord k, Ord v)
             => TreeConfig
             -> [Map k (NakedHitchhikerSet v)]
             -> HitchhikerSetMap k v
fromLeafMaps config []   = HitchhikerSetMap.empty config
fromLeafMaps config [m]
  | M.null m = HitchhikerSetMap.empty config
  | otherwise = HITCHHIKERSETMAP config $ Just
              $ HitchhikerSetMapNodeLeaf m
fromLeafMaps config rawMaps = HITCHHIKERSETMAP config $ Just node
  where
    node = fixUp config (hhSetMapTF config) treeIndex
    treeIndex = indexFromList idxV vals
    idxV = P.fromList $ tailSafe $ map (fst . M.findMin) rawMaps
    vals = P.fromList $ map HitchhikerSetMapNodeLeaf
                      $ filter (not . M.null) rawMaps

-- SetMap is the complicated one, because we use two different representations:
-- one for the leafs (where we use HitchhikerSets to collect the mass keys) and
-- one for hitchhikers (which are normal sets for quick insertion).
--
hhSetMapTF
  :: (Show k, Show v, Ord k, Ord v)
  => TreeConfig
  -> TreeFun k
             v
             (HitchhikerSetMapNode k v)
             (Map k (ArraySet v))
             (Map k (NakedHitchhikerSet v))
hhSetMapTF setConfig = TreeFun {
  mkNode = HitchhikerSetMapNodeIndex,
  mkLeaf = HitchhikerSetMapNodeLeaf,
  caseNode = \case
      HitchhikerSetMapNodeIndex a b -> CaseIndex a b
      HitchhikerSetMapNodeLeaf l    -> CaseLeaf l,

  leafInsert = leafInsertImpl setConfig,
  leafMerge = leafMergeImpl setConfig {- M.unionWith HS.union -},
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = \k mybV sm -> case mybV of
      Just v  -> M.update (\s -> Just $ strip $ HS.delete v (weave setConfig s)) k sm
      Nothing -> error "Impossible leaf delete in setmap",

  hhMerge = M.unionWith ssetUnion,
  -- For the hitchhikers, we count the number of all items in a set instead of
  -- the number of keys.
  hhLength = sum . map ssetSize . M.elems,
  hhWholeSplit = hhDefaultWholeSplit splitImpl,
  hhEmpty = M.empty,
  hhDelete = \k mybV sm -> case mybV of
      Just v  -> M.update (Just . ssetDelete v) k sm
      Nothing -> error "Impossible leaf delete in setmap"
  }

strip :: HitchhikerSet v -> NakedHitchhikerSet v
strip (HITCHHIKERSET _ x) = NAKEDSET x

weave :: TreeConfig -> NakedHitchhikerSet v -> HitchhikerSet v
weave tc (NAKEDSET x) = (HITCHHIKERSET tc x)

leafInsertImpl :: forall k v
                . (Show k, Show v, Ord k, Ord v)
               => TreeConfig
               -> Map k (NakedHitchhikerSet v)
               -> Map k (ArraySet v)
               -> Map k (NakedHitchhikerSet v)
leafInsertImpl config leaf hh = foldl' merge leaf (M.toList hh)
  where
    merge items (k, vSet) = M.alter (alt vSet) k items

    alt :: ArraySet v
        -> Maybe (NakedHitchhikerSet v)
        -> Maybe (NakedHitchhikerSet v)
    alt new Nothing    = Just (strip $ HS.fromArraySet config new)
    alt new (Just old) =
      -- TODO: Change the type here to use the new representation.
      Just (strip $ HS.insertMany (ssetToAscList new) (weave config old))

leafMergeImpl :: forall k v
               . (Show k, Show v, Ord k, Ord v)
              => TreeConfig
              -> Map k (NakedHitchhikerSet v)
              -> Map k (NakedHitchhikerSet v)
              -> Map k (NakedHitchhikerSet v)
leafMergeImpl config = M.unionWith doUnion
  where
    doUnion :: NakedHitchhikerSet v
            -> NakedHitchhikerSet v
            -> NakedHitchhikerSet v
    doUnion a b = strip $ HS.union (weave config a) (weave config b)

splitImpl :: Ord k => k -> Map k v -> (Map k v, Map k v)
splitImpl k m = M.spanAntitone (< k) m

insert :: (Show k, Show v, Ord k, Ord v)
       => k -> v -> HitchhikerSetMap k v -> HitchhikerSetMap k v
insert !k !v !(HITCHHIKERSETMAP config (Just root)) =
    HITCHHIKERSETMAP config $ Just $ insertRaw config k v root

insert !k !v (HITCHHIKERSETMAP config Nothing)
  = HITCHHIKERSETMAP config $ Just $ HitchhikerSetMapNodeLeaf $
                       M.singleton k (strip $ HS.singleton config v)

insertRaw :: (Show k, Show v, Ord k, Ord v)
          => TreeConfig -> k -> v
          -> HitchhikerSetMapNode k v
          -> HitchhikerSetMapNode k v
insertRaw config !k !v root =
    fixUp config (hhSetMapTF config) $
    insertRec config (hhSetMapTF config) (M.singleton k (ssetSingleton v)) root

insertMany :: (Show k, Show v, Ord k, Ord v)
           => [(k, v)] -> HitchhikerSetMap k v -> HitchhikerSetMap k v
insertMany !items !(HITCHHIKERSETMAP config Nothing)
  = HITCHHIKERSETMAP config $ Just $ fixUp config (hhSetMapTF config) $
    splitLeafMany (hhSetMapTF config)
                  (maxLeafItems config)
                  (listToLeaves config items)

insertMany !items !(HITCHHIKERSETMAP config (Just root))
  = HITCHHIKERSETMAP config $ Just $
    fixUp config (hhSetMapTF config) $
    insertRec config (hhSetMapTF config) (listToHitchhikers items) root

delete :: (Show k, Ord k, Show v, Ord v)
       => k -> v -> HitchhikerSetMap k v -> HitchhikerSetMap k v
delete _ _ !(HITCHHIKERSETMAP config Nothing) = HITCHHIKERSETMAP config Nothing
delete !k !v !(HITCHHIKERSETMAP config (Just root)) =
  case deleteRec config (hhSetMapTF config) k (Just v) root of
    HitchhikerSetMapNodeIndex index hitchhikers
      | SJust childNode <- fromSingletonIndex index ->
          if M.null hitchhikers then HITCHHIKERSETMAP config (Just childNode)
          else insertMany (mapSetToList hitchhikers) $
               HITCHHIKERSETMAP config (Just childNode)
    HitchhikerSetMapNodeLeaf items
      | M.null items -> HITCHHIKERSETMAP config Nothing
    newRootNode -> HITCHHIKERSETMAP config (Just newRootNode)

listToLeaves :: (Show v, Show k, Ord k, Ord v)
             => TreeConfig -> [(k, v)] -> Map k (NakedHitchhikerSet v)
listToLeaves config = go M.empty
  where
    go m []         = m
    go m ((k,v):xs) = go (M.alter (alt v) k m) xs

    alt v Nothing  = Just (strip $ HS.singleton config v)
    alt v (Just s) = Just (strip $ HS.insert v (weave config s))

listToHitchhikers :: (Show v, Show k, Ord k, Ord v)
                  => [(k, v)] -> Map k (ArraySet v)
listToHitchhikers = go M.empty
  where
    go m []         = m
    go m ((k,v):xs) = go (M.alter (alt v) k m) xs

    alt v Nothing  = Just (ssetSingleton v)
    alt v (Just s) = Just (ssetInsert v s)

-- For SetMap lookup, we must always resolve our hitchhikers downwards to a
-- leaf for combination into a set. We can't just stop halfway.
--
-- Unlike Map lookup and Set member, we can't just stop when we find a
-- hitchhiker. We must take the hitchhikers which match and carry them
-- down to the leaves.
lookup :: forall k v
        . (Show k, Show v, Ord k, Ord v)
       => k
       -> HitchhikerSetMap k v
       -> HitchhikerSet v
lookup key (HITCHHIKERSETMAP config Nothing)    = HS.empty config
lookup key (HITCHHIKERSETMAP config (Just top)) = lookInNode mempty top
  where
    lookInNode :: ArraySet v -> HitchhikerSetMapNode k v -> HitchhikerSet v
    lookInNode hh b =
      case b of
        HitchhikerSetMapNodeIndex index hitchhikers ->
          lookInNode (hh <> matchHitchhikers hitchhikers) $
            findSubnodeByKey key index
        HitchhikerSetMapNodeLeaf items ->
          buildSetFrom items hh

    matchHitchhikers :: Map k (ArraySet v) -> ArraySet v
    matchHitchhikers hh = fromMaybe mempty (M.lookup key hh)

    -- We have a list of hitchhikers and the leaf values. Turn that into a
    -- final list.
    buildSetFrom leaves hh = case M.lookup key leaves of
      Nothing  -> HS.fromArraySet config hh
      -- TODO: Use the new representation.
      Just ret -> HS.insertMany (ssetToAscList hh) (weave config ret)

-- TODO: This isn't the best performing implementation, but it's Good Enough
-- for now. This is an attempt at using something like the
-- MultiIntesersectV3Naive code in a different context. This should be good
-- enough at "small" (<100K) items, and will probably be faster on the naive
-- tree walking interpreter than a proper tree pruning implementation for the
-- same reason V6Stack was: function frames are super expensive.
--
restrictKeys :: forall k v
              . (Show k, Show v, Ord k, Ord v)
             => HitchhikerSet k
             -> HitchhikerSetMap k v
             -> HitchhikerSetMap k v
restrictKeys _ orig@(HITCHHIKERSETMAP _ Nothing) = orig

restrictKeys (HITCHHIKERSET _ SNothing) (HITCHHIKERSETMAP config _) =
  HitchhikerSetMap.empty config

restrictKeys s@(HITCHHIKERSET sConfig (SJust a))
             sm@(HITCHHIKERSETMAP mConfig (Just b)) =
  fromLeafMaps mConfig $ setlistMaplistIntersect [] as bs
  where
    as = getLeafList HS.hhSetTF a
    bs = getLeafList (hhSetMapTF mConfig) b

-- Given a predicate function which takes the key and set of values, restricts
-- all keys to
restrictKeysWithPred :: forall k v
                      . (Show k, Show v, Ord k, Ord v)
                     => (k -> HitchhikerSet v -> Maybe (HitchhikerSet v))
                     -> HitchhikerSet k
                     -> HitchhikerSetMap k v
                     -> HitchhikerSetMap k v
restrictKeysWithPred _ _ orig@(HITCHHIKERSETMAP _ Nothing) = orig

restrictKeysWithPred _ (HITCHHIKERSET _ SNothing) (HITCHHIKERSETMAP config _) =
  HitchhikerSetMap.empty config

restrictKeysWithPred func (HITCHHIKERSET sConfig (SJust a))
                          (HITCHHIKERSETMAP mConfig (Just b)) =
  fromLeafMaps mConfig $ setlistMaplistIntersectWithPred convertFunc [] as bs
  where
    as = getLeafList HS.hhSetTF a
    bs = getLeafList (hhSetMapTF mConfig) b

    convertFunc k (NAKEDSET n) = strip <$> func k (HITCHHIKERSET sConfig n)

toKeySet :: forall k v
          . (Show k, Show v, Ord k, Ord v)
         => HitchhikerSetMap k v
         -> HitchhikerSet k
toKeySet (HITCHHIKERSETMAP config Nothing) = (HITCHHIKERSET config SNothing)
toKeySet (HITCHHIKERSETMAP config (Just top)) =
    (HITCHHIKERSET config newTop)
  where
    newTop = SJust $ translate top

    -- TODO: ssetFromList $ M.keys -> tabKeysSet.
    translate = \case
      HitchhikerSetMapNodeIndex idx hh ->
        let ks = M.keys hh
        in HitchhikerSetNodeIndex (mapIndex translate idx) (toCountList ks)
      HitchhikerSetMapNodeLeaf l ->
        HitchhikerSetNodeLeaf $ ssetFromDistinctAscList $ M.keys l

takeWhileAntitone :: forall k v
                   . (Show k, Show v, Ord k, Ord v)
                  => (k -> Bool)
                  -> HitchhikerSetMap k v
                  -> HitchhikerSetMap k v
takeWhileAntitone fun hsm@(HITCHHIKERSETMAP _ Nothing) = hsm
takeWhileAntitone fun (HITCHHIKERSETMAP config (Just top)) =
  (HITCHHIKERSETMAP config newTop)
  where
    newTop = case hsmTakeWhile $ flushDownwards (hhSetMapTF config) top of
      HitchhikerSetMapNodeLeaf l | M.null l -> Nothing
      x                                     -> Just x

    hsmTakeWhile = \case
      HitchhikerSetMapNodeIndex (TreeIndex keys vals) _ ->
        -- The antitone function is run for every value in the index. While
        -- the function holds, we don't need to recur into the subnodes.
        let nuKeys = takeWhile fun keys
            tVals = take (length nuKeys + 1) vals
            lastItem = length tVals - 1
            nuVals = rowPut lastItem (hsmTakeWhile (tVals ! lastItem)) tVals
        in if P.null nuKeys
           then hsmTakeWhile $ vals ! 0
           else HitchhikerSetMapNodeIndex (TreeIndex nuKeys nuVals) mempty

      HitchhikerSetMapNodeLeaf l ->
        HitchhikerSetMapNodeLeaf $ M.takeWhileAntitone fun l

-- TODO: Doesn't deal with balancing at the ends at all.
dropWhileAntitone :: forall k v
                   . (Show k, Show v, Ord k, Ord v)
                  => (k -> Bool)
                  -> HitchhikerSetMap k v
                  -> HitchhikerSetMap k v
dropWhileAntitone fun hsm@(HITCHHIKERSETMAP _ Nothing)     = hsm
dropWhileAntitone fun (HITCHHIKERSETMAP config (Just top)) =
  HITCHHIKERSETMAP config newTop
  where
    newTop = case hsmDropWhile $ flushDownwards (hhSetMapTF config) top of
      HitchhikerSetMapNodeLeaf l | M.null l -> Nothing
      x                                     -> Just x

    hsmDropWhile = \case
      HitchhikerSetMapNodeIndex (TreeIndex keys vals) _ ->
        -- The antitone function is run for every value in the index. While
        -- the function holds, we don't need to recur into the subnodes.
        if P.null nuKeys
        then hsmDropWhile $ nuVals ! 0
        else HitchhikerSetMapNodeIndex (TreeIndex nuKeys nuVals) mempty
        where
          nuKeys = dropWhile fun keys
          dropCount = length keys - length nuKeys
          tVals = drop dropCount vals
          nuVals = case dropCount of
            0 -> rowPut 0 (hsmDropWhile (vals ! 0)) vals
            x -> rowPut 0 (hsmDropWhile (tVals ! 0)) tVals

      HitchhikerSetMapNodeLeaf l ->
        HitchhikerSetMapNodeLeaf $ M.dropWhileAntitone fun l

toList :: forall k v
        . (Show k, Show v, Ord k, Ord v)
       => HitchhikerSetMap k v
       -> [(k, v)]
toList (HITCHHIKERSETMAP _ Nothing) = []
toList (HITCHHIKERSETMAP config (Just top)) =
  go $ flushDownwards (hhSetMapTF config) top
  where
    go = \case
      HitchhikerSetMapNodeIndex (TreeIndex keys vals) _ ->
        concat $ map go vals
      HitchhikerSetMapNodeLeaf l -> concat $ map mkItems $ M.toList l

    mkItems :: (k, NakedHitchhikerSet v) -> [(k, v)]
    mkItems (k, hhs) = map (k,) $ HS.toList $ weave config hhs

findMinKey :: forall k v
            . (Show k, Show v, Ord k, Ord v)
           => HitchhikerSetMap k v
           -> k
findMinKey (HITCHHIKERSETMAP _ Nothing) =
  error "HitchhikerSetMap.findMinKey: emptySet"
findMinKey (HITCHHIKERSETMAP _ (Just top)) = go Nothing top
  where
    go curMin = \case
      HitchhikerSetMapNodeIndex (TreeIndex _ vals) hh ->
        go (check curMin hh) $ unsafeHead vals
      HitchhikerSetMapNodeLeaf l -> case check curMin l of
        Nothing -> error "HitchhikerSetMap.findMinKey: empty leaf"
        Just x  -> x

    check Nothing hh
      | M.null hh = Nothing
      | otherwise = Just $ fst $ M.findMin hh
    check j@(Just x) hh
      | M.null hh = j
      | otherwise = Just $ min x $ fst $ M.findMin hh

findMaxKey :: forall k v
            . (Show k, Show v, Ord k, Ord v)
           => HitchhikerSetMap k v
           -> k
findMaxKey (HITCHHIKERSETMAP _ Nothing) =
  error "HitchhikerSetMap.findMaxKey: emptySet"
findMaxKey (HITCHHIKERSETMAP _ (Just top)) = go Nothing top
  where
    go curMax = \case
      HitchhikerSetMapNodeIndex (TreeIndex _ vals) hh ->
        go (check curMax hh) $ unsafeHead vals
      HitchhikerSetMapNodeLeaf l -> case check curMax l of
        Nothing -> error "HitchhikerSetMap.findMaxKey: empty leaf"
        Just x  -> x

    check Nothing hh
      | M.null hh = Nothing
      | otherwise = Just $ fst $ M.findMax hh
    check j@(Just x) hh
      | M.null hh = j
      | otherwise = Just $ max x $ fst $ M.findMax hh

mapKeysMonotonic :: forall k a v
                  . (Show k, Show a, Show v, Ord k, Ord a, Ord v)
                 => (k -> a)
                 -> HitchhikerSetMap k v
                 -> HitchhikerSetMap a v
mapKeysMonotonic _ (HITCHHIKERSETMAP config Nothing) =
  HITCHHIKERSETMAP config Nothing
mapKeysMonotonic func (HITCHHIKERSETMAP config (Just top)) =
  (HITCHHIKERSETMAP config (Just $ go top))
  where
    go :: HitchhikerSetMapNode k v -> HitchhikerSetMapNode a v
    go = \case
      HitchhikerSetMapNodeIndex (TreeIndex keys vals) hh ->
        HitchhikerSetMapNodeIndex (TreeIndex mkeys mvals) mhh
        where
          mkeys = map func keys
          mvals = map go vals
          mhh = M.mapKeysMonotonic func hh
      HitchhikerSetMapNodeLeaf l ->
        HitchhikerSetMapNodeLeaf $ M.mapKeysMonotonic func l

-- Maps all the values in a HitchhikerSetMap, where the transformation is
-- monotonic.
mapValsMonotonic :: forall k v a
     . (Show k, Show v, Show a, Ord k, Ord v, Ord a)
    => (v -> a)
    -> HitchhikerSetMap k v
    -> HitchhikerSetMap k a
mapValsMonotonic _ (HITCHHIKERSETMAP config Nothing) =
  HITCHHIKERSETMAP config Nothing
mapValsMonotonic func (HITCHHIKERSETMAP config (Just top)) =
  (HITCHHIKERSETMAP config (Just $ go top))
  where
    go :: HitchhikerSetMapNode k v -> HitchhikerSetMapNode k a
    go = \case
      HitchhikerSetMapNodeIndex (TreeIndex keys vals) hh ->
        HitchhikerSetMapNodeIndex (TreeIndex keys mvals) mhh
        where
          mvals = map go vals
          mhh = M.map (ssetMap func) hh
      HitchhikerSetMapNodeLeaf l ->
        HitchhikerSetMapNodeLeaf $
        M.map (strip . HS.mapMonotonic func . weave config) l

-- TODO: Both Claude and ChatGPT suggest that performing inner rebalancing
-- usually isn't worth it, especially when you end up filtering out large parts
-- of the tree. This implementation just does the obvious thing.
--
-- Consider either going through all the other functions that tried to do
-- something smarter and just do this instead.
mapMaybeWithKey :: (Show k, Ord k, Show a, Ord a, Show b, Ord b)
                => (k -> HitchhikerSet a -> Maybe (HitchhikerSet b))
                -> HitchhikerSetMap k a -> HitchhikerSetMap k b
mapMaybeWithKey fun (HITCHHIKERSETMAP config Nothing) =
  HITCHHIKERSETMAP config Nothing
mapMaybeWithKey fun (HITCHHIKERSETMAP config (Just top)) =
  fromLeafMaps config $
  map (M.mapMaybeWithKey mapFun) $
  getLeafList (hhSetMapTF config) top
  where
    mapFun k v = strip <$> fun k (weave config v)
