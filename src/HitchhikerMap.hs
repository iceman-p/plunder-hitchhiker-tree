module HitchhikerMap where

import           ClassyPrelude hiding (empty)

import           Data.Map      (Map)

import           Safe          (tailSafe)

import           Impl.Index
import           Impl.Leaf
import           Impl.Tree
import           Impl.Types
import           Types
import           Utils

import qualified Data.Map      as M
import qualified Data.Vector   as V

import qualified HitchhikerSet as HS

empty :: TreeConfig -> HitchhikerMap k v
empty config = HITCHHIKERMAP config Nothing

insert :: (Show k, Show v, Ord k)
       => k -> v -> HitchhikerMap k v -> HitchhikerMap k v
insert !k !v !(HITCHHIKERMAP config (Just root)) =
  HITCHHIKERMAP config $ Just $ insertRaw config k v root

insert !k !v (HITCHHIKERMAP config Nothing)
  = HITCHHIKERMAP config (Just $ HitchhikerMapNodeLeaf $ M.singleton k v)

insertRaw :: (Show k, Show v, Ord k)
          => TreeConfig -> k -> v -> HitchhikerMapNode k v
          -> HitchhikerMapNode k v
insertRaw config !k !v root =
  fixUp config hhMapTF $
  insertRec config hhMapTF (M.singleton k v) root

insertMany :: (Show k, Show v, Ord k)
           => Map k v -> HitchhikerMap k v -> HitchhikerMap k v

insertMany !items hhmap@(HITCHHIKERMAP config Nothing)
  | M.null items = hhmap
  | otherwise = HITCHHIKERMAP config $ Just $
                fixUp config hhMapTF $
                splitLeafMany hhMapTF (maxLeafItems config) items

insertMany !items hhmap@(HITCHHIKERMAP config (Just root))
  | M.null items = hhmap
  | otherwise = HITCHHIKERMAP config $ Just $
                fixUp config hhMapTF $
                insertRec config hhMapTF items root

-- Takes a list of leaves and make a valid set
fromLeafMaps :: (Show k, Show v, Ord k)
             => TreeConfig
             -> [Map k v]
             -> HitchhikerMap k v
fromLeafMaps config []   = HitchhikerMap.empty config
-- TODO: The following should check split.
fromLeafMaps config [m]  = HITCHHIKERMAP config $ Just
                         $ HitchhikerMapNodeLeaf m
fromLeafMaps config rawMaps = HITCHHIKERMAP config $ Just node
  where
    node = fixUp config hhMapTF treeIndex
    treeIndex = indexFromList idxV vals
    idxV = V.fromList $ tailSafe $ map (fst . M.findMin) rawMaps
    vals = V.fromList $ map HitchhikerMapNodeLeaf rawMaps

-- -----------------------------------------------------------------------

delete :: (Show k, Show v, Ord k, Ord v)
       => k -> HitchhikerMap k v -> HitchhikerMap k v
delete _ !(HITCHHIKERMAP config Nothing) = HITCHHIKERMAP config Nothing
delete !k !(HITCHHIKERMAP config (Just root)) =
  case deleteRec config hhMapTF k Nothing root of
    HitchhikerMapNodeIndex index hitchhikers
      | Just childNode <- fromSingletonIndex index ->
          if M.null hitchhikers then HITCHHIKERMAP config (Just childNode)
          else insertMany hitchhikers $ HITCHHIKERMAP config (Just childNode)
    HitchhikerMapNodeLeaf items
      | M.null items -> HITCHHIKERMAP config Nothing
    newRootNode -> HITCHHIKERMAP config (Just newRootNode)

-- -----------------------------------------------------------------------

hhMapTF :: (Show k, Show v, Ord k)
        => TreeFun k v (HitchhikerMapNode k v) (Map k v) (Map k v)
hhMapTF = TreeFun {
  mkNode = HitchhikerMapNodeIndex,
  mkLeaf = HitchhikerMapNodeLeaf,
  caseNode = \case
      HitchhikerMapNodeIndex a b -> Left (a, b)
      HitchhikerMapNodeLeaf l    -> Right l,

  leafInsert = M.unionWith (\a b -> b),
  leafMerge = (<>),
  leafLength = M.size,
  leafSplitAt = M.splitAt,
  leafFirstKey = fst . M.findMin,
  leafEmpty = M.empty,
  leafDelete = \k mybV m -> case mybV of
      Nothing -> M.delete k m
      Just _  -> error "Can't delete specific values in map",

  hhMerge = M.unionWith (\a b -> b),
  hhLength = M.size,
  hhWholeSplit = hhDefaultWholeSplit splitImpl,
  hhEmpty = M.empty,
  hhDelete = \k mybV m -> case mybV of
      Nothing -> M.delete k m
      Just _  -> error "Can't delete specific values in map"
  }

splitImpl :: Ord k => k -> Map k v -> (Map k v, Map k v)
splitImpl k m = M.spanAntitone (< k) m

-- Lookup --------------------------------------------------------------------

lookup :: Ord k => k -> HitchhikerMap k v -> Maybe v
lookup key (HITCHHIKERMAP _ Nothing) = Nothing
lookup key (HITCHHIKERMAP _ (Just top)) = lookInNode top
  where
    lookInNode = \case
      HitchhikerMapNodeIndex index hitchhikers ->
        case M.lookup key hitchhikers of
          Just v  -> Just v
          Nothing -> lookInNode $ findSubnodeByKey key index
      HitchhikerMapNodeLeaf items -> M.lookup key items

-- Map ------------------------------------------------------------------------

-- TODO: Balancing? Handling empty leaf maps? This stub is really restricted to
-- current usages to make forward progress. Come back and fix this later.
mapMaybe :: (Show k, Ord k, Show a, Show b)
         => (a -> Maybe b) -> HitchhikerMap k a -> HitchhikerMap k b
mapMaybe fun (HITCHHIKERMAP config Nothing) = (HITCHHIKERMAP config Nothing)
mapMaybe fun (HITCHHIKERMAP config (Just top)) =
  HITCHHIKERMAP config $ Just (go $ flushDownwards hhMapTF top)
  where
    go = \case
      HitchhikerMapNodeIndex index _ ->
        HitchhikerMapNodeIndex (mapIndex go index) M.empty
      HitchhikerMapNodeLeaf m ->
        let nm = M.mapMaybe fun m
        in if M.null nm
           then error "TODO: handle empty map in mapMaybe"
           else HitchhikerMapNodeLeaf nm

-- TODO: This isn't the best performing implementation, but it's Good Enough
-- for now. This is an attempt at using something like the
-- MultiIntesersectV3Naive code in a different context. This should be good
-- enough at "small" (<100K) items, and will probably be faster on the naive
-- tree walking interpreter than a proper tree pruning implementation for the
-- same reason V6Stack was: function frames are super expensive.
--
restrictKeys :: forall k v
              . (Show k, Show v, Ord k)
             => HitchhikerSet k
             -> HitchhikerMap k v
             -> HitchhikerMap k v
restrictKeys _ orig@(HITCHHIKERMAP _ Nothing) = orig

restrictKeys (HITCHHIKERSET _ Nothing) (HITCHHIKERMAP config _) =
  HitchhikerMap.empty config

restrictKeys (HITCHHIKERSET sConfig (Just a))
             (HITCHHIKERMAP mConfig (Just b)) =
  -- trace ("restrictKeys: " <> show a <> ", " <> show b) $
  fromLeafMaps mConfig $ setlistMaplistIntersect [] as bs
  where
    as = getLeafList HS.hhSetTF a
    bs = getLeafList hhMapTF b

intersectionWith :: forall k a b c
                  . (Show k, Ord k, Show a, Show b, Show c)
                 => (a -> b -> c)
                 -> HitchhikerMap k a
                 -> HitchhikerMap k b
                 -> HitchhikerMap k c
intersectionWith _ (HITCHHIKERMAP config Nothing) _ =
  (HITCHHIKERMAP config Nothing)

intersectionWith _ _ (HITCHHIKERMAP config Nothing) =
  (HITCHHIKERMAP config Nothing)

intersectionWith fun (HITCHHIKERMAP config (Just a)) (HITCHHIKERMAP _ (Just b))
  = fromLeafMaps config $ maplistMaplistIntersect fun [] as bs
  where
    as = getLeafList hhMapTF a
    bs = getLeafList hhMapTF b

toList :: (Show k, Ord k, Show v) => HitchhikerMap k v -> [(k, v)]
toList (HITCHHIKERMAP _ Nothing) = []
toList (HITCHHIKERMAP _ (Just a)) = concat $ map M.toList $ getLeafList hhMapTF a
