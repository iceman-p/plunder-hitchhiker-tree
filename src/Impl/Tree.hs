module Impl.Tree where

import           ClassyPrelude

import           Data.Map      (Map)
import           Data.Vector   (Vector)

import           Impl.Index
import           Impl.Leaf
import           Impl.Types
import           Types
import           Utils

import           Data.Sorted

import qualified Data.List     as L
import qualified Data.Map      as M
import qualified Data.Set      as S
import qualified Data.Vector   as V

treeDepth :: TreeFun k v a hh lt
          -> a
          -> Int
treeDepth tf@TreeFun{..} node = case caseNode node of
  Left (TreeIndex _ vals, _) -> 1 + (treeDepth tf $ vals V.! 0)
  Right _                    -> 1

treeWeightEstimate :: TreeFun k v a hh lt
                   -> a
                   -> Int
treeWeightEstimate tf@TreeFun{..} node = case caseNode node of
  Left (TreeIndex keys vals, _) -> (length keys) * (treeDepth tf $ vals V.! 0)
  Right l                       -> leafLength l

fixUp :: TreeConfig
      -> TreeFun k v a hh lt
      -> TreeIndex k a
      -> a
fixUp config tf@TreeFun{..} idx = case fromSingletonIndex idx of
  Just newRootNode -> newRootNode
  Nothing          ->
    fixUp config tf (extendIndex tf (maxLeafItems config) idx)

insertRec :: (Show k, Show a, Show lt, Show hh, Ord k)
          => TreeConfig
          -> TreeFun k v a hh lt
          -> hh
          -> a
          -> TreeIndex k a
insertRec config tf@TreeFun{..} toAdd node =
  case caseNode node of
    Left (children, hitchhikers)
      | hhLength merged > maxHitchhikers config ->
          -- We have reached the maximum number of hitchhikers, we now need to
          -- flush these downwards.
          extendIndex tf (maxLeafItems config) $
            distributeDownwards config tf merged children
      | otherwise ->
          -- All we must do is rebuild the node with the new k/v pair added on
          -- as a hitchhiker to this node.
          singletonIndex $ mkNode children merged
      where
        merged = hhMerge hitchhikers toAdd

    Right items                  ->
      splitLeafMany tf (maxLeafItems config) $ leafInsert items toAdd

{-
-- Given a list of hitchhikers, try to distribute each downward to the next
-- level. This function is responsible for sending the right output to
-- indexRec, and parsing that return value back into a coherent index.
distributeDownwards
  :: forall k v a hh lt
   . (Show k, Show a, Show hh, Show lt, Ord k)
  => TreeConfig
  -> TreeFun k v a hh lt
  -> hh
  -> TreeIndex k a
  -> TreeIndex k a
distributeDownwards config tf@TreeFun{..} inHitchhikers (TreeIndex keys vals) =
  build $ L.unfoldr go (0, inHitchhikers)
  where
    build :: [(Vector k, Vector a)] -> TreeIndex k a
    build a = uncurry TreeIndex $ concatUnzip a

    go :: (Int, hh) -> Maybe ((Vector k, Vector a), (Int, hh))
    go (idx, hh) = case (keys V.!? idx, vals V.!? idx) of
      (Nothing, Nothing)    -> Nothing
      (Just _, Nothing)     -> error "Completely broken tree structure."
      (Nothing, Just node)  ->
        case hhLength hh of
          0 ->
            -- There are no hitchhikers, running insertRec will just break the
            -- node structure.
            Just ((V.empty, V.singleton node), (idx + 1, hhEmpty))
          _ ->
            -- This is when we're in a singleton val index or in the rightmost
            -- entry.
            let (TreeIndex oKeys oVals) = insertRec config tf hh node
            in Just ((oKeys, oVals), (idx + 1, hhEmpty))
      (Just key, Just node) ->
        let (toAdd, rest) = hhSplit key hh
        in case hhLength toAdd of
             0 ->
               -- There's nothing here to distribute downwards, so copy the
               -- input
               Just ((V.singleton key, V.singleton node),
                     (idx + 1, hh))
             _ ->
               let (TreeIndex subKeys subHashes) =
                     insertRec config tf toAdd node
               in Just ((V.snoc subKeys key, subHashes),
                        (idx + 1, rest))
-}

-- Given a list of hitchhikers, try to distribute each downward, but written as
-- a map.
distributeDownwards
  :: forall k v a hh lt
   . (Show k, Show a, Show hh, Show lt, Ord k)
  => TreeConfig
  -> TreeFun k v a hh lt
  -> hh
  -> TreeIndex k a
  -> TreeIndex k a
distributeDownwards config tf@TreeFun{..} inHH treeIn@(TreeIndex keys vals)
  -- No things to distribute, no need to do the calculations.
  | hhLength inHH == 0 = treeIn

  | otherwise =
      let keyList    = V.toList keys
          splitHH    = split keyList inHH
          indexList  = map push $ zip splitHH (V.toList vals)
          (newKeys, newVals) = joinIndex keyList indexList
      in TreeIndex (V.fromList newKeys) (V.concat newVals)
  where
    split :: [k] -> hh -> [hh]
    split [] hh = [hh]
    split (key:keys) hh = let (toAdd, rest) = hhSplit key hh
                          in (toAdd:(split keys rest))

    push :: (hh, a) -> TreeIndex k a
    push (hh, node)
      | hhLength hh == 0 = singletonIndex node
      | otherwise        = insertRec config tf hh node

    joinIndex :: [k] -> [TreeIndex k a] -> ([k], [Vector a])
    joinIndex [] [] = ([], [])
    joinIndex [] [TreeIndex keys vals] = (V.toList keys, [vals])
    joinIndex (k:ks) ((TreeIndex keys vals):ts) =
      let (keyrest, valrest) = joinIndex ks ts
      in ( (V.toList keys) ++ [k] ++ keyrest
         , (vals:valrest) )

-- Forces a flush of all hitchhikers down to the leaf levels and return the
-- resulting leaf vectors.
getLeafList :: Ord k => TreeFun k v a hh lt -> a -> [lt]
getLeafList tf@TreeFun{..} = go hhEmpty
  where
    go hh node = case caseNode node of
      Right leaves                 -> [leafInsert leaves hh]
      Left (children, hitchhikers) ->
        join $ L.unfoldr distribute (0, children, hhMerge hitchhikers hh)

    distribute (idx, i@(TreeIndex keys vals), hh) =
      case (keys V.!? idx, vals V.!? idx) of
        (Nothing, Nothing) -> Nothing
        (Just _, Nothing) -> error "Completely broken tree structure."
        (Nothing, Just node) -> Just (go hh node, (idx + 1, i, hhEmpty))
        (Just key, Just node) ->
          let (toAdd, rest) = hhSplit key hh
          in Just (go toAdd node, (idx + 1, i, rest))

-- Given a node, ensure that all hitchhikers have been pushed down to leaves.
flushDownwards :: Ord k => TreeFun k v a hh lt -> a -> a
flushDownwards tf@TreeFun{..} = go hhEmpty
  where
    go hh node = case caseNode node of
      Right leaves                 -> mkLeaf $ leafInsert leaves hh
      Left (children@(TreeIndex keys vals), hitchhikers) ->
        let newVals = V.unfoldr distribute (0, children, hhMerge hitchhikers hh)
        in mkNode (TreeIndex keys newVals) hhEmpty

    distribute (idx, i@(TreeIndex keys vals), hh) =
      case (keys V.!? idx, vals V.!? idx) of
        (Nothing, Nothing) -> Nothing
        (Just _, Nothing) -> error "Completely broken tree structure."
        (Nothing, Just node) -> Just (go hh node, (idx + 1, i, hhEmpty))
        (Just key, Just node) ->
          let (toAdd, rest) = hhSplit key hh
          in Just (go toAdd node, (idx + 1, i, rest))

-- -- Distribute all hitchhikers down to the leaves without making any
-- -- intermediate new
-- nocopyFlushDownwards :: Ord k => TreeFun k v a hh lt -> a -> a
-- nocopyFlushDownwards tf@TreeFun{..} = go []
--   where
--     go hh node =

-- Deletion

-- What do we have to do here? haskey-btree just recurses downwards, but
-- hitchhiker trees also have to delete entries at each level.
--
-- Some users of Tree will want to delete just a key while others will want to
-- delete a specific k/v pair. We make this generic by passing maybe the value
-- to delete.
deleteRec :: (Show k, Show a, Show lt, Show hh, Ord k)
          => TreeConfig
          -> TreeFun k v a hh lt
          -> k
          -> Maybe v
          -> a
          -> a
deleteRec config tf@TreeFun{..} key mybV node =
  case caseNode node of
    Right items                  -> mkLeaf $ leafDelete key mybV items
    Left (children, hitchhikers) ->
      let (ctx, child) = valView key children
          newChild = deleteRec config tf key mybV child
          childNeedsMerge = nodeNeedsMerge config tf newChild
          prunedHH = hhDelete key mybV hitchhikers
      in case (childNeedsMerge, leftView ctx, rightView ctx) of
           (True, _, Just (rKey, rChild, rCtx)) ->
             mkNode (putIdx rCtx (mergeNodes config tf newChild rKey rChild))
                    prunedHH
           (True, Just (lCtx, lChild, lKey), _) ->
             mkNode (putIdx lCtx (mergeNodes config tf lChild lKey newChild))
                    prunedHH
           (True, _, _) -> error "deleteRec: node with single child"
           _ -> mkNode (putVal ctx newChild) prunedHH

nodeNeedsMerge :: TreeConfig
               -> TreeFun k v a hh lt
               -> a
               -> Bool
nodeNeedsMerge config TreeFun{..} node = case caseNode node of
  Left (children, hitchhikers) -> indexNumKeys children < minIdxKeys config
  Right items                  -> leafLength items < minLeafItems config

mergeNodes :: (Show k, Show a, Show hh, Show lt, Ord k)
           => TreeConfig
           -> TreeFun k v a hh lt
           -> a
           -> k
           -> a
           -> TreeIndex k a
mergeNodes config tf@TreeFun{..} left middleKey right =
  case (caseNode left, caseNode right) of
    (Left (leftIdx, leftHH), Left (rightIdx, rightHH)) ->
      let left  = distributeDownwards config tf leftHH leftIdx
          right = distributeDownwards config tf rightHH rightIdx
      in extendIndex tf (maxIdxKeys config) (mergeIndex left middleKey right)
    (Right leftLeaf, Right rightLeaf)                  ->
      splitLeafMany tf (maxLeafItems config) (leafMerge leftLeaf rightLeaf)


asToSet :: Ord k => [ArraySet k] -> Set k
asToSet = foldl' S.union S.empty . map (S.fromList . ssetToAscList)

-- We walk the as and bs list in parallel. When the current a overlaps with
-- the current b, we put a into a list of partial mapping sets and keep
-- accumulating those until we don't have a
setlistMaplistIntersect :: forall k v
         . (Show k, Ord k, Show v)
        => [ArraySet k]
        -> [ArraySet k]
        -> [Map k v]
        -> [Map k v]
setlistMaplistIntersect _ a []                = []
setlistMaplistIntersect [] [] b                = []
setlistMaplistIntersect partial [] (b:bs) =
  let f = M.restrictKeys b $ asToSet partial
  in if M.null f
     then []
     else f:[]
setlistMaplistIntersect partial ao@(a:as) bo@(b:bs) =
    let aMin = ssetFindMin a
        aMax = ssetFindMax a
        bMin = fst $ M.findMin b
        bMax = fst $ M.findMax b
        overlap = aMin <= bMax && bMin <= aMax

        filteredBy s rest = let f = M.restrictKeys b s
                            in if M.null f then rest
                                           else f:rest

    in case (partial, overlap) of
         ([], False)
           | aMax > bMax -> setlistMaplistIntersect [] ao bs
           | otherwise   -> setlistMaplistIntersect [] as bo
         (partial, False) ->
             filteredBy (asToSet partial) $ setlistMaplistIntersect [] ao bs
         (partial, True)
           | aMax == bMax ->
               filteredBy (asToSet (a:partial)) $
               setlistMaplistIntersect [] as bs
           | aMax < bMax ->
               setlistMaplistIntersect (a:partial) as bo
           | otherwise ->
               filteredBy (asToSet (a:partial)) $
               setlistMaplistIntersect [] ao bs

setlistMaplistIntersectWithPred :: forall k v
         . (Show k, Ord k, Show v)
        => (k -> v -> Maybe v)
        -> [ArraySet k]
        -> [ArraySet k]
        -> [Map k v]
        -> [Map k v]
setlistMaplistIntersectWithPred func _ a []                = []
setlistMaplistIntersectWithPred func [] [] b                = []
setlistMaplistIntersectWithPred func partial [] (b:bs) =
  let f = M.restrictKeys b $ asToSet partial
  in if M.null f
     then []
     else let ff = M.mapMaybeWithKey func f
          in if M.null ff
             then []
             else ff:[]
setlistMaplistIntersectWithPred func partial ao@(a:as) bo@(b:bs) =
    let aMin = ssetFindMin a
        aMax = ssetFindMax a
        bMin = fst $ M.findMin b
        bMax = fst $ M.findMax b
        overlap = aMin <= bMax && bMin <= aMax

        filteredBy s rest = let f = M.restrictKeys b s
                            in if M.null f
                               then rest
                               else let ff = M.mapMaybeWithKey func f
                                    in if M.null ff
                                       then rest
                                       else ff:rest

        toSet :: [ArraySet k] -> Set k
        toSet = foldl' S.union S.empty . map (S.fromList . ssetToAscList)

    in case (partial, overlap) of
         ([], False)
           | aMax > bMax -> setlistMaplistIntersectWithPred func [] ao bs
           | otherwise   -> setlistMaplistIntersectWithPred func [] as bo
         (partial, False) ->
             filteredBy (asToSet partial) $
             setlistMaplistIntersectWithPred func [] ao bs
         (partial, True)
           | aMax == bMax ->
               filteredBy (toSet (a:partial)) $
               setlistMaplistIntersectWithPred func [] as bs
           | aMax < bMax ->
               setlistMaplistIntersectWithPred func (a:partial) as bo
           | otherwise ->
               filteredBy (toSet (a:partial)) $
               setlistMaplistIntersectWithPred func [] ao bs


-- With maplist x maplist, we have to also have a mapping function for when
-- there's an intersection.
maplistMaplistIntersect
  :: forall k a b c
   . (Show k, Ord k, Show a, Show b, Show c)
  => (a -> b -> c)
  -> [Map k a]
  -> [Map k a]
  -> [Map k b]
  -> [Map k c]
maplistMaplistIntersect _ _ a []                        = []
maplistMaplistIntersect _ [] [] b                       = []
maplistMaplistIntersect fun partial ao@(a:as) bo@(b:bs) =
    let aMin = fst $ M.findMin a
        aMax = fst $ M.findMax a
        bMin = fst $ M.findMin b
        bMax = fst $ M.findMax b
        overlap = aMin <= bMax && bMin <= aMax

        filteredBy ms rest = let f = M.intersectionWith fun ms b
                             in if M.null f then rest
                                            else f:rest

        toMap :: [Map k a] -> Map k a
        toMap = foldl' M.union M.empty

    in case (partial, overlap) of
         ([], False)
           | aMax > bMax -> maplistMaplistIntersect fun [] ao bs
           | otherwise   -> maplistMaplistIntersect fun [] as bo
         (partial, False) -> error "Should be impossible"
         (partial, True)
           | aMax == bMax ->
               filteredBy (toMap (a:partial)) $ maplistMaplistIntersect fun [] as bs
           | aMax < bMax ->
               maplistMaplistIntersect fun (a:partial) as bo
           | otherwise ->
               filteredBy (toMap (a:partial)) $ maplistMaplistIntersect fun [] ao bs


countListMerge :: Monoid a => (Int, a) -> (Int, a) -> (Int, a)
countListMerge (lc, l) (rc, r) = (lc + rc, l ++ r)


splitCountList :: Ord e
               => (e -> a -> Bool)
               -> e -> (Int, [a]) -> ((Int, [a]), (Int, [a]))
splitCountList fun e (_, i) = ((length left, left), (length right, right))
  where
    (left, right) = partition (fun e) i

