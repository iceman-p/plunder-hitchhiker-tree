module Impl.Tree where

import           ClassyPrelude

import           Data.Map      (Map)
import           Data.Vector   (Vector)

import           Impl.Index
import           Impl.Leaf
import           Impl.Types
import           Types
import           Utils

import qualified Data.List     as L
import qualified Data.Map      as M
import qualified Data.Vector   as V

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
