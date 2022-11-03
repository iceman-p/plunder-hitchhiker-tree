module Impl.Tree2 where

import           Control.Monad
import           Data.Map      (Map)
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))
import           Debug.Trace

import           Impl.Index2
import           Impl.Leaf2
import           Impl.Types
import           Leaf
import           Types
import           Utils

import qualified Data.Map      as M
import qualified Data.Sequence as Q

fixUp :: TreeConfig
      -> TreeFun2 k a hh lt
      -> Index k a
      -> a
fixUp config tf@TreeFun2{..} idx = case fromSingletonIndex idx of
  Just newRootNode -> newRootNode
  Nothing          ->
    fixUp config tf (extendIndex tf (maxLeafItems config) idx)

insertRec :: (Show k, Show a, Show lt, Show hh, Ord k)
          => TreeConfig
          -> TreeFun2 k a hh lt
          -> hh
          -> a
          -> Index k a
insertRec config tf@TreeFun2{..} toAdd node =
  case caseNode node of
    Left (children, hitchhikers)
      | hhLength merged > maxHitchhikers config ->
          -- We have reached the maximum number of hitchhikers, we now need to
          -- flush these downwards.
          extendIndex tf (maxLeafItems config) $
            distributeDownwards config tf merged children emptyIndex
      | otherwise ->
          -- All we must do is rebuild the node with the new k/v pair added on
          -- as a hitchhiker to this node.
          singletonIndex $ mkNode children merged
      where
        merged = hhMerge hitchhikers toAdd

    Right items                  ->
      splitLeafMany2 tf (maxLeafItems config) $ leafMerge items toAdd

-- Given a list of hitchhikers, try to distribute each downward to the next
-- level. This function is responsible for sending the right output to
-- indexRec, and parsing that return value back into a coherent index.
distributeDownwards
  :: (Show k, Show a, Show lt, Show hh, Ord k)
  => TreeConfig
  -> TreeFun2 k a hh lt
  -> hh
  -> Index k a  -- input
  -> Index k a  -- building output
  -> Index k a

-- Base case: single subtree or end of list:
distributeDownwards config tf@TreeFun2{..}
                    hitchhikers
                    (Index Empty (node :<| Empty))
                    o@(Index oKeys oHashes) =
  case hhLength hitchhikers of
    0 ->
      -- there are no hitchhikers, running insertRec will just break the node
      -- structure, since removeNode will just remove the existing node.
      Index oKeys (oHashes |> node)
    _ ->
      -- all remaining hitchhikers are either to the right or are in the mono
      -- node.
      let (Index endKeys endHashes) = insertRec config tf hitchhikers node
      in Index (oKeys <> endKeys) (oHashes <> endHashes)

distributeDownwards config tf@TreeFun2{..}
                    hitchhikers
                    i@(Index (key :<| restKeys) (node :<| restNodes))
                    o@(Index outKeys outNodes) =
  let (toAdd, rest) = hhSplit key hitchhikers
  in case hhLength toAdd of
    0 ->
      -- There are no things to distribute downward to this subtree.
      distributeDownwards config tf
                          hitchhikers
                          (Index restKeys restNodes)
                          (Index (outKeys |> key) (outNodes |> node))
    _ ->
      -- We distribute downwards all items up to the split point.
      let inner@(Index subKeys subHashes) = insertRec config tf toAdd node
          out = Index (outKeys <> subKeys <> (Q.singleton key))
                      (outNodes <> subHashes)
      in distributeDownwards config tf
                             rest
                             (Index restKeys restNodes)
                             out


-- Forces a flush of all hitchhikers down to the leaf levels and return the
-- resulting leaf vectors.

flushDownwards :: Ord k => TreeFun2 k a hh lt -> a -> Seq lt
flushDownwards tf@TreeFun2{..} = go hhEmpty
  where
    go hh node = case caseNode node of
      Right leaves                 -> Q.singleton $ leafMerge leaves hh
      Left (children, hitchhikers) ->
        distribute children (hhMerge hitchhikers hh)

    distribute (Index Empty (node :<| Empty)) hitchhikers =
      go hitchhikers node
    distribute (Index (key :<| xsKeys) (node :<| xsNodes)) hitchhikers =
      let (toAdd, rest) = hhSplit key hitchhikers
      in (go toAdd node) <> (distribute (Index xsKeys xsNodes) rest)
