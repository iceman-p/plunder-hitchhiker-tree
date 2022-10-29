module Impl.Tree where

import           Control.Monad
import           Data.Map      (Map)
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))

import           Index
import           Leaf
import           Types
import           Utils

import qualified Data.Map      as M
import qualified Data.Sequence as Q


-- TreeImpl is where the bulk logic of tree manipulation lives. It's an
-- internal module which is only used by the user facing data structures.

-- Generalize operations on a tree.
--
-- TreeFun keyType indexType hitchhikerType leafType
data TreeFun k a hh lt = TreeFun {
  mkIndex   :: Index k a -> Seq hh -> a,

  mkLeaf    :: Seq lt -> a,

  caseNode  :: a -> Either ((Index k a), Seq hh) (Seq lt),

  leafMerge :: Seq lt -> Seq hh -> Seq lt,

  hhMerge   :: Seq hh -> Seq hh -> Seq hh,

  leafKey   :: lt -> k,

  hhKey     :: hh -> k
  }

fixUp :: TreeConfig
      -> TreeFun k a hh lt
      -> Index k a
      -> a
fixUp config tf@TreeFun{..} idx = case fromSingletonIndex idx of
  Just newRootNode -> newRootNode
  Nothing          ->
    fixUp config tf (extendIndex (maxLeafItems config) mkIndex idx)


insertRec :: (Show k, Show a, Show lt, Show hh, Ord k)
          => TreeConfig
          -> TreeFun k a hh lt
          -> Seq hh
          -> a
          -> Index k a
insertRec config tf@TreeFun{..} toAdd node =
  case caseNode node of
    Left (children, hitchhikers)
      | Q.length merged > maxHitchhikers config ->
          -- We have reached the maximum number of hitchhikers, we now need to
          -- flush these downwards.
          extendIndex (maxLeafItems config) mkIndex $
            distributeDownwards config tf merged children emptyIndex
      | otherwise ->
          -- All we must do is rebuild the node with the new k/v pair added on
          -- as a hitchhiker to this node.
          singletonIndex $ mkIndex children merged
      where
        merged = hhMerge hitchhikers toAdd

    Right items                  ->
      splitLeafMany (maxLeafItems config) mkLeaf leafKey $
        leafMerge items toAdd

-- Given a list of hitchhikers, try to distribute each downward to the next
-- level. This function is responsible for sending the right output to
-- indexRec, and parsing that return value back into a coherent index.
distributeDownwards
  :: (Show k, Show a, Show lt, Show hh, Ord k)
  => TreeConfig
  -> TreeFun k a hh lt
  -> Seq hh
  -> Index k a  -- input
  -> Index k a  -- building output
  -> Index k a

-- Base case: single subtree or end of list:
distributeDownwards config tf@TreeFun{..}
                    hitchhikers
                    (Index Empty (node :<| Empty))
                    o@(Index oKeys oHashes) =
  case hitchhikers of
    Empty ->
      -- there are no hitchhikers, running insertRec will just break the node
      -- structure, since removeNode will just remove the existing node.
      Index oKeys (oHashes |> node)
    hh ->
      -- all remaining hitchhikers are either to the right or are in the mono
      -- node.
      let (Index endKeys endHashes) = insertRec config tf hitchhikers node
      in Index (oKeys <> endKeys) (oHashes <> endHashes)

distributeDownwards config tf@TreeFun{..}
                    hitchhikers
                    i@(Index (key :<| restKeys) (node :<| restNodes))
                    o@(Index outKeys outNodes) =
  let (toAdd, rest) = Q.spanl (\h -> (hhKey h) < key) hitchhikers
  in case toAdd of
    Empty ->
      -- There are no things to distribute downward to this subtree.
      distributeDownwards config tf
                          hitchhikers
                          (Index restKeys restNodes)
                          (Index (outKeys |> key) (outNodes |> node))
    toAdd ->
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

flushDownwards :: Ord k => TreeFun k a hh lt -> a -> Seq (Seq lt)
flushDownwards tf@TreeFun{..} = go Empty
  where
    go hh node = case caseNode node of
      Right leaves                 -> Q.singleton $ leafMerge leaves hh
      Left (children, hitchhikers) ->
        distribute children (hhMerge hitchhikers hh)

    distribute (Index Empty (node :<| Empty)) hitchhikers =
      go hitchhikers node
    distribute (Index (key :<| xsKeys) (node :<| xsNodes)) hitchhikers =
      let (toAdd, rest) = Q.spanl (\h -> (hhKey h) < key) hitchhikers
      in (go toAdd node) <> (distribute (Index xsKeys xsNodes) rest)
