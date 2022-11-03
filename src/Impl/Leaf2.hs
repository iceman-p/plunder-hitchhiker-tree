module Impl.Leaf2 where

import           Control.Arrow ((***))
import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))

import           Impl.Types
import           Index
import           Types
import           Utils

import qualified Data.Sequence as Q

splitLeafMany2 :: forall k n h l. TreeFun2 k n h l -> Int -> l -> Index k n
splitLeafMany2 TreeFun2{..} maxLeafItems items
  -- Leaf items don't overflow a single node.
  | itemLen <= maxLeafItems =
      singletonIndex $ mkLeaf items

  -- We have to split, but only into two nodes.
  | itemLen <= 2 * maxLeafItems =
      let numLeft = div itemLen 2
          (leftLeaf, rightLeaf) = leafSplitAt numLeft items
          rightFirstItem = leafFirstKey rightLeaf
      in indexFromList (Q.singleton rightFirstItem)
                       (Q.fromList [mkLeaf leftLeaf,
                                    mkLeaf rightLeaf])

  -- We have to split the node into more than two nodes.
  | otherwise =
      uncurry indexFromList $
      (Q.fromList *** (Q.fromList . map mkLeaf)) $
      split' items ([], [])

  where
    itemLen = leafLength items

    split' items (keys, leafs)
      | itemLen > 2 * maxLeafItems =
          let (leaf, rem') = leafSplitAt maxLeafItems items
              key = leafFirstKey rem'
          in split' rem' (key : keys, leaf:leafs)
      | itemLen > maxLeafItems =
          let numLeft = div itemLen 2
              (left, right) = leafSplitAt numLeft items
              key = leafFirstKey right
          in split' leafEmpty (key:keys, right:(left:leafs))
      | itemLen == 0 = (reverse keys, reverse leafs)
      | otherwise = error "constraint violation"
      where
        itemLen = leafLength items