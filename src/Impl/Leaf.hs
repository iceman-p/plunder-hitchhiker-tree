{-# LANGUAGE Strict     #-}
{-# LANGUAGE StrictData #-}
module Impl.Leaf where

import           ClassyPrelude

import           Control.Arrow     ((***))

import           Data.Sorted.Row
import           Data.Sorted.Types

import           Impl.Index
import           Impl.Types
import           Types
import           Utils

splitLeafMany :: forall k v n h l
               . TreeFun k v n h l -> Int -> l -> TreeIndex k n
splitLeafMany TreeFun{..} maxLeafItems items
  -- Leaf items don't overflow a single node.
  | itemLen <= maxLeafItems =
      singletonIndex $ mkLeaf items

  -- We have to split, but only into two nodes.
  | itemLen <= 2 * maxLeafItems =
      let numLeft = div itemLen 2
          (leftLeaf, rightLeaf) = leafSplitAt numLeft items
          rightFirstItem = leafFirstKey rightLeaf
      in indexFromList (singleton rightFirstItem)
                       (fromList [mkLeaf leftLeaf,
                                  mkLeaf rightLeaf])

  -- We have to split the node into more than two nodes.
  | otherwise =
      uncurry indexFromList $
      (fromList *** (fromList . map mkLeaf)) $
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
