module Leaf where

import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))

import           Impl.Types
import           Index
import           Types
import           Utils

import qualified Data.Sequence as Q

-- splitLeafMany returns an index to all the leaves, even if it's a singleton
-- leaf.
splitLeafMany :: forall k v a lt
               . (Show k, Show a, Show lt)
              => Int
              -> (Seq lt -> a)
              -> (lt -> k)
              -> Seq lt
              -> Index k a
splitLeafMany maxLeafItems mkNode leafKey items
  -- Leaf items don't overflow a single node.
  | Q.length items <= maxLeafItems =
      singletonIndex $ mkNode items

  -- We have to split, but only into two nodes.
  | Q.length items <= 2 * maxLeafItems =
      let numLeft = div (Q.length items) 2
          (leftLeaf, rightLeaf) = Q.splitAt numLeft items
          rightFirstItem = leafKey $ qHeadUnsafe rightLeaf
      in indexFromList (Q.singleton rightFirstItem)
                       (Q.fromList [mkNode leftLeaf,
                                    mkNode rightLeaf])


  -- We have to split the node into more than two nodes.
  | otherwise = uncurry indexFromList $ split' items (Q.Empty, Q.Empty)

  where
    split' :: Seq lt -> (Seq k, Seq a) -> (Seq k, Seq a)
    split' items (keys, leafs)
      | Q.length items > 2 * maxLeafItems =
          let (leaf, rem') = Q.splitAt maxLeafItems items
              key = leafKey $ qHeadUnsafe rem'
          in split' rem' (keys |> key, leafs |> (mkNode leaf))
      | Q.length items > maxLeafItems =
          let numLeft = div (Q.length items) 2
              (leftLeaf, rightLeaf) = Q.splitAt numLeft items
              key = leafKey $ qHeadUnsafe rightLeaf
          in (keys |> key,
              leafs |> (mkNode leftLeaf) |> (mkNode rightLeaf))
      | otherwise = error "split many error"
