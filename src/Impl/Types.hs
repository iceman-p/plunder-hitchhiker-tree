module Impl.Types where

import           ClassyPrelude

-- An Index is the main payload of an index node: the inner node of a B+ tree.
--
-- An index is a parallel array where each key is the smallest item in the
-- following value sequence.
--
-- (Contrast that with how the Clojure hitchhiker tree works where things are
-- right aligned instead.)
data TreeIndex k v = TreeIndex (Vector k) (Vector v)
  deriving (Show, Eq, Generic, NFData)

-- | Bundle of functions for manipulating a given tree type.
--
-- This generalizes how to deal with the leaves and hitchhiker data on each
-- node since HitchhikerMap, HitchhierSet and HitchhikerSetMap have different
-- requirements, but all otherwise share the core tree.
data TreeFun key subnode hhMap leafMap = TreeFun {
  -- Constructor/elimination.
  mkNode       :: TreeIndex key subnode -> hhMap -> subnode,
  mkLeaf       :: leafMap -> subnode,
  caseNode     :: subnode -> Either ((TreeIndex key subnode), hhMap) leafMap,

  -- Leaf storage options.
  leafMerge    :: leafMap -> hhMap -> leafMap,
  leafLength   :: leafMap -> Int,
  leafSplitAt  :: Int -> leafMap -> (leafMap, leafMap),
  leafFirstKey :: leafMap -> key,
  leafEmpty    :: leafMap,

  -- Hitchhiker storage options.
  hhMerge      :: hhMap -> hhMap -> hhMap,
  hhLength     :: hhMap -> Int,
  hhSplit      :: key -> hhMap -> (hhMap, hhMap),
  hhEmpty      :: hhMap
  };
