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
data TreeFun key value subnode hhMap leafMap = TreeFun {
  -- Constructor/elimination.
  mkNode       :: TreeIndex key subnode -> hhMap -> subnode,
  mkLeaf       :: leafMap -> subnode,
  caseNode     :: subnode -> Either ((TreeIndex key subnode), hhMap) leafMap,

  -- Leaf storage options.
  leafInsert   :: leafMap -> hhMap -> leafMap,
  leafMerge    :: leafMap -> leafMap -> leafMap,
  leafLength   :: leafMap -> Int,
  leafSplitAt  :: Int -> leafMap -> (leafMap, leafMap),
  leafFirstKey :: leafMap -> key,
  leafEmpty    :: leafMap,
  leafDelete   :: key -> Maybe value -> leafMap -> leafMap,

  -- Hitchhiker storage options.
  hhMerge      :: hhMap -> hhMap -> hhMap,
  hhLength     :: hhMap -> Int,
  hhWholeSplit :: [key] -> hhMap -> [hhMap],
  hhEmpty      :: hhMap,
  hhDelete     :: key -> Maybe value -> hhMap -> hhMap
  };

-- Default implementation of wholeSplit, which is passed in the legacy
-- implementation.
hhDefaultWholeSplit :: (k -> hh -> (hh, hh)) -> [k] -> hh -> [hh]
hhDefaultWholeSplit _ [] hh = [hh]
hhDefaultWholeSplit hhSplit (key:keys) hh =
  let (toAdd, rest) = hhSplit key hh
  in (toAdd:(hhDefaultWholeSplit hhSplit keys rest))
