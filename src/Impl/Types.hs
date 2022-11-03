module Impl.Types where

import           Types

-- TreeFun2 k n l h
data TreeFun2 key subnode hhMap leafMap = TreeFun2 {
  -- Constructor/elimination.
  mkNode       :: Index key subnode -> hhMap -> subnode,
  mkLeaf       :: leafMap -> subnode,
  caseNode     :: subnode -> Either ((Index key subnode), hhMap) leafMap,

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
