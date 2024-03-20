module Impl.Index where

import           ClassyPrelude

import           Data.Sorted.Row
import           Data.Sorted.Types

import           Impl.Strict
import           Impl.Types
import           Types
import           Utils

emptyIndex :: TreeIndex k v
emptyIndex = TreeIndex mempty mempty

mergeIndex :: TreeIndex key val -> key -> TreeIndex key val -> TreeIndex key val
mergeIndex (TreeIndex !leftKeys !leftVals) !middleKey
           (TreeIndex !rightKeys !rightVals) =
    TreeIndex
      (concat [leftKeys, singleton middleKey, rightKeys])
      (leftVals <> rightVals)

indexFromList :: Row k -> Row v -> TreeIndex k v
indexFromList !keys !valPtrs = TreeIndex keys valPtrs

singletonIndex :: v -> TreeIndex k v
singletonIndex !v = TreeIndex mempty $! singleton v

fromSingletonIndex :: TreeIndex k v -> StrictMaybe v
fromSingletonIndex (TreeIndex _keys vals) =
    if length vals == 1 then SJust $ unsafeHead vals else SNothing

indexNumKeys :: TreeIndex k v -> Int
indexNumKeys (TreeIndex keys _vals) = length keys

indexNumVals :: TreeIndex k v -> Int
indexNumVals (TreeIndex _keys vals) = length vals

splitIndexAt :: Int -> TreeIndex k v -> (TreeIndex k v, k, TreeIndex k v)
splitIndexAt numLeftKeys (TreeIndex keys vals)
    | (!leftKeys, middleKeyAndRightKeys) <- splitAt numLeftKeys     keys
    , (!leftVals, !rightVals)             <- splitAt (numLeftKeys+1) vals
    = case uncons middleKeyAndRightKeys of
        Just (!middleKey, !rightKeys) ->
            (TreeIndex leftKeys leftVals, middleKey,
             TreeIndex rightKeys rightVals)
        Nothing -> error "splitIndex: cannot split an empty index"

mapIndex :: (v -> w) -> TreeIndex k v -> TreeIndex k w
mapIndex fun (TreeIndex keys vals) = TreeIndex keys $ map fun vals

-- Given a pure index with no hitchhikers, create a node.
extendIndex :: TreeFun k v a hh lt
            -> Int
            -> TreeIndex k a
            -> TreeIndex k a
extendIndex tf@TreeFun{..} maxIdxKeys = go
  where
    maxIdxVals = maxIdxKeys + 1

    go !index
      | numVals <= maxIdxVals =
          singletonIndex $ mkNode index hhEmpty

      | numVals <= 2 * maxIdxVals =
          let (!leftIndex, !middleKey, !rightIndex) =
                splitIndexAt (div numVals 2 - 1) index
          in indexFromList (singleton middleKey)
                           (fromList [mkNode leftIndex hhEmpty,
                                      mkNode rightIndex hhEmpty])

      | otherwise =
          let (!leftIndex, !middleKey, !rightIndex) =
                splitIndexAt maxIdxKeys index
          in mergeIndex (singletonIndex (mkNode leftIndex hhEmpty))
                        middleKey
                        (go rightIndex)
      where
        numVals = indexNumVals index

-- ----------------------------------------------------------------------------

vecUncons :: Row a -> StrictMaybe (a, Row a)
vecUncons v
    | null v  = SNothing
    | otherwise =
      let !h = unsafeHead v
          !t = unsafeTail v
      in SJust (h, t)

vecUnsnoc :: Row a -> StrictMaybe (Row a, a)
vecUnsnoc v
    | null v  = SNothing
    | otherwise =
      let !i = unsafeInit v
          !l = unsafeLast v
      in SJust (i, l)

-- A TreeIndex with a hole in it.
--
data IndexContext k a = IndexContext {
  leftKeys  :: Row k,
  leftVals  :: Row a,
  rightKeys :: Row k,
  rightVals :: Row a
  }

-- Like splitIndexAt, but instead breaks on where a given key should be.
valView :: Ord k
        => k
        -> TreeIndex k a
        -> (IndexContext k a, a)
valView key (TreeIndex keys vals)
    | (!leftKeys, !rightKeys)       <- span (<=key) keys
    , n                           <- length leftKeys
    , (!leftVals, !valAndRightVals) <- splitAt n vals
    , SJust (!val, !rightVals)      <- vecUncons valAndRightVals
    = ( IndexContext{..}, val )
    | otherwise
    = error "valView: cannot split an empty index"


leftView :: IndexContext key val
         -> StrictMaybe (IndexContext key val, val, key)
leftView ctx = do
  case (vecUnsnoc (leftVals ctx), vecUnsnoc (leftKeys ctx)) of
    (SJust (!leftVals, !leftVal), SJust (!leftKeys, !leftKey)) ->
      SJust (ctx { leftKeys = leftKeys
                 , leftVals = leftVals
                 }, leftVal, leftKey)
    _ -> SNothing

rightView :: IndexContext key val
          -> StrictMaybe (key, val, IndexContext key val)
rightView ctx =
  case (vecUncons (rightVals ctx), vecUncons (rightKeys ctx)) of
    (SJust (!rightVal, !rightVals), SJust (!rightKey, !rightKeys)) ->
      SJust (rightKey, rightVal,
             ctx { rightKeys = rightKeys
                 , rightVals = rightVals
                 })
    _ -> SNothing

putVal :: IndexContext key val -> val -> TreeIndex key val
putVal ctx !val =
  TreeIndex
    (leftKeys ctx <> rightKeys ctx)
    (leftVals ctx <> singleton val <> rightVals ctx)

putIdx :: IndexContext key val -> TreeIndex key val -> TreeIndex key val
putIdx ctx (TreeIndex keys vals) =
  TreeIndex
    (leftKeys ctx <> keys <> rightKeys ctx)
    (leftVals ctx <> vals <> rightVals ctx)

