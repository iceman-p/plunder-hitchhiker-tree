module Impl.Index where

import           ClassyPrelude

import           Impl.Types
import           Types
import           Utils

import qualified Data.Vector   as V

emptyIndex :: TreeIndex k v
emptyIndex = TreeIndex mempty mempty

mergeIndex :: TreeIndex key val -> key -> TreeIndex key val -> TreeIndex key val
mergeIndex (TreeIndex leftKeys leftVals) middleKey
           (TreeIndex rightKeys rightVals) =
    TreeIndex
      (V.concat [leftKeys, V.singleton middleKey, rightKeys])
      (leftVals <> rightVals)

indexFromList :: Vector k -> Vector v -> TreeIndex k v
indexFromList keys valPtrs = TreeIndex keys valPtrs

singletonIndex :: v -> TreeIndex k v
singletonIndex = TreeIndex V.empty . V.singleton

fromSingletonIndex :: TreeIndex k v -> Maybe v
fromSingletonIndex (TreeIndex _keys vals) =
    if V.length vals == 1 then Just $! V.head vals else Nothing

indexNumKeys :: TreeIndex k v -> Int
indexNumKeys (TreeIndex keys _vals) = V.length keys

indexNumVals :: TreeIndex k v -> Int
indexNumVals (TreeIndex _keys vals) = V.length vals

splitIndexAt :: Int -> TreeIndex k v -> (TreeIndex k v, k, TreeIndex k v)
splitIndexAt numLeftKeys (TreeIndex keys vals)
    | (leftKeys, middleKeyAndRightKeys) <- V.splitAt numLeftKeys     keys
    , (leftVals, rightVals)             <- V.splitAt (numLeftKeys+1) vals
    = case V.uncons middleKeyAndRightKeys of
        Just (middleKey, rightKeys) ->
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

    go index
      | numVals <= maxIdxVals =
          singletonIndex $ mkNode index hhEmpty

      | numVals <= 2 * maxIdxVals =
          let (leftIndex, middleKey, rightIndex) =
                splitIndexAt (div numVals 2 - 1) index
          in indexFromList (V.singleton middleKey)
                           (V.fromList [mkNode leftIndex hhEmpty,
                                        mkNode rightIndex hhEmpty])

      | otherwise =
          let (leftIndex, middleKey, rightIndex) =
                splitIndexAt maxIdxKeys index
          in mergeIndex (singletonIndex (mkNode leftIndex hhEmpty))
                        middleKey
                        (go rightIndex)
      where
        numVals = indexNumVals index

-- ----------------------------------------------------------------------------

vecUncons :: Vector a -> Maybe (a, Vector a)
vecUncons v
    | V.null v  = Nothing
    | otherwise = Just (V.unsafeHead v, V.unsafeTail v)

vecUnsnoc :: Vector a -> Maybe (Vector a, a)
vecUnsnoc v
    | V.null v  = Nothing
    | otherwise = Just (V.unsafeInit v, V.unsafeLast v)

-- A TreeIndex with a hole in it.
--
data IndexContext k a = IndexContext {
  leftKeys  :: Vector k,
  leftVals  :: Vector a,
  rightKeys :: Vector k,
  rightVals :: Vector a
  }

-- Like splitIndexAt, but instead breaks on where a given key should be.
valView :: Ord k
        => k
        -> TreeIndex k a
        -> (IndexContext k a, a)
valView key (TreeIndex keys vals)
    | (leftKeys, rightKeys)       <- V.span (<=key) keys
    , n                           <- V.length leftKeys
    , (leftVals, valAndRightVals) <- V.splitAt n vals
    , Just (val, rightVals)       <- vecUncons valAndRightVals
    = ( IndexContext{..}, val )
    | otherwise
    = error "valView: cannot split an empty index"


leftView :: IndexContext key val -> Maybe (IndexContext key val, val, key)
leftView ctx = do
  (leftVals, leftVal) <- vecUnsnoc (leftVals ctx)
  (leftKeys, leftKey) <- vecUnsnoc (leftKeys ctx)
  pure (ctx { leftKeys = leftKeys
            , leftVals = leftVals
            }, leftVal, leftKey)

rightView :: IndexContext key val -> Maybe (key, val, IndexContext key val)
rightView ctx = do
  (rightVal, rightVals) <- vecUncons (rightVals ctx)
  (rightKey, rightKeys) <- vecUncons (rightKeys ctx)
  pure (rightKey, rightVal,
         ctx { rightKeys = rightKeys
             , rightVals = rightVals
             })

putVal :: IndexContext key val -> val -> TreeIndex key val
putVal ctx val =
  TreeIndex
    (leftKeys ctx <> rightKeys ctx)
    (leftVals ctx <> V.singleton val <> rightVals ctx)

putIdx :: IndexContext key val -> TreeIndex key val -> TreeIndex key val
putIdx ctx (TreeIndex keys vals) =
  TreeIndex
    (leftKeys ctx <> keys <> rightKeys ctx)
    (leftVals ctx <> vals <> rightVals ctx)

