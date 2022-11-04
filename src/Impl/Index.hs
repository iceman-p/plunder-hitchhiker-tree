module Impl.Index where

import           Data.Vector (Vector)

import           Impl.Types
import           Types
import           Utils

import qualified Data.Vector as V

emptyIndex :: Index k v
emptyIndex = Index mempty mempty

mergeIndex :: Index key val -> key -> Index key val -> Index key val
mergeIndex (Index leftKeys leftVals) middleKey (Index rightKeys rightVals) =
    Index
      (V.concat [leftKeys, V.singleton middleKey, rightKeys])
      (leftVals <> rightVals)

indexFromList :: Vector k -> Vector v -> Index k v
indexFromList keys valPtrs = Index keys valPtrs

singletonIndex :: v -> Index k v
singletonIndex = Index V.empty . V.singleton

fromSingletonIndex :: Index k v -> Maybe v
fromSingletonIndex (Index _keys vals) =
    if V.length vals == 1 then Just $! V.head vals else Nothing

indexNumKeys :: Index k v -> Int
indexNumKeys (Index keys _vals) = V.length keys

indexNumVals :: Index k v -> Int
indexNumVals (Index _keys vals) = V.length vals

splitIndexAt :: Int -> Index k v -> (Index k v, k, Index k v)
splitIndexAt numLeftKeys (Index keys vals)
    | (leftKeys, middleKeyAndRightKeys) <- V.splitAt numLeftKeys     keys
    , (leftVals, rightVals)             <- V.splitAt (numLeftKeys+1) vals
    = case V.uncons middleKeyAndRightKeys of
        Just (middleKey, rightKeys) ->
            (Index leftKeys leftVals, middleKey, Index rightKeys rightVals)
        Nothing -> error "splitIndex: cannot split an empty index"

-- Given a pure index with no hitchhikers, create a node.
extendIndex :: TreeFun k a hh lt
            -> Int
            -> Index k a
            -> Index k a
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
