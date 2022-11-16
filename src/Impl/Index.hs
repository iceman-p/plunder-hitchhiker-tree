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

-- Given a pure index with no hitchhikers, create a node.
extendIndex :: TreeFun k a hh lt
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
