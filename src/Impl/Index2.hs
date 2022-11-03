module Impl.Index2 where

import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))

import           Impl.Types
import           Types
import           Utils

import qualified Data.Sequence as Q

emptyIndex :: Index k v
emptyIndex = Index mempty mempty

mergeIndex :: Index key val -> key -> Index key val -> Index key val
mergeIndex (Index leftKeys leftVals) middleKey (Index rightKeys rightVals) =
    Index
      (leftKeys <> Q.singleton middleKey <> rightKeys)
      (leftVals <> rightVals)

indexFromList :: Seq k -> Seq v -> Index k v
indexFromList keys valPtrs = Index keys valPtrs

singletonIndex :: v -> Index k v
singletonIndex = Index Q.empty . Q.singleton

fromSingletonIndex :: Index k v -> Maybe v
fromSingletonIndex (Index _keys vals) =
    if Q.length vals == 1 then Just $! qHeadUnsafe vals else Nothing

indexNumKeys :: Index k v -> Int
indexNumKeys (Index keys _vals) = Q.length keys

indexNumVals :: Index k v -> Int
indexNumVals (Index _keys vals) = Q.length vals

splitIndexAt :: Int -> Index k v -> (Index k v, k, Index k v)
splitIndexAt numLeftKeys (Index keys vals)
    | (leftKeys, middleKeyAndRightKeys) <- Q.splitAt numLeftKeys     keys
    , (leftVals, rightVals)             <- Q.splitAt (numLeftKeys+1) vals
    = case qUncons middleKeyAndRightKeys of
        Just (middleKey,rightKeys) ->
            (Index leftKeys leftVals, middleKey, Index rightKeys rightVals)
        Nothing -> error "splitIndex: cannot split an empty index"


-- Given a pure index with no hitchhikers, create a node.
extendIndex :: TreeFun2 k a hh lt
            -> Int
            -> Index k a
            -> Index k a
extendIndex tf@TreeFun2{..} maxIdxKeys = go
  where
    maxIdxVals = maxIdxKeys + 1

    go index
      | numVals <= maxIdxVals =
          singletonIndex $ mkNode index hhEmpty

      | numVals <= 2 * maxIdxVals =
          let (leftIndex, middleKey, rightIndex) =
                splitIndexAt (div numVals 2 - 1) index
          in indexFromList (Q.singleton middleKey)
                           (Q.fromList [mkNode leftIndex hhEmpty,
                                        mkNode rightIndex hhEmpty])

      | otherwise =
          let (leftIndex, middleKey, rightIndex) =
                splitIndexAt maxIdxKeys index
          in mergeIndex (singletonIndex (mkNode leftIndex hhEmpty))
                        middleKey
                        (go rightIndex)
      where
        numVals = indexNumVals index
