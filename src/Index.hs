module Index where

import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))

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
extendIndex :: Monoid h
            => Int
            -> (Index k a -> h -> a)
            -> Index k a
            -> Index k a
extendIndex maxIdxKeys mkNode = go
  where
    maxIdxVals = maxIdxKeys + 1

    go index
      | numVals <= maxIdxVals =
          singletonIndex $ mkNode index mempty

      | numVals <= 2 * maxIdxVals =
          let (leftIndex, middleKey, rightIndex) =
                splitIndexAt (div numVals 2 - 1) index
          in indexFromList (Q.singleton middleKey)
                           (Q.fromList [mkNode leftIndex mempty,
                                        mkNode rightIndex mempty])

      | otherwise =
          let (leftIndex, middleKey, rightIndex) =
                splitIndexAt maxIdxKeys index
          in mergeIndex (singletonIndex (mkNode leftIndex mempty))
                        middleKey
                        (go rightIndex)
      where
        numVals = indexNumVals index
