module Index where

import           Data.Sequence (Seq (Empty, (:<|), (:|>)), (<|), (|>))

import           Types
import           Utils

import qualified Data.Sequence as Q

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
