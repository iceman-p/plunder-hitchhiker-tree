module PartialTreeTests (tests) where

import           PartialTree
import           Types
import           Utils

import           Test.Tasty
import           Test.Tasty.HUnit

import qualified Data.Sequence    as Q

tests :: TestTree
tests =
  testGroup "PartialTree" [
    testCase "removeGreaterThan leftmost" $ removeGTLeftmost,
    testCase "removeGreaterThan first" $ removeGTFirst,
    testCase "removeGreaterThan none" $ removeGTNone,

    testCase "removeLessThan rightmost" $ removeLTRightmost,
    testCase "removeLessThan first" $ removeLTFirst,
    testCase "removeLessThan none" $ removeLTNone
    ]

indexFromList :: [k] -> [v] -> Index k v
indexFromList ks vs = Index (Q.fromList ks) (Q.fromList vs)

-- Sample one level index for quick testing.
largeIndex :: Index Int Int
largeIndex = indexFromList [5, 7, 10] [1, 2, 3, 4]

removeGTLeftmost = removeGreaterThan 2 largeIndex @?= indexFromList [] [1]
removeGTFirst = removeGreaterThan 6 largeIndex @?= indexFromList [5] [1,2]
removeGTNone = removeGreaterThan 12 largeIndex @?= largeIndex

removeLTRightmost = removeLessThan 11 largeIndex @?= indexFromList [] [4]
removeLTFirst = removeLessThan 8 largeIndex @?= indexFromList [10] [3, 4]
removeLTNone = removeLessThan 3 largeIndex @?= largeIndex
