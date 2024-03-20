module UtilTests (tests) where

import           ClassyPrelude

import           Impl.Types
import           Types
import           Utils

import           Test.Tasty
import           Test.Tasty.HUnit

import qualified Data.Vector      as V

tests :: TestTree
tests =
  testGroup "Util" [
    testCase "removeGreaterThan leftmost" $ removeGTLeftmost,
    testCase "removeGreaterThan first" $ removeGTFirst,
    testCase "removeGreaterThan none" $ removeGTNone,

    testCase "removeLessThan rightmost" $ removeLTRightmost,
    testCase "removeLessThan first" $ removeLTFirst,
    testCase "removeLessThan none" $ removeLTNone,

    testCase "findSubnodeByKey first" $ findSubnodeByKeyFirst,
    testCase "findSubnodeByKey mid" $ findSubnodeByKeyMid,
    testCase "findSubnodeByKey last" $ findSubnodeByKeyLast
    ]

indexFromList :: [k] -> [v] -> TreeIndex k v
indexFromList ks vs = TreeIndex (fromList ks) (fromList vs)

-- Sample one level index for quick testing.
largeIndex :: TreeIndex Int Int
largeIndex = indexFromList [5, 7, 10] [1, 2, 3, 4]

removeGTLeftmost = removeGreaterThan 2 largeIndex @?= indexFromList [] [1]
removeGTFirst = removeGreaterThan 6 largeIndex @?= indexFromList [5] [1,2]
removeGTNone = removeGreaterThan 12 largeIndex @?= largeIndex

removeLTRightmost = removeLessThan 11 largeIndex @?= indexFromList [] [4]
removeLTFirst = removeLessThan 8 largeIndex @?= indexFromList [10] [3, 4]
removeLTNone = removeLessThan 3 largeIndex @?= largeIndex



testIdx = indexFromList [179, 210] [[5, 10], [179, 200], [210, 300]]

findSubnodeByKeyFirst = findSubnodeByKey 5 testIdx @?= [5, 10]
findSubnodeByKeyMid = findSubnodeByKey 179 testIdx @?= [179, 200]
findSubnodeByKeyLast = findSubnodeByKey 215 testIdx @?= [210, 300]
