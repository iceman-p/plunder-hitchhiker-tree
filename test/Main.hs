module Main where

import           Test.Tasty
import           Test.Tasty.TH

import qualified HitchhikerMapTests
import qualified HitchhikerSetTests
import qualified SubscriberTreeTests
import qualified UtilTests

main :: IO ()
main = defaultMain $
  testGroup "Tree"
  [ UtilTests.tests
  , HitchhikerMapTests.tests
  , HitchhikerSetTests.tests
  , SubscriberTreeTests.tests
  ]
