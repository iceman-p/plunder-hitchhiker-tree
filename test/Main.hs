module Main where

import           Test.Tasty
import           Test.Tasty.TH

import qualified HitchhikerTreeTests
import qualified UtilTests

main :: IO ()
main = defaultMain $
  testGroup "Tree"
  [ UtilTests.tests
  , HitchhikerTreeTests.tests
  ]
