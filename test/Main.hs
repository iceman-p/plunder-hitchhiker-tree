module Main where

import           Test.Tasty
import           Test.Tasty.TH

import qualified PartialTreeTests

main :: IO ()
main = defaultMain $
  testGroup "Tree"
  [ PartialTreeTests.tests ]
