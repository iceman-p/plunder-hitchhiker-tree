module HitchhikerSetTests (tests) where

import           Control.DeepSeq
import           Debug.Trace
import           System.Random
import           System.Random.Shuffle (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Types

import qualified Data.Set              as S
import qualified HitchhikerSet         as H

-- What are we doing here? A quickcheck test that

prop_set_fulltree_eq :: [Int] -> Bool
prop_set_fulltree_eq raw = go raw (H.empty twoThreeConfig) (S.empty)
  where
    go (x:xs) ft m =
      let ft' = force $ H.insert x ft
      in go xs ft' (S.insert x m)
    go [] ft m = case doShuffle raw of
      []    -> True -- empty list, ok.
      (k:_) -> H.member k ft == S.member k m

    doShuffle :: [a] -> [a]
    doShuffle [] = []
    doShuffle i  = shuffle' i (length i) (mkStdGen 999999)


tests :: TestTree
tests =
  testGroup "HitchhikerSet" [
    testProperty "Lookup same as set" prop_set_fulltree_eq
    ]
