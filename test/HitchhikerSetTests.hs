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

doShuffle :: [a] -> [a]
doShuffle [] = []
doShuffle i  = shuffle' i (length i) (mkStdGen 999999)

prop_set_insert_eq :: [Int] -> Bool
prop_set_insert_eq raw = go raw (H.empty twoThreeConfig) (S.empty)
  where
    go (x:xs) ft m =
      let ft' = force $ H.insert x ft
      in go xs ft' (S.insert x m)
    go [] ft m = case doShuffle raw of
      []    -> True -- empty list, ok.
      (k:_) -> H.member k ft == S.member k m

prop_set_insertMany_eq :: [Int] -> Bool
prop_set_insertMany_eq raw = case doShuffle raw of
  []    -> True -- empty list, ok.
  (k:_) -> H.member k ft == S.member k s
  where
    ft = H.insertMany (S.fromList raw) (H.empty twoThreeConfig)
    s = S.fromList raw

prop_set_intersection_eq :: [Int] -> [Int] -> Bool
prop_set_intersection_eq raw1 raw2 =
  eq (mkHHSet raw1) (mkHHSet raw2) (S.fromList raw1) (S.fromList raw2)
  where
    eq hh1 hh2 s1 s2 = (H.toSet (H.intersection hh1 hh2)) ==
                       (S.intersection s1 s2)

    mkHHSet = go (H.empty twoThreeConfig)
      where
        go h []     = h
        go h (k:ks) = go (H.insert k h) ks

tests :: TestTree
tests =
  testGroup "HitchhikerSet" [
    testProperty "Insert same as set" prop_set_insert_eq,
    testProperty "InsertMany same as set" prop_set_insertMany_eq,
    testProperty "Same intersection" prop_set_intersection_eq
    ]
