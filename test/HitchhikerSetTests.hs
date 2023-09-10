module HitchhikerSetTests (tests) where

import           ClassyPrelude

import           Control.DeepSeq
import           System.Random
import           System.Random.Shuffle  (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Types

import           Data.List              (foldl1)

import qualified Data.Set               as S
import qualified HitchhikerSet          as H

import qualified MultiIntersectV2Vector as MIV
import qualified MultiIntersectV3Naive  as MIN

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

prop_set_delete_eq :: [Int] -> Bool
prop_set_delete_eq raw = go raw (H.empty twoThreeConfig) (S.empty)
  where
    go (x:xs) ft m =
      let ft' = force $ H.insert x ft
      in go xs ft' (S.insert x m)
    go [] ft m = case doShuffle raw of
      []    -> True -- empty list, ok.
      (k:_) ->
        let dft = H.delete k ft
            dm = S.delete k m
            isGone = not $ H.member k dft
        in isGone && all (exists dft) (S.toList dm)

    exists dft k = H.member k dft

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

prop_naive_set_intersection_eq :: [Int] -> [Int] -> Bool
prop_naive_set_intersection_eq raw1 raw2 =
  eq (mkHHSet raw1) (mkHHSet raw2) (S.fromList raw1) (S.fromList raw2)
  where
    eq hh1 hh2 s1 s2 = (unionsets (MIN.naiveIntersection hh1 hh2)) ==
                       (S.intersection s1 s2)

    unionsets = foldr S.union S.empty

    mkHHSet = go (H.empty twoThreeConfig)
      where
        go h []     = h
        go h (k:ks) = go (H.insert k h) ks

prop_set_multiintersection_eq :: [Int] -> [Int] -> [Int] -> [Int] -> Bool
prop_set_multiintersection_eq raw1 raw2 raw3 raw4 =
  eq (mkHHSet raw1) (mkHHSet raw2) (mkHHSet raw3) (mkHHSet raw4)
     (S.fromList raw1) (S.fromList raw2) (S.fromList raw3) (S.fromList raw4)
  where
    eq hh1 hh2 hh3 hh4 s1 s2 s3 s4 =
      let nu = unionall $ (MIV.nuIntersect [hh1, hh2, hh3, hh4])
          si = S.intersection s4 $ S.intersection s3 $ S.intersection s2 s1
      in nu == si

    -- The output of nuIntersect is a list of non-overlapping sets. To compare
    -- to S.intersection, we must union them together.
    unionall :: Ord a => [S.Set a] -> S.Set a
    unionall [] = S.empty
    unionall xs = foldl1 S.union xs

    mkHHSet = go (H.empty twoThreeConfig)
      where
        go h []     = h
        go h (k:ks) = go (H.insert k h) ks

tests :: TestTree
tests =
  testGroup "HitchhikerSet" [
    testProperty "Insert same as set" prop_set_insert_eq,
    testProperty "InsertMany same as set" prop_set_insertMany_eq,
    testProperty "Delete same as set" prop_set_delete_eq,
    localOption (QuickCheckTests 5000) $
      testProperty "Same intersection" prop_set_intersection_eq,
    localOption (QuickCheckTests 5000) $
      testProperty "New multi intersection v2" prop_set_multiintersection_eq,
    testProperty "Naive intersection v3" prop_naive_set_intersection_eq
    ]
