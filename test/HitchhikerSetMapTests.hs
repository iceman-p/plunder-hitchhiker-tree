module HitchhikerSetMapTests (tests) where

import           ClassyPrelude

import           Control.DeepSeq
import           System.Random
import           System.Random.Shuffle (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Types
import           Utils

import qualified Data.Map              as M
import qualified Data.Set              as Set
import qualified Data.SetMap           as S
import qualified HitchhikerSet         as HS
import qualified HitchhikerSetMap      as SM

data TestPair = TestPair Int String
  deriving Show

toTup :: TestPair -> (Int, String)
toTup (TestPair i s) = (i, s)

instance Arbitrary TestPair where
  arbitrary = TestPair <$> choose (0, 256) <*> (listOf $ elements ['a'..'z'])

-- TODO: This doesn't compile yet. Make it.
prop_setmap_fulltree_eq :: [TestPair] -> Bool
prop_setmap_fulltree_eq raw =
  go raw (SM.empty twoThreeConfig) (S.empty)
  where
    go ((TestPair k v):xs) ft m =
      let ft' = force $ SM.insert k v ft
      in go xs ft' (S.insert k v m)
    go [] ft m = case doShuffle raw of
      []                 -> True -- empty list, ok.
      ((TestPair k _):_) ->
        (HS.toSet $ SM.lookup k ft) == S.lookup k m

-- Mass insert results in an equivalent map.
prop_map_insertmany_eq :: [TestPair] -> Bool
prop_map_insertmany_eq raw = go raw (S.empty)
  where
    go ((TestPair k v):xs) m =
      go xs (S.insert k v m)
    go [] m = case doShuffle raw of
      [] -> True -- empty list, ok.
      ((TestPair k _): _) ->
        let hm = SM.insertMany asItems emp
        in (HS.toSet $ SM.lookup k hm) == S.lookup k m

    emp = SM.empty twoThreeConfig
    asItems = map toTup raw

-- prop_setmap_delete_eq :: [TestPair] -> Bool
-- prop_setmap_delete_eq raw =
--   go raw (SM.empty twoThreeConfig) (S.empty)
--   where
--     go ((TestPair k v):xs) ft m =
--       let ft' = force $ SM.insert k v ft
--       in go xs ft' (S.insert k v m)
--     go [] ft m = case doShuffle raw of
--       []                 -> True -- empty list, ok.
--       ((TestPair k v):_) ->
--         let dft = SM.delete k v ft
--             dm = doUpstreamSetMapDelete k v m
--             isGone = not $ HS.member v $ SM.lookup k dft
--         in isGone && all (exists dft) (mapSetToList $ S.toMap dm)

--     exists dft (k, v) = HS.member v $ SM.lookup k dft


doShuffle :: [a] -> [a]
doShuffle [] = []
doShuffle i  = shuffle' i (length i) (mkStdGen 999999)

-- Works around Data.SetMap having a setmap valid `insert` but a normal map
-- `delete`: you can't delete individual values, only all values for a key.
doUpstreamSetMapDelete :: (Ord k, Ord v)
                       => k -> v -> S.SetMap k v -> S.SetMap k v
doUpstreamSetMapDelete k v s =
  let valSet = S.lookup k s
      readd = Set.delete v valSet
      del = S.delete k s
  in Set.foldl (\s v -> S.insert k v s) del readd


prop_setmap_take_antitone :: [TestPair] -> Bool
prop_setmap_take_antitone raw = go raw (S.empty)
  where
    go ((TestPair k v):xs) m =
      go xs (S.insert k v m)
    go [] m = case doShuffle raw of
      [] -> True -- empty list, ok.
      ((TestPair k _): _) ->
        let hm = SM.insertMany asItems emp
            !smA = HS.toSet $ SM.toKeySet $ SM.takeWhileAntitone (< k) hm
            sA = M.keysSet $ M.takeWhileAntitone (< k) $ S.toMap m
        in smA == sA

    emp = SM.empty twoThreeConfig
    asItems = map toTup raw


tests :: TestTree
tests =
  testGroup "HitchhikerSetMap" [
    testProperty "Lookup same as setmap" prop_setmap_fulltree_eq,
    testProperty "Mass insert equivalence" prop_map_insertmany_eq,
    -- testProperty "Delete equivalence" prop_setmap_delete_eq

    testProperty "Taken antitone equivalence" prop_setmap_take_antitone
    ]
