module HitchhikerSetMapTests (tests) where

import           Control.DeepSeq
import           Debug.Trace
import           System.Random
import           System.Random.Shuffle (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Types

import qualified Data.Map              as M
import qualified Data.Sequence         as Q
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
    asItems = Q.fromList $ map toTup raw


doShuffle :: [a] -> [a]
doShuffle [] = []
doShuffle i  = shuffle' i (length i) (mkStdGen 999999)

tests :: TestTree
tests =
  testGroup "HitchhikerSetMap" [
    testProperty "Lookup same as setmap" prop_setmap_fulltree_eq,
    testProperty "Mass insert equivalence" prop_map_insertmany_eq
    ]
