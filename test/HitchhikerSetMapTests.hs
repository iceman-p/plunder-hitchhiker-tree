module HitchhikerSetMapTests (tests) where

import           Control.DeepSeq
import           Debug.Trace
import           System.Random
import           System.Random.Shuffle (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Types

import qualified Data.SetMap           as S
import qualified HitchhikerSet         as HS
import qualified HitchhikerSetMap      as SM

data TestPair = TestPair Int String
  deriving Show

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

    doShuffle :: [a] -> [a]
    doShuffle [] = []
    doShuffle i  = shuffle' i (length i) (mkStdGen 999999)

tests :: TestTree
tests =
  testGroup "HitchhikerSetMap" [
    testProperty "Lookup same as setmap" prop_setmap_fulltree_eq
    ]
