module HitchhikerMapTests (tests) where

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
import qualified HitchhikerMap         as H

data TestPair = TestPair Int String
  deriving Show

toTup :: TestPair -> (Int, String)
toTup (TestPair i s) = (i, s)

instance Arbitrary TestPair where
  arbitrary = TestPair <$> choose (0, 256) <*> (listOf $ elements ['a'..'z'])

prop_map_fulltree_eq :: [TestPair] -> Bool
prop_map_fulltree_eq raw = go raw (H.empty twoThreeConfig) (M.empty)
  where
    go ((TestPair k v):xs) ft m =
      let ft' = force $ H.insert k v ft
      in go xs ft' (M.insert k v m)
    go [] ft m = case doShuffle raw of
      []                 -> True -- empty list, ok.
      ((TestPair k _):_) -> H.lookup k ft == M.lookup k m

-- Mass insert results in an equivalent map.
prop_map_insertmany_eq :: [TestPair] -> Bool
prop_map_insertmany_eq raw = go raw (M.empty)
  where
    go ((TestPair k v):xs) m =
      go xs (M.insert k v m)
    go [] m = case doShuffle raw of
      [] -> True -- empty list, ok.
      ((TestPair k _): _) ->
        let hm = H.insertMany asItems emp
        in case (H.lookup k hm) == M.lookup k m of
          True  -> True
          False -> trace ("Raw: " ++ show asItems ++ "\nM: " ++ show hm ++
                          "\na=" ++ (show $ H.lookup k hm) ++ ", b=" ++
                          (show $ M.lookup k m)) False

    emp = H.empty twoThreeConfig
    asItems = Q.fromList $ map toTup raw

doShuffle :: [a] -> [a]
doShuffle [] = []
doShuffle i  = shuffle' i (length i) (mkStdGen 999999)

tests :: TestTree
tests =
  testGroup "HitchhikerMap" [
    testProperty "Lookup same as map" prop_map_fulltree_eq,
    testProperty "Mass insert equivalence" prop_map_insertmany_eq
    ]
