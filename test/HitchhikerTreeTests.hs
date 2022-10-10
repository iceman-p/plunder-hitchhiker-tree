module HitchhikerTreeTests (tests) where

import           Control.DeepSeq
import           Debug.Trace
import           System.Random
import           System.Random.Shuffle (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck

import qualified Data.Map              as M
import qualified HitchhikerTree        as H

-- What are we doing here? A quickcheck test that

data TestPair = TestPair Int String
  deriving Show

instance Arbitrary TestPair where
  arbitrary = TestPair <$> choose (0, 256) <*> (listOf $ elements ['a'..'z'])

prop_map_fulltree_eq :: [TestPair] -> Bool
prop_map_fulltree_eq raw = trace "-------------------- NEW TST" $ go raw (H.empty H.twoThreeConfig) (M.empty)
  where
    go ((TestPair k v):xs) ft m =
      let ft' = force $ H.insert k v ft
      in go xs ft' (M.insert k v m)
    go [] ft m = trace "Beginning random lookup..." $ case doShuffle raw of
      []                 -> True -- empty list, ok.
      -- todo: deal with single case.
      ((TestPair k _):_) -> trace "Final lookup: " $
        let hl = H.lookup k ft
        in trace ("h.lookup=" ++ show hl) (hl == M.lookup k m)


    doShuffle :: [a] -> [a]
    doShuffle [] = []
    doShuffle i  = shuffle' i (length i) (mkStdGen 999999)


tests :: TestTree
tests =
  testGroup "HitchhikerTree" [
    testProperty "Lookup same as map" prop_map_fulltree_eq
    ]
