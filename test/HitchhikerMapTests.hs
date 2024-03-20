module HitchhikerMapTests where -- (tests) where

import           ClassyPrelude

import           Control.DeepSeq
import           System.Random
import           System.Random.Shuffle (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Types

import           Data.Sorted.Set

import qualified Data.Map.Strict       as M
import qualified Data.Set              as S
import qualified HitchhikerMap         as HM
import qualified HitchhikerSet         as HS

data TestPair = TestPair Int String
  deriving Show

toTup :: TestPair -> (Int, String)
toTup (TestPair i s) = (i, s)

instance Arbitrary TestPair where
  arbitrary = TestPair <$> choose (0, 256) <*> (listOf $ elements ['a'..'z'])

prop_map_fulltree_eq :: [TestPair] -> Bool
prop_map_fulltree_eq raw = go raw (HM.empty twoThreeConfig) (M.empty)
  where
    go ((TestPair k v):xs) ft m =
      let ft' = force $ HM.insert k v ft
      in go xs ft' (M.insert k v m)
    go [] ft m = case doShuffle raw of
      []                 -> True -- empty list, ok.
      ((TestPair k _):_) ->
        case HM.lookup k ft == M.lookup k m of
          True  -> True
          False -> False
          -- False -> trace ("Raw: " ++ show raw ++ "\nM: " ++ show ft ++
          --                 "\na=" ++ (show $ HM.lookup k ft) ++ ", b=" ++
          --                 (show $ M.lookup k m)) False

-- Mass insert results in an equivalent map.
prop_map_insertmany_eq :: [TestPair] -> Bool
prop_map_insertmany_eq raw = go raw (M.empty)
  where
    go ((TestPair k v):xs) m =
      go xs (M.insert k v m)
    go [] m = case doShuffle raw of
      [] -> True -- empty list, ok.
      ((TestPair k _): _) ->
        let hm = HM.insertMany asItems emp
        in case (HM.lookup k hm) == M.lookup k m of
          True  -> True
          False -> False
          -- False -> trace ("Raw: " ++ show asItems ++ "\nM: " ++ show hm ++
          --                 "\na=" ++ (show $ HM.lookup k hm) ++ ", b=" ++
          --                 (show $ M.lookup k m)) False

    emp = HM.empty twoThreeConfig
    asItems = M.fromList $ map toTup raw

-- When we delete an item, test that it is both gone, and that we didn't delete
-- any other value. (Important because hitchhiker handling is hard.)
prop_map_delete_eq :: [TestPair] -> Bool
prop_map_delete_eq raw = go raw (HM.empty twoThreeConfig) (M.empty)
  where
    go ((TestPair k v):xs) ft m =
      let ft' = force $ HM.insert k v ft
      in go xs ft' (M.insert k v m)
    go [] ft m = case doShuffle raw of
      []                 -> True -- empty list, ok.
      ((TestPair k _):_) ->
        let dft = HM.delete k ft
            dm = M.delete k m
            isGone = HM.lookup k dft == Nothing
        in isGone && case all (exists dft) (M.toList dm) of
          True  -> True
          False -> False
          -- False -> trace ("Base: " ++ show ft ++ "\nDelete: " ++ show k ++
          --                 "\nRaw: " ++ (show $ M.toList dm) ++ "\nM: " ++
          --                 show dft)
          --                False

    exists dft (k, v) = HM.lookup k dft == Just v

-- OK. Actually test out that restrictKeys works equivalently as
prop_map_restrict_keys_eq :: [TestPair] -> [Int] -> Bool
prop_map_restrict_keys_eq rawPair rawSet = golden == testResult
  where
    haskellMap = M.fromList $ map toTup rawPair
    haskellSet = S.fromList rawSet
    golden = M.restrictKeys haskellMap haskellSet

    hhMap = HM.insertMany haskellMap $ HM.empty twoThreeConfig
    hhSet = HS.insertMany rawSet $ HS.empty twoThreeConfig
    hhRestricted = HM.restrictKeys hhSet hhMap
    testResult = M.fromList $ HM.toList hhRestricted

doShuffle :: [a] -> [a]
doShuffle [] = []
doShuffle i  = shuffle' i (length i) (mkStdGen 999999)

tests :: TestTree
tests =
  testGroup "HitchhikerMap" [
      testProperty "Lookup same as map" prop_map_fulltree_eq
    , testProperty "Mass insert equivalence" prop_map_insertmany_eq
    , testProperty "Delete means gone" prop_map_delete_eq
    , testProperty "Restrict Keys Equivalence" prop_map_restrict_keys_eq
    , testCase "Map handles duplicates" $
        (prop_map_fulltree_eq mapDupeBug @?= True)
    , testCase "Make sure don't over delete" $
        (prop_map_delete_eq mapDeleteBug @?= True)
    ]

-- -----------------------------------------------------------------------

mapDupeBug = [
  TestPair 169 "twwlbvkpjqgxbqekhteqaqafihjvyiywmleod",
  TestPair 130 "qvothnobtcfsiyxbbnjnjtpodukk",
  TestPair 199 "nlwwxlueykws",
  TestPair 22 "xwxzpxoiebnwnftnhbtfxznrgljrnrdyuomtsbgteipjmrt",
  TestPair 101 "kvatcgdiejggvfxmh",
  TestPair 43 "qnkzmsvpdklawtyebpymrktyvtqayoxclzajjsfwjqkivt",
  TestPair 175 "qpfpbxqwjvhqwumdbajhipi",
  TestPair 96 "rdcfosuojdcqdloyeiwqiddwtoviqdfgeye",
  TestPair 249 "uloucneuw",
  TestPair 10 "yvfxgseqgl",
  TestPair 2 "fvkrqakhuucboeueewwezrqpxajjboetyar",
  TestPair 3 "vqavkidzo",
  TestPair 21 "yfmfjijiyvupzeqohnmifela",
  TestPair 234 "mxppfmhgomtmlxfftzflhutpcbiiqrb",
  TestPair 10 "kemghbtepygjdbmdragujiefxnqc",
  TestPair 219 "uuwzojkuyoltoxxatljtpxnenryovwmnhwvib",
  TestPair 1 "xxmvwzwcvshqcmmseibabzswxvlxfrdnstalb",
  TestPair 39 "ydomxduaedltoehodvdnw",
  TestPair 146 "skdmsgdh"
  ]
-- Duplicated key is 10.

mapDeleteBug = [
  TestPair 155 "dypbxhikgpzuomi",
  TestPair 91 "gxlvha",
  TestPair 109 "kzywg",
  TestPair 15 "rgbwwojveuyowkn",
  TestPair 14 "mklfeawekuzvlryi",
  TestPair 34 "",
  TestPair 148 "fcfthwcibmdbhxfiofp",
  TestPair 212 "qdbhmkdkaz",
  TestPair 201 "vtcgxvgmqsuyzsa",
  TestPair 155 "iydnrtn",
  TestPair 255 "jcjpcekiinvco",
  TestPair 247 "mghzuanvlutjmzimecabtpqkwo",
  TestPair 51 "cpnrsewbievadfluoehjssb",
  TestPair 199 "a",
  TestPair 190 "sbkkawh",
  TestPair 242 "rcul",
  TestPair 48 "fortrppffe",
  TestPair 253 "tthsbqhypwezqothedqumezuc",
  TestPair 180 ""
  ]
