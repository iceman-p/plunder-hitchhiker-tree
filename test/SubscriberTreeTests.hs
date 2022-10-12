module SubscriberTreeTests (tests) where

import           Data.Maybe
import           Debug.Trace
import           System.Random
import           System.Random.Shuffle (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck

import           PublishTree
import           Types

import qualified Data.Map              as M
import qualified HitchhikerTree        as H
import qualified SubscriberTree        as S

data TestPair = TestPair Int String
  deriving Show

instance Arbitrary TestPair where
  arbitrary = TestPair <$> choose (0, 256) <*> (listOf $ elements ['a'..'z'])

prop_map_subscribe_pull :: [TestPair] -> Bool
prop_map_subscribe_pull raw = go raw (H.empty H.twoThreeConfig) M.empty
  where
    go ((TestPair k v):xs) ft m = go xs (H.insert k v ft) (M.insert k v m)
    go [] ft m = let pt = toPublishTree ft
                 in check (take 4 $ doShuffle raw) m pt
                          (S.newSubscriberFromRoot $ fromJust $ publishRoot pt)

    check :: [TestPair]
          -> M.Map Int String
          -> PublishTree Int String
          -> S.SubscriberTree Int String
          -> Bool
    check [] m pt st = True
    check (x@(TestPair k _):xs) m pt st = case S.lookup k st of
      Left fr@(loc, hash) -> case M.lookup hash (storage pt) of
        Nothing -> error "Missing node in full tree. Desynced."
        Just tn -> check (x:xs) m pt $ S.fetched fr tn st
      Right result -> if result == M.lookup k m
                      then check xs m pt st
                      else False

    doShuffle [] = []
    doShuffle i  = shuffle' i (length i) (mkStdGen 999999)

tests :: TestTree
tests =
  testGroup "SubscriberTree" [
    testProperty "Synced properly" prop_map_subscribe_pull
    ]
