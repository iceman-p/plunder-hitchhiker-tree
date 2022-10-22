module SubscriberTreeTests (tests) where

import           Data.Maybe
import           System.Random
import           System.Random.Shuffle (shuffle')
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck

import           PublishTree
import           Types

import qualified Data.Map              as M
import qualified HitchhikerMap         as H
import qualified SubscriberTree        as S

data TestPair = TestPair Int String
  deriving Show

instance Arbitrary TestPair where
  arbitrary = TestPair <$> choose (0, 256) <*> (listOf $ elements ['a'..'z'])

doShuffle :: [a] -> [a]
doShuffle [] = []
doShuffle i  = shuffle' i (length i) (mkStdGen 999999)

applyPairs :: [TestPair]
           -> HitchhikerMap Int String
           -> M.Map Int String
           -> (HitchhikerMap Int String, M.Map Int String)
applyPairs ((TestPair k v):xs) ft m =
  applyPairs xs (H.insert k v ft) (M.insert k v m)
applyPairs [] ft m = (ft, m)

-- Returns the result on success.
checkResult :: [TestPair]
            -> M.Map Int String
            -> PublishTree Int String
            -> S.SubscriberTree Int String
            -> Maybe (S.SubscriberTree Int String)
checkResult [] m pt st = Just st
checkResult (x@(TestPair k _):xs) m pt st =
  case S.lookup k st of
    Left fr@(loc, hash) -> case M.lookup hash (storage pt) of
      Nothing -> error "Missing node in full tree. Desynced."
      Just tn -> checkResult (x:xs) m pt $ S.fetched fr tn st
    Right result -> if result == M.lookup k m
                    then checkResult xs m pt st
                    else Nothing

filterChanges :: [TestPair] -> TestPair -> Bool
filterChanges [] _ = True
filterChanges ((TestPair k _):xs) tp@(TestPair tk _) =
  if k == tk then False
  else filterChanges xs tp

-------------------------------------------------------------------------------

prop_map_subscribe_pull :: [TestPair] -> Bool
prop_map_subscribe_pull raw =
  let (ft, m) = applyPairs raw (H.empty twoThreeConfig) M.empty
      pt = toPublishTree ft
  in case checkResult (take 4 $ doShuffle raw) m pt
                      (S.newSubscriberFromRoot $ publishRoot pt) of
       Nothing -> False
       Just _  -> True

prop_map_retain_after_update :: [TestPair] -> [TestPair] -> Bool
prop_map_retain_after_update one two = isJust $ do
  let (ft, m) = applyPairs one (H.empty twoThreeConfig) M.empty
      pt = toPublishTree ft

  let oneChecks = take 6 $ doShuffle one
  st <- checkResult oneChecks m pt $ S.newSubscriberFromRoot $ publishRoot pt

  let (ft2, m2) = applyPairs two ft m
      pt2 = toPublishTree ft2
      st2 = S.updateRoot (publishRoot pt2) st

  -- The subscription tree is updated, but step one is to try to rerun the
  -- checks of part one because we should already know them and we shouldn't
  -- have to go to the publishTree.
  --
  -- But we only do checks where part two has not overridden a value.
  let oneChecks' = filter (filterChanges two) oneChecks
  checkResult oneChecks' m2 pt2 st2

prop_map_fetch_new_after_update :: [TestPair] -> [TestPair] -> Bool
prop_map_fetch_new_after_update one two = isJust $ do
  let (ft, m) = applyPairs one (H.empty twoThreeConfig) M.empty
      pt = toPublishTree ft

  let oneChecks = take 4 $ doShuffle one
  st <- checkResult oneChecks m pt $ S.newSubscriberFromRoot $ publishRoot pt

  -- Applying the new values to the source publish tree, and then ensuring you
  -- can read them in the subscriber tree.
  let (ft2, m2) = applyPairs two ft m
      pt2 = toPublishTree ft2
      st2 = S.updateRoot (publishRoot pt2) st
      twoChecks = take 4 $ doShuffle two
  checkResult twoChecks m2 pt2 st2

tests :: TestTree
tests =
  testGroup "SubscriberTree" [
    testProperty "Synced properly" prop_map_subscribe_pull,
    testProperty "Retain knowledge after update" prop_map_retain_after_update,
    testProperty "Fetch new after update" prop_map_fetch_new_after_update
    ]
