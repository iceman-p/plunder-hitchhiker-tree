module Main (main) where

import           Data.Maybe
import           Debug.Trace

import           Types

import           Data.BTree.Primitives.Key

import qualified Data.BTree.Pure           as HB
import qualified Data.BTree.Pure.Setup     as HB

import qualified HitchhikerMap             as H
import qualified PublishTree               as P
import qualified SubscriberTree            as S

import qualified Data.Map                  as M

recurseLookup :: (Ord k, Show k, Show v)
              => k -> PublishTree k v -> S.SubscriberTree k v -> Maybe v
recurseLookup k pt st = let lu = S.lookup k st in
  trace ("Lookup: " ++ show lu) $
  case lu of
    Left fr@(loc, hash) -> case M.lookup hash (storage pt) of
      Nothing -> error "Missing node in full tree. Desynced. Impossible?"
      Just tn -> recurseLookup k pt $ S.fetched fr tn st
    Right result -> result

main :: IO ()
main = do
  let srcTree :: (HitchhikerMap Int String) =
        H.insert 7 "seven" $ H.insert 2 "one" $ H.insert 4 "four" $
        H.insert 10 "ten" $ H.insert 1 "ah" $ H.insert 9 "nine" $
        H.insert 5 "hello" $ H.empty twoThreeConfig

  traceM ("Full srctree: " ++ show srcTree)

  let lookupKey = 4

  traceM ("src lookup: " ++ (show $ H.lookup lookupKey srcTree))

  let pubTree = P.toPublishTree srcTree
  traceM ("pub: " ++ (show pubTree))

  let subTree :: (S.SubscriberTree Int String) =
        S.newSubscriberFromRoot $ publishRoot pubTree

  traceM ("Result:" ++ show (recurseLookup lookupKey pubTree subTree))

  pure ()
