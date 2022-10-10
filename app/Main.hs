module Main (main) where

import           Data.Maybe
import           Debug.Trace

import           Types

import           Data.BTree.Primitives.Key

import qualified Data.BTree.Pure           as HB
import qualified Data.BTree.Pure.Setup     as HB

import qualified HitchhikerTree            as H
import qualified PublishTree               as P
--import qualified PartialTree               as P

import qualified Data.Map                  as M

{-
recurseLookup :: (Ord k, Show k, Show v)
              => k -> FullTree k v -> P.PartialTree k v -> Maybe v
recurseLookup k ft pt = let lu = P.lookup k pt in
  trace ("Lookup: " ++ show lu) $
  case lu of
    Left fr@(loc, hash) -> case M.lookup hash (storage ft) of
      Nothing -> error "Missing node in full tree. Desynced. Impossible?"
      Just tn -> recurseLookup k ft $ P.fetched fr tn pt
    Right result -> result
-}

main :: IO ()
main = do
  let srcTree :: (HitchhikerTree Int String) =
        H.insert 7 "seven" $ H.insert 2 "one" $ H.insert 4 "four" $
        H.insert 10 "ten" $ H.insert 1 "ah" $ H.insert 9 "nine" $
        H.insert 5 "hello" $ H.empty H.twoThreeConfig

  traceM ("Full srctree: " ++ show srcTree)

  let lookupKey = 4

  traceM ("src lookup: " ++ (show $ H.lookup lookupKey srcTree))

  let pub = P.toPublishTree srcTree
  traceM ("pub: " ++ (show pub))

  -- let hbt :: (HB.Tree Integer String) =
  --       HB.insert 7 "seven" $ HB.insert 2 "one" $ HB.insert 4 "four" $
  --       HB.insert 10 "ten" $ HB.insert 1 "ah" $ HB.insert 9 "nine" $
  --       HB.insert 5 "hello" $ HB.empty HB.twoThreeSetup

  -- traceM ("hasky tree: " ++ (show hbt))

{-

  let dstTree :: (P.PartialTree Int String) =
        P.newPartialFromRoot $ fromJust $ root srcTree

  traceM ("Result:" ++ show (recurseLookup lookupKey srcTree dstTree))

  -- let src2Tree = H.insert
-}

  pure ()
