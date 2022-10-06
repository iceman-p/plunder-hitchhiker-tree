module Main (main) where

import           Data.Maybe
import           Debug.Trace

import           Types

import qualified HitchhikerTree as H
import qualified PartialTree    as P

import qualified Data.Map       as M

recurseLookup :: (Ord k, Show k, Show v)
              => k -> FullTree k v -> P.PartialTree k v -> Maybe v
recurseLookup k ft pt = let lu = P.lookup k pt in
  trace ("Lookup: " ++ show lu) $
  case lu of
    Left fr@(loc, hash) -> case M.lookup hash (storage ft) of
      Nothing -> error "Missing node in full tree. Desynced. Impossible?"
      Just tn -> recurseLookup k ft $ P.fetched fr tn pt
    Right result -> result

main :: IO ()
main = do
  let srcTree :: (FullTree Int String) = H.insert 7 "seven" $ H.insert 2 "one" $ H.insert 4 "four" $ H.insert 10 "ten" $ H.insert 1 "ah" $ H.insert 6 "yo" $ H.insert 5 "hello" $ H.empty H.twoThreeConfig

  traceM ("Full srctree: " ++ show srcTree)

  let dstTree :: (P.PartialTree Int String) = P.newPartialFromRoot $ fromJust $ root srcTree

  traceM ("Result:" ++ show (recurseLookup 4 srcTree dstTree))

  pure ()
