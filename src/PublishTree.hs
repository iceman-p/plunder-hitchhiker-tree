module PublishTree where

import           Control.Monad.State.Strict (State, evalState, get, modify',
                                             runState)
import           Data.Hashable

import           Types

import qualified Data.Map                   as M

toPublishTree :: (Hashable k, Hashable v)
              => HitchhikerTree k v
              -> PublishTree k v
toPublishTree (HITCHHIKERTREE _ Nothing) = (PUBLISHTREE Nothing mempty)
toPublishTree (HITCHHIKERTREE _ (Just root)) = PUBLISHTREE (Just ptRoot) storage
  where
    (ptRoot, storage) = runState (gather root) M.empty

    gather = \case
      HitchhikerNodeIndex (Index keys vals) hh -> do
         hashvals <- mapM gather vals
         hashAdd $ PublishNodeIndex (Index keys hashvals) hh
      HitchhikerNodeLeaf lv -> hashAdd (PublishNodeLeaf lv)

    hashAdd pub = do
      let h = hash pub
      modify' (M.insert h pub)
      pure h
