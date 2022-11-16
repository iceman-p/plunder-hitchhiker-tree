module PublishTree where

import           ClassyPrelude

import           Control.Monad.State.Strict (State, evalState, get, modify',
                                             runState)

import           Impl.Types
import           Types

import qualified Data.Map                   as M

toPublishTree :: (Hashable k, Hashable v)
              => HitchhikerMap k v
              -> PublishTree k v
toPublishTree (HITCHHIKERMAP _ Nothing)     = (PUBLISHTREE Nothing mempty)
toPublishTree (HITCHHIKERMAP _ (Just root)) = PUBLISHTREE (Just ptRoot) storage
  where
    (ptRoot, storage) = runState (gather root) M.empty

    gather = \case
      HitchhikerMapNodeIndex (TreeIndex keys vals) hh -> do
         hashvals <- mapM gather vals
         hashAdd $ PublishNodeIndex (TreeIndex keys hashvals) hh
      HitchhikerMapNodeLeaf lv -> hashAdd (PublishNodeLeaf lv)

    hashAdd pub = do
      let h = hash pub
      modify' (M.insert h pub)
      pure h

