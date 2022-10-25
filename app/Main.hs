module Main (main) where

import           Control.Monad.State       (MonadState, StateT, evalStateT,
                                            execState, get, gets, modify', put,
                                            runStateT)
import           Data.Maybe
import           Debug.Trace
import           Optics                    hiding ((%%))

--import System.Console.Repline
import           Data.BTree.Primitives.Key
import           Types

import qualified Data.BTree.Pure           as HB
import qualified Data.BTree.Pure.Setup     as HB
import qualified Data.Sequence             as Q
import qualified Data.Set                  as S

import qualified HitchhikerMap             as HM
import qualified HitchhikerSet             as HS
import qualified HitchhikerSetMap          as SM

-- A single row of data.
data Item = Item { idNum :: Int, tags :: [String] {- , data :: ByteString -} }
  deriving (Show)

-- The entire world.
data Model = Model {
  items :: HitchhikerMap Int Item,
  tags  :: HitchhikerSetMap String Int
  }

makeFieldLabelsNoPrefix ''Model

emptyModel = Model (HM.empty twoThreeConfig) (SM.empty twoThreeConfig)

addEntry :: Monad m => Item -> StateT Model m ()
addEntry item@(Item idNum tags) = do
  modifying' #items (HM.insert idNum item)
  modifying' #tags (SM.insertMany (Q.fromList $ fmap (\t -> (t, idNum)) tags))

-- Searches for a given set of tags.
search :: Monad m => [String] -> StateT Model m (S.Set Int)
search tags = do
  tagMap <- use #tags
  let sets = map (\t -> HS.toSet $ SM.lookup t tagMap) tags
  case sets of
    []     -> pure $ S.empty
    x:[]   -> pure $ x
    (x:xs) -> pure $ foldl S.intersection x xs

main = do
  flip evalStateT emptyModel $ do
    addEntry $ Item 1 ["twilight sparkle", "safe"]
    addEntry $ Item 2 ["twilight sparkle", "questionable"]
    addEntry $ Item 3 ["twilight sparkle", "safe", "looking at you"]
    addEntry $ Item 4 ["twilight sparkle", "questionable", "looking at you"]
    addEntry $ Item 5 ["rainbow dash", "safe"]
    addEntry $ Item 6 ["rainbow dash", "questionable", "iwtcird"]
    addEntry $ Item 7 ["rainbow dash", "questionable", "looking at you"]

    s <- search ["questionable", "looking at you"]
    traceM $ show s

    (Model items tags) <- get
    traceM $ show items
    traceM $ show tags


{-

type Repl a = HaskelineT (StateT Model IO) a

cmd :: String -> Repl ()
cmd input = do
  let

repl :: IO ()
repl = flip evalStateT emptyModel $
  evalRepl (const $ pure ">>> ") cmd opts Nothing Nothing

main :: IO ()
main = loop emptyModel
  where
    loop
-}

-- main = putStrLn "todo"
