module Main (main) where

import           Control.Monad             (forM, join)
import           Control.Monad.State       (MonadState, StateT, evalStateT,
                                            execState, get, gets, modify', put,
                                            runStateT)
import           Data.Aeson
import           Data.Maybe
import           Debug.Trace
import           Optics                    hiding ((%%))
import           System.Directory
import           System.Environment
import           System.FilePath

--import System.Console.Repline
import           Data.BTree.Primitives.Key
import           Types

import qualified Data.ByteString.Lazy      as BS

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

instance FromJSON Item where
  parseJSON (Object v) =
    Item <$> v .: "id"
         <*> v .: "tags"
  parseJSON _ = fail "not an object"

-- The "images" container in the derpibooru
data ImagesJSON = ImagesJSON [Item]
  deriving (Show)

instance FromJSON ImagesJSON where
  parseJSON (Object v) = (v .: "images") >>= fmap ImagesJSON . parseJSON
  parseJSON _          = fail "not an object"


-- The entire world.
data Model = Model {
  items :: HitchhikerMap Int Item,
  tags  :: HitchhikerSetMap String Int
  }

makeFieldLabelsNoPrefix ''Model

emptyModel = Model (HM.empty largeConfig) (SM.empty largeConfig)

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

-- OK, we're going to parse the

main = do
  -- Load the json into a series of Items
  [indir] <- getArgs
  files :: [FilePath] <- listDirectory indir

  allImages <- forM files $ \file -> do
    bs <- BS.readFile (indir </> file)
    case eitherDecode bs of
      Left err                 -> do { putStrLn err; pure [] }
      Right (ImagesJSON items) -> pure items

  flip evalStateT emptyModel $ do
    mapM addEntry (join allImages)

    s <- search ["twilight sparkle", "cute"]
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
