module ImgJSON where

import           Control.DeepSeq
import           Control.Monad.State       (MonadState, StateT, evalStateT,
                                            execState, get, gets, liftIO,
                                            modify', put, runStateT)
import           Data.Aeson                hiding (parse)
import           Data.Maybe
import           Optics                    hiding (noneOf, (%%))
import           System.Directory
import           System.Environment
import           System.FilePath
import           System.IO
import           Text.Parsec

import           Data.BTree.Primitives.Key
import           Types

import qualified Data.ByteString.Lazy      as BS

import qualified Data.BTree.Pure           as HB
import qualified Data.BTree.Pure.Setup     as HB
import qualified Data.Set                  as S

import qualified HitchhikerMap             as HM
import qualified HitchhikerSet             as HS
import qualified HitchhikerSetMap          as SM


-- A single row of data.
data Item = Item { idNum :: Int,
                   tags  :: [String],
                   thumb :: String,
                   img   :: String
                 }
  deriving (Show)

instance FromJSON Item where
  parseJSON (Object v) = do
    i <- v .: "id"
    t <- v .: "tags"
    rep <- v .: "representation"
    thumb <- rep .: "thumb"
    medium <- rep .: "medium"
    pure $ Item i t thumb medium
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
addEntry !item@(Item idNum tags _ _) = do
  modifying' #items (HM.insert idNum item)
  modifying' #tags $
    (SM.insertMany (force $ fmap (\t -> (t, idNum)) tags))

-- Searches for a given set of tags.
search :: Monad m => [String] -> StateT Model m (HitchhikerSet Int)
search tags = do
  tagMap <- use #tags
  let sets = map (\t -> SM.lookup t tagMap) tags
  case sets of
    []     -> pure $ HS.empty largeConfig
    x:[]   -> pure $ x
    (x:xs) -> pure $ foldl HS.intersection x xs
