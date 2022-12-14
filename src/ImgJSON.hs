module ImgJSON where

import           ClassyPrelude

import           Control.Monad             (fail)
import           Control.Monad.State       (MonadState, StateT, evalStateT,
                                            execState, get, gets, liftIO,
                                            modify', put, runStateT)
import           Data.Aeson                hiding (parse)
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
    rep <- v .: "representations"
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
--
-- Returns either a list of tags that don't exist in the database, or the set
-- of images that match all those tags.
search :: Monad m
       => [String]
       -> StateT Model m (Either [String] (HitchhikerSet Int))
search tags = do
  tagMap <- use #tags
  let sets = map (lookupTag tagMap) tags
  pure $ case (lefts sets, rights sets) of
    ([], [])   -> Right $ HS.empty largeConfig
    ([], x:xs) -> Right $ foldl' HS.intersection x xs
    (lefts, _) -> Left lefts
  where
    lookupTag m t = let s = SM.lookup t m
                    in if HS.null s
                       then Left t
                       else Right s

type SearchResult = (Int, String)  -- imgid, thumbnail url

lookupItems :: Monad m
            => HitchhikerSet Int
            -> StateT Model m [SearchResult]
lookupItems s = do
  itemMap <- use #items
  pure $ catMaybes $ map (toSearchResult itemMap) $ S.toList $ HS.toSet s
  where
    toSearchResult im i = case HM.lookup i im of
      Nothing       -> Nothing
      Just Item{..} -> Just (idNum, thumb)
