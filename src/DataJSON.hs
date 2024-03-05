-- The Query/Database version of ImgJSON
module DataJSON where

import           ClassyPrelude

import           Control.Monad              (fail)
import           Control.Monad.State        (MonadState, StateT, evalStateT,
                                             execState, get, gets, liftIO,
                                             modify', put, runStateT)
import           Data.Aeson                 hiding (parse)
import           Data.Time.Clock
import           Data.Time.Format
import           Optics                     hiding (noneOf, (%%))
import           System.Directory
import           System.Environment
import           System.FilePath
import           System.IO
import           Text.Parsec

import           Query.HitchhikerDatomStore
import           Query.Types


-- | Converts a time string to the number of seconds since the epoch.
timeStringToEpoch :: String -> Int
timeStringToEpoch timeStr =
    let format = "%Y-%m-%dT%H:%M:%SZ"
        utcTime = parseTimeOrError True defaultTimeLocale format timeStr :: UTCTime
        epochTime = UTCTime (fromGregorian 1970 1 1) 0
        secondsSinceEpoch = floor $ utcTime `diffUTCTime` epochTime
    in secondsSinceEpoch

-- A single row of data from the
data Item = Item { idNum       :: Int,
                   tags        :: [String],
                   thumb       :: String,
                   img         :: String,

                   downvotes   :: Int,
                   upvotes     :: Int,
                   score       :: Int,
                   faves       :: Int,

                   description :: String,

                   firstSeenAt :: String,
                   updatedAt   :: String,

                   width       :: Int,
                   height      :: Int
                 }
  deriving (Show)

instance FromJSON Item where
  parseJSON (Object v) = do
    idNum <- v .: "id"
    tags <- v .: "tags"
    rep <- v .: "representations"
    thumb <- rep .: "thumb"
    img <- rep .: "medium"

    downvotes <- v .: "downvotes"
    upvotes <- v .: "upvotes"
    score <- v .: "score"
    faves <- v .: "faves"
    description <- v .: "description"
    firstSeenAt <- v .: "first_seen_at"
    updatedAt <- v .: "updated_at"
    width <- v .: "width"
    height <- v .: "height"
    pure Item{..}
  parseJSON _ = fail "not an object"

-- The "images" container in the derpibooru
data ImagesJSON = ImagesJSON [Item]
  deriving (Show)

instance FromJSON ImagesJSON where
  parseJSON (Object v) = (v .: "images") >>= fmap ImagesJSON . parseJSON
  parseJSON _          = fail "not an object"


data Base = BASE {
  nextEid  :: Int,
  nextTx   :: Int,
  database :: !Database
  }
  deriving (Generic, NFData)

emptyBase = BASE {
  nextEid = 1,
  nextTx = 2^16 + 1,
  database = emptyDB
  }


-- mkDatoms :: Item -> Int -> Int -> [(Value, Value, Value, Int, Bool)]
mkDatoms item rawEid tx =
  let eid = VAL_ENTID $ ENTID rawEid
      fillRow (a, b) = (eid, VAL_ATTR $ ATTR a, b, tx, True)
      tags = map (\a -> (":derp/tags", VAL_STR a)) item.tags
  in map fillRow $ tags ++ [
    (":derp/id", VAL_INT $ item.idNum),
    (":derp/thumbURL", VAL_STR $ item.thumb),
    (":derp/imgURL", VAL_STR $ item.img),
    (":derp/downvotes", VAL_INT $ item.downvotes),
    (":derp/upvotes", VAL_INT $ item.upvotes),
    (":derp/score", VAL_INT $ item.score),
    (":derp/faves", VAL_INT $ item.faves),
    (":derp/description", VAL_STR $ item.description),
    (":derp/firstSeenAt", VAL_INT $ timeStringToEpoch item.firstSeenAt),
    (":derp/updatedAt", VAL_INT $ timeStringToEpoch item.updatedAt),
    (":derp/width", VAL_INT $ item.width),
    (":derp/height", VAL_INT $ item.height)
    ]

addEntry :: Monad m => Item -> StateT Base m ()
addEntry item = do
  (BASE eid tx db) <- get

  let datoms = mkDatoms item eid tx
--  traceM $ "D: " <> show datoms

  put BASE{ nextEid = eid + 1
          , nextTx = tx + 1
          , database = learns datoms db}

-- TODO: Continue here, I'm done for the day.
