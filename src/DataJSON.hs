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

import           Text.Pretty.Simple

-- | Converts a time string to the number of seconds since the epoch.
timeStringToEpoch :: String -> Int
timeStringToEpoch timeStr =
    let format = "%Y-%m-%dT%H:%M:%SZ"
        utcTime = parseTimeOrError True defaultTimeLocale format timeStr :: UTCTime
        epochTime = UTCTime (fromGregorian 1970 1 1) 0
        secondsSinceEpoch = floor $ utcTime `diffUTCTime` epochTime
    in secondsSinceEpoch

-- A single row of data from the
data Item = Item { idNum       :: !Int,
                   tags        :: ![String],
                   thumb       :: !String,
                   img         :: !String,

                   downvotes   :: !Int,
                   upvotes     :: !Int,
                   score       :: !Int,
                   faves       :: !Int,

                   description :: !String,

                   firstSeenAt :: !String,
                   updatedAt   :: !String,

                   width       :: !Int,
                   height      :: !Int
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

-- We have to rework everything here to be built around

data Base = BASE {
  database        :: !Database,

  -- Stuff some meta attributes here.
  derpIdAttr      :: EntityId,
  derpTagsAttr    :: EntityId,
  derpThumbAttr   :: EntityId,
  derpImgAttr     :: EntityId,
  derpUpvotesAttr :: EntityId
  }
  deriving (Generic, NFData)




emptyBase =
  let database =
        learnAttribute ":derp/tags" True MANY VT_STR $
        learnAttribute ":derp/upvotes" False ONE VT_INT $
        learnAttribute ":derp/id" True ONE VT_INT $
        learnAttribute ":derp/thumbURL" False ONE VT_STR $
        learnAttribute ":derp/imgURL" False ONE VT_STR $
        emptyDB
      getAttr x = findWithDefault (ENTID 9999999) x (database.attributes)
  in BASE {
    database,
    derpIdAttr = getAttr ":derp/id",
    derpTagsAttr = getAttr ":derp/tags",
    derpThumbAttr = getAttr ":derp/thumbURL",
    derpImgAttr = getAttr ":derp/imgURL",
    derpUpvotesAttr = getAttr ":derp/upvotes"
    }

mkDatoms :: Item -> EntityRef -> Base
         -> [(EntityRef, EntityId, Query.Types.Value, Bool)]
mkDatoms !item eid
         BASE{derpTagsAttr,derpIdAttr,derpThumbAttr,derpImgAttr,
              derpUpvotesAttr} =
  let fillRow (a, b) = (eid, a, b, True)
      tags = map (\a -> (derpTagsAttr, VAL_STR a)) item.tags
  in map fillRow $ tags ++ [
    (derpIdAttr, VAL_INT $ item.idNum),
    (derpThumbAttr, VAL_STR $ item.thumb),
    (derpImgAttr, VAL_STR $ item.img),
    -- (":derp/downvotes", VAL_INT $ item.downvotes),
    (derpUpvotesAttr, VAL_INT $ item.upvotes)
    -- (":derp/score", VAL_INT $ item.score),
    -- (":derp/faves", VAL_INT $ item.faves),
    -- (":derp/description", VAL_STR $ item.description),
    -- (":derp/firstSeenAt", VAL_INT $ timeStringToEpoch item.firstSeenAt),
    -- (":derp/updatedAt", VAL_INT $ timeStringToEpoch item.updatedAt),
    -- (":derp/width", VAL_INT $ item.width),
    -- (":derp/height", VAL_INT $ item.height)
    ]

addEntry :: MonadIO m => Item -> StateT Base m ()
addEntry item = do
  b@BASE{..} <- get

  let datoms = mkDatoms item (TMPREF 0) b
  -- pPrint "D: "
  -- pPrint datoms

  put b { database = learns datoms database}

addEntries :: MonadIO m => [Item] -> StateT Base m ()
addEntries items = do
  b@BASE{..} <- get

  let datoms = concat
             $ map (\(id, item) -> mkDatoms item id b)
             $ zip (map TMPREF [0..]) items
  -- pPrint "D: "
  -- pPrint datoms

  put b { database = learns datoms database}
