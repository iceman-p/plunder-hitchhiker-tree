{-# LANGUAGE Strict     #-}
{-# LANGUAGE StrictData #-}
-- The Query/Database version of ImgJSON
module DataJSON where

import           ClassyPrelude

import           Control.Monad              (fail)
import           Control.Monad.State.Strict (MonadState, StateT, evalStateT,
                                             execState, get, gets, liftIO,
                                             modify', put, runStateT)
import           Data.Aeson                 hiding (parse)
import           Data.Time.Clock
import           Data.Time.Format
import           NoThunks.Class
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
timeStringToEpoch :: Text -> Int
timeStringToEpoch timeStr =
    let format = "%Y-%m-%dT%H:%M:%SZ"
        utcTime = parseTimeOrError True defaultTimeLocale format (unpack timeStr) :: UTCTime
        epochTime = UTCTime (fromGregorian 1970 1 1) 0
        secondsSinceEpoch = floor $ utcTime `diffUTCTime` epochTime
    in secondsSinceEpoch

-- A single row of data from the
data Item = Item { idNum       :: !Int,
                   tags        :: ![Text],
                   thumb       :: !Text,
                   img         :: !Text,

                   downvotes   :: !Int,
                   upvotes     :: !Int,
                   score       :: !Int,
                   faves       :: !Int,

                   description :: !Text,

                   firstSeenAt :: !Text,
                   updatedAt   :: !Text,

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
  database            :: !Database,

  -- Stuff some meta attributes here.
  derpIdAttr          :: EntityId,
  derpTagsAttr        :: EntityId,
  derpThumbAttr       :: EntityId,
  derpImgAttr         :: EntityId,
  derpUpvotesAttr     :: EntityId,
  derpDownvotesAttr   :: EntityId,
  derpScoreAttr       :: EntityId,
  derpDescriptionAttr :: EntityId,
  derpFirstSeenAtAttr :: EntityId,
  derpWidthAttr       :: EntityId,
  derpHeightAttr      :: EntityId
  }
  deriving (Generic, NFData)




emptyBase =
  let database =
        learnAttribute ":derp/tags" True MANY VT_STR $
        learnAttribute ":derp/upvotes" False ONE VT_INT $
        learnAttribute ":derp/downvotes" False ONE VT_INT $
        learnAttribute ":derp/score" False ONE VT_INT $
        learnAttribute ":derp/id" True ONE VT_INT $
        learnAttribute ":derp/thumbURL" False ONE VT_STR $
        learnAttribute ":derp/imgURL" False ONE VT_STR $
        learnAttribute ":derp/description" False ONE VT_STR $
        learnAttribute ":derp/firstSeenAt" False ONE VT_INT $
        learnAttribute ":derp/width" False ONE VT_INT $
        learnAttribute ":derp/height" False ONE VT_INT $
        emptyDB
      getAttr x = findWithDefault (ENTID 9999999) x (database.attributes)
  in BASE {
    database,
    derpIdAttr = getAttr ":derp/id",
    derpTagsAttr = getAttr ":derp/tags",
    derpThumbAttr = getAttr ":derp/thumbURL",
    derpImgAttr = getAttr ":derp/imgURL",
    derpUpvotesAttr = getAttr ":derp/upvotes",

    derpDownvotesAttr = getAttr ":derp/downvotes",
    derpScoreAttr = getAttr ":derp/score",
    derpDescriptionAttr = getAttr ":derp/description",
    derpFirstSeenAtAttr = getAttr ":derp/firstSeenAt",
    derpWidthAttr = getAttr ":derp/width",
    derpHeightAttr = getAttr ":derp/height"
    }

mkDatoms :: Item -> EntityRef -> Base
         -> [(EntityRef, EntityId, Query.Types.Value, Bool)]
mkDatoms !item eid BASE{..} =
  let fillRow (!a, !b) = (eid, a, b, True)
      tags = map (\a -> (derpTagsAttr, VAL_STR $! a)) item.tags
  in map fillRow $ tags ++ [
    (derpIdAttr, VAL_INT $ item.idNum),
    (derpThumbAttr, VAL_STR $ item.thumb),
    (derpImgAttr, VAL_STR $ item.img),
    (derpDownvotesAttr, VAL_INT $ item.downvotes),
    (derpUpvotesAttr, VAL_INT $ item.upvotes),
    (derpScoreAttr, VAL_INT $ item.score),
    -- (":derp/faves", VAL_INT $ item.faves),
    (derpDescriptionAttr, VAL_STR $ item.description),
    (derpFirstSeenAtAttr, VAL_INT $ timeStringToEpoch item.firstSeenAt),
    -- (":derp/updatedAt", VAL_INT $ timeStringToEpoch item.updatedAt),
    (derpWidthAttr, VAL_INT $ item.width),
    (derpHeightAttr, VAL_INT $ item.height)
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
  let !newDB = learns datoms database
  case unsafeNoThunks newDB.eav of
    Nothing    -> pure ()
    Just thunk -> error . concat $ [
        "Unexpected thunk with context "
      , show (thunkInfo thunk)
      ]

  put b { database = newDB }
