{-# LANGUAGE NoFieldSelectors #-}
module Main where

import           ClassyPrelude

import           Control.DeepSeq
import           Control.Monad                  (forM, forever, join)
import           Control.Monad.State            (MonadState, StateT, evalStateT,
                                                 execStateT, get, gets, liftIO,
                                                 modify', put, runStateT)
import           Data.Aeson                     (FromJSON, ToJSON)
import           GHC.Generics                   (Generic, Generic1)

import           Optics                         hiding (noneOf, (%%))
import           System.Directory
import           System.FilePath
import           System.Posix.Directory

import           ImgJSON
import           Types

import qualified HitchhikerSet                  as HS

import qualified Data.Aeson                     as A
import qualified Data.ByteString.Lazy           as BS
import qualified Network.Wai.Application.Static as WSt
import qualified Network.Wai.Handler.Warp       as W
import qualified Network.Wai.Handler.WebSockets as W
import qualified Network.WebSockets             as WS


data UserCmd
    = UC_Search [String]
    | UC_Info Int
  deriving (Generic, Show)

instance FromJSON UserCmd where
  parseJSON = A.genericParseJSON
            $ A.defaultOptions { A.constructorTagModifier = drop 3 }

instance ToJSON UserCmd where
  toEncoding = A.genericToEncoding
             $ A.defaultOptions { A.constructorTagModifier = drop 3 }

data Response
    = SearchResponseBadTag [String]
    | SearchResponseOK [(Int, String)]  -- (imgid, thumbnail url)
    | SearchResponseInfo { id :: Int, tags :: [String], imgUrl :: String }
  deriving (Generic, Show)

instance FromJSON Response where
  parseJSON = A.genericParseJSON
            $ A.defaultOptions { A.constructorTagModifier = drop 14 }

instance ToJSON Response where
  toEncoding = A.genericToEncoding
             $ A.defaultOptions { A.constructorTagModifier = drop 14 }



main = do
  -- Load the json into a series of Items
  [i] <- getArgs
  let datadir = unpack i
      jsondir = datadir </> "json"

  putStrLn "Loading json files..."
  files :: [FilePath] <- listDirectory jsondir

  allImages <- forM files $ \file -> do
    let path = jsondir </> file
    bs <- BS.readFile path
    case A.eitherDecode bs of
      Left err                 -> do { putStrLn $ tshow err; pure [] }
      Right (ImagesJSON items) -> pure items

  model <- flip execStateT emptyModel $ do
    mapM addEntry (join allImages)
    modifying' #tags force

  runServer datadir model

runServer :: FilePath -> Model -> IO ()
runServer indir m = do
  let opt = WSt.defaultWebAppSettings indir

  putStrLn "Running server on port 8888"
  W.run 8888
      $ W.websocketsOr WS.defaultConnectionOptions (wsApp m)
      $ WSt.staticApp opt

wsApp :: Model -> WS.PendingConnection ->  IO ()
wsApp model conn = do
  sock <- WS.acceptRequest conn
  WS.withPingThread sock 5 (pure ()) $ forever do
    buf <- try (WS.receiveData @ByteString sock) >>= \case
           Left (e :: SomeException) -> do
               print ("DIED", e)
               throwIO e
           Right bs -> pure bs
    case A.decodeStrict buf of
        Nothing               -> putStrLn "BAD JSON"
        Just (UC_Search tags) -> flip evalStateT model $ do
          results <- search tags
          msg <- case results of
            Left missing -> pure $ SearchResponseBadTag missing
            Right items  -> SearchResponseOK <$> lookupItems items
          liftIO $ WS.sendBinaryData sock $ A.encode msg

        Just (UC_Info imgid)  -> undefined
