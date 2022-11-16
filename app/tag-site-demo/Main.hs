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

data Response
    = SearchResponseBadTag [String]
    | SearchResponseOK [(Int, String)]  -- (imgid, thumbnail url)
    | SearchResponseInfo { id :: Int, tags :: [String], imgUrl :: String }
  deriving (Generic, Show)

instance FromJSON UserCmd where
    parseJSON = A.genericParseJSON
               $ A.defaultOptions { A.constructorTagModifier = drop 3 }

instance ToJSON UserCmd where
    toEncoding = A.genericToEncoding
               $ A.defaultOptions { A.constructorTagModifier = drop 3 }


-- OK, we're going to parse the

main = do
  -- Load the json into a series of Items
  [_, i] <- getArgs
  let indir = unpack i
  files :: [FilePath] <- listDirectory indir

  putStrLn "Loading json files..."
  files :: [FilePath] <- listDirectory indir

  allImages <- forM files $ \file -> do
    bs <- BS.readFile (indir </> file)
    case A.eitherDecode bs of
      Left err                 -> do { putStrLn $ tshow err; pure [] }
      Right (ImagesJSON items) -> pure items

  model <- flip execStateT emptyModel $ do
    mapM addEntry (join allImages)
    modifying' #tags force

  runServer model

runServer :: Model -> IO ()
runServer m = do
  -- TODO: Something real for serving images instead of crap to make it
  -- compile.
  dir <- getWorkingDirectory
  let opt = WSt.defaultWebAppSettings dir

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
        Just (UC_Search tags) -> do
          --s <- HS.toSet <$> evalStateT (search tags) model
          --WS.sendBinaryData sock $ A.encode (
          undefined
        Just (UC_Info imgid)  -> undefined
