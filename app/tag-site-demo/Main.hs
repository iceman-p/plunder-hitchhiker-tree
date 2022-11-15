module Main where

import           Control.DeepSeq
import           Control.Monad                  (forM, join)
import           Control.Monad.State            (MonadState, StateT, execStateT,
                                                 get, gets, liftIO, modify',
                                                 put, runStateT)
import           Data.Aeson                     hiding (parse)
import           Debug.Trace
import           GHC.Generics                   (Generic, Generic1)

import           Optics                         hiding (noneOf, (%%))
import           System.Directory
import           System.Environment
import           System.FilePath
import           System.IO
import           System.Posix.Directory
import           Text.Parsec

import           Data.BTree.Primitives.Key
import           ImgJSON
import           Types

import qualified Data.ByteString.Lazy           as BS

import qualified Data.BTree.Pure                as HB
import qualified Data.BTree.Pure.Setup          as HB
import qualified Data.Set                       as S

import qualified HitchhikerMap                  as HM
import qualified HitchhikerSet                  as HS
import qualified HitchhikerSetMap               as SM

import qualified Network.Wai.Application.Static as WSt
import qualified Network.Wai.Handler.Warp       as W
import qualified Network.Wai.Handler.WebSockets as W
import qualified Network.WebSockets             as WS


data UserCmd
    = UC_Search [String]
  deriving (Generic, Show)

data SearchResponse
    = ResponseBadTag [String]
    | ResponseOK [(Int, String, String)]  -- (imgid, thumbnail url)
  deriving (Generic, Show)

-- OK, we're going to parse the

main = do
  -- Load the json into a series of Items
  [indir] <- getArgs

  putStrLn "Loading json files..."
  files :: [FilePath] <- listDirectory indir

  allImages <- forM files $ \file -> do
    bs <- BS.readFile (indir </> file)
    case eitherDecode bs of
      Left err                 -> do { putStrLn err; pure [] }
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
wsApp as conn = do
  sock <- WS.acceptRequest conn
  WS.withPingThread sock 5 (pure ()) do
    undefined
