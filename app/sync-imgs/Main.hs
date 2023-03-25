module Main (main) where

import           ClassyPrelude        hiding (many)


import           Control.Concurrent
import           Control.Monad.State  (MonadState, StateT, evalStateT,
                                       execState, get, gets, liftIO, modify',
                                       put, runStateT)
import           Data.Aeson           hiding (parse)
import           Network.HTTP.Client
import           Network.HTTP.Simple
import           Network.URI
import           System.Directory
import           System.Exit
import           System.FilePath
import           System.IO.Unsafe     (unsafePerformIO)
import           Text.Parsec

import qualified Data.ByteString      as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List            as L

import           ImgJSON

-- We are literally trying to download two million files, and filesystems fall
-- over when you do this. So we have to be fairly careful about how we organize
-- the directory structure. There are currently over 3_000_000 images in the
-- derpibooru corpus, and we are going to be downloading
--
-- img/[id mod 1000]/[full id]/{thumb,large}.ext

-- Use a directory format like Sibusten/derpibooru-downloader, but break every
-- 10,000 instead of every 1,000.

getContainerDir :: Int -> String
getContainerDir i | i < 10000 = "0"
getContainerDir i = (show $ i `div` 10000) <> "0000"

getImgDir :: FilePath -> Int -> FilePath
getImgDir base i = base </> (getContainerDir i) </> (show i)

vWaitAmountMs :: IORef Int
vWaitAmountMs = unsafePerformIO $ newIORef 500

vErrorCount :: IORef Int
vErrorCount = unsafePerformIO $ newIORef 0

-- Checks if an image already exists and then . Backs off once
tryDownloadImg :: FilePath -> Int -> String -> IO ()
tryDownloadImg baseDir fileNo strUri = do
  let dir = getImgDir baseDir fileNo

  request <- parseRequest strUri
  let fileName = L.last . pathSegments . getUri $ request
      fullPath = dir </> fileName

  exists <- doesFileExist fullPath
  when (not exists) $ do
    waitMs <- readIORef vWaitAmountMs
    threadDelay $ waitMs * 1000

    putStrLn $ " - Downloading " <> pack strUri <> "..."

    createDirectoryIfMissing True dir

    -- Attempt to download the file
    resp <- httpBS request
    case getResponseStatusCode resp of
      200 -> do
        BS.writeFile fullPath $ getResponseBody resp
      404 -> do
        putStrLn $ "    - ERROR: 404"
        -- INVESTIGATE WHY SOME FILES ARE MISSING LATER.
        -- threadDelay $ 2_000_000
        -- errCount <- atomicModifyIORef vErrorCount (\i -> ((i + 1), (i + 1)))
        -- when (errCount >= 5) $ do
        --   putStrLn "EXITING DUE TO TOO MANY ERRORS"
        --   exitFailure

        pure ()
      x -> do
        putStrLn $ "   - ERROR: " <> tshow x

        errCount <- atomicModifyIORef vErrorCount (\i -> ((i + 1), (i + 1)))
        when (errCount >= 5) $ do
          putStrLn "EXITING DUE TO TOO MANY ERRORS"
          exitFailure

        -- Permanently back off more.
        modifyIORef' vWaitAmountMs (+ 250)
        tryDownloadImg baseDir fileNo strUri

  pure ()

main = do
  -- Load the json into a series of Items
  x <- getArgs
  putStrLn $ "args: " <> tshow x
  [i] <- getArgs
  let datadir = unpack i
      jsondir = datadir </> "json"
      imgdir = datadir </> "img"

  files :: [FilePath] <- listDirectory jsondir
  let numFiles = tshow $ length files

  forM (zip [1..] files) $ \(fileIdx, file) -> do
    -- putStrLn $ "============== JSON FILE " <> (tshow fileIdx) <> " OF "
    --         <> numFiles <> " =============="

    let path = jsondir </> file
    bs <- BSL.readFile path
    case eitherDecode bs of
      Left err                 -> do { putStrLn $ pack err; pure [] }
      Right (ImagesJSON items) -> do
        forM items $ \(Item id _ thumb medium) -> do
          tryDownloadImg imgdir id thumb
          --tryDownloadImg imgdir id medium
