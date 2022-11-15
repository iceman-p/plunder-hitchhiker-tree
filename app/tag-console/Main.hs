module Main (main) where

import           Control.DeepSeq
import           Control.Monad             (forM, join)
import           Control.Monad.State       (MonadState, StateT, evalStateT,
                                            execState, get, gets, liftIO,
                                            modify', put, runStateT)
import           Data.Aeson                hiding (parse)
import           Debug.Trace

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

import           ImgJSON

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
    modifying' #tags force

    repl

delim = do
  many (char ' ')
  char ','
  many (char ' ')
  pure ()

read' :: StateT Model IO String
read' = liftIO $ do
  putStr "SEARCH> "
  hFlush stdout
  getLine

repl :: StateT Model IO ()
repl = do
  raw <- read'

  -- Chunk out on comma.
  let c = parse (sepBy (many (noneOf ",")) delim) "" raw
  case c of
    Left err -> do
      liftIO $ putStrLn $ show err
      repl
    Right [":showitems"] -> do
      (Model items _) <- get
      traceM $ show items
      repl
    Right [":showtags"] -> do
      (Model _ tags) <- get
      traceM $ show tags
      repl
    Right [":quit"] -> do
      liftIO $ putStrLn "goodbye"
      pure ()
    Right tags -> do
      liftIO $ putStrLn ("TAGS: " ++ show tags)
      s <- HS.toSet <$> search tags
      liftIO $ putStrLn ("RESULT: " ++ show s)
      repl
