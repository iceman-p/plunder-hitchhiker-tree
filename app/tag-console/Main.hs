module Main (main) where

import           ClassyPrelude        hiding (many)

import           Control.Monad.State  (MonadState, StateT, evalStateT,
                                       execState, get, gets, liftIO, modify',
                                       put, runStateT)
import           Data.Aeson           hiding (parse)

import           Optics               hiding (noneOf, (%%))
import           System.Directory
import           System.FilePath
import           Text.Parsec

import           Types

import qualified Data.ByteString.Lazy as BS

import qualified Data.Set             as S

import qualified HitchhikerMap        as HM
import qualified HitchhikerSet        as HS
import qualified HitchhikerSetMap     as SM

import           ImgJSON

-- OK, we're going to parse the

main = do
  -- Load the json into a series of Items
  x <- getArgs
  putStrLn $ "args: " <> tshow x
  [_, i] <- getArgs
  let datadir = unpack i
      jsondir = datadir </> "json"

--  files :: [FilePath] <- take 10000 <$> listDirectory jsondir
  files :: [FilePath] <- take 250 <$> listDirectory jsondir

  let z = zip [1..] files
      l = show $ length files

  flip evalStateT emptyModel $ do
    forM z $ \(i, file) -> do
      traceM $ "Loading " ++ show i ++ " of " ++ l
      let path = jsondir </> file
      bs <- liftIO $ BS.readFile path
      case eitherDecode bs of
        Left err                 -> putStrLn $ pack err
        Right (ImagesJSON items) -> do
          mapM addEntry items
          pure ()

      modifying' #tags force

    -- repl

delim = do
  many (char ' ')
  char ','
  many (char ' ')
  pure ()

read' :: StateT Model IO Text
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
      liftIO $ putStrLn $ tshow err
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
      liftIO $ putStrLn ("TAGS: " ++ tshow tags)
      s <- search tags
      case s of
        Left tags -> liftIO $ putStrLn ("INVALID TAGS: " ++ tshow tags)
        Right s   -> do
          longResults <- lookupItems s
          forM_ longResults $ \(imgid, thumb) -> do
            putStrLn (" " ++ (tshow imgid) ++ ": " ++ pack thumb)
      repl
