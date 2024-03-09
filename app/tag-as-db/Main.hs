module Main (main) where

import           ClassyPrelude              hiding (many)

import           Control.Monad.State        (MonadState, StateT, evalStateT,
                                             execState, get, gets, liftIO,
                                             modify', put, runStateT)
import           Data.Aeson                 hiding (parse)

import           Optics                     hiding (noneOf, (%%))
import           System.Directory
import           System.FilePath
import           Text.Parsec

import           Text.Pretty.Simple

import           Query.HitchhikerDatomStore
import           Query.NaiveEvaluator
import           Query.PlanEvaluator
import           Query.Planner
import           Query.Types
import           Types

import qualified Data.ByteString.Lazy       as BS

import qualified Data.Set                   as S

import qualified HitchhikerMap              as HM
import qualified HitchhikerSet              as HS
import qualified HitchhikerSetMap           as SM

import           DataJSON

-- OK, we're going to parse the

main = do
  -- Load the json into a series of Items
  x <- getArgs
  putStrLn $ "args: " <> tshow x
  [_, i] <- getArgs
  let datadir = unpack i
      jsondir = datadir </> "json"

  files :: [FilePath] <- take 250 <$> listDirectory jsondir
--  files :: [FilePath] <- take 10000 <$> listDirectory jsondir

  let z = zip [1..] files
      l = show $ length files

  flip evalStateT emptyBase $ do
    forM z $ \(i, file) -> do
      traceM $ "Loading " ++ show i ++ " of " ++ l ++ " " ++ (show file)
      let path = jsondir </> file
      bs <- liftIO $ BS.readFile path
      case eitherDecode bs of
        Left err                 -> putStrLn $ pack err
        Right (ImagesJSON !items) -> do
          -- mapM addEntry items
          addEntries items
          pure ()

      modify' force

    -- (BASE _ _ db) <- get
    -- pPrint $ (eav db)

    -- repl

delim = do
  many (char ' ')
  char ','
  many (char ' ')
  pure ()

read' :: StateT Base IO Text
read' = liftIO $ do
  putStr "SEARCH> "
  hFlush stdout
  getLine

repl :: StateT Base IO ()
repl = do
  raw <- read'

  (BASE _ _ db) <- get
  -- Chunk out on comma.
  let c = parse (sepBy (many (noneOf ",")) delim) "" raw
  case c of
    Left err -> do
      liftIO $ putStrLn $ tshow err
      repl
    -- Right [":showitems"] -> do
    --   (Model items _) <- get
    --   traceM $ show items
    --   repl
    -- Right [":showtags"] -> do
    --   (Model _ tags) <- get
    --   traceM $ show tags
    --   repl
    Right [":quit"] -> do
      liftIO $ putStrLn "goodbye"
      pure ()
    Right tags -> do
      liftIO $ putStrLn ("TAGS: " ++ tshow tags)
      traceM ("Plan: " <> show fullDerpPlan)
      let qOut = evalPlan
            [REL_SET $ RSET (VAR "?tags")
                            (HS.fromSet twoThreeConfig $ S.fromList $
                             map VAL_STR tags),
             REL_SCALAR $ RSCALAR (VAR "?amount") (VAL_INT 100)]
            db
            fullDerpPlan
      putStrLn $ "OUT: " <> tshow qOut
      -- s <- search tags
      -- case s of
      --   Left tags -> liftIO $ putStrLn ("INVALID TAGS: " ++ tshow tags)
      --   Right s   -> do
      --     longResults <- lookupItems s
      --     forM_ longResults $ \(imgid, thumb) -> do
      --       putStrLn (" " ++ (tshow imgid) ++ ": " ++ pack thumb)
      repl

fullDerpClauses = [
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/tags") (VAR "?tags")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/upvotes") (VAR "?upvotes")),
  BiPredicateExpression B_GT (ARG_VAR (VAR "?upvotes"))
                             (ARG_VAR (VAR "?amount")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/thumbURL") (VAR "?thumburl"))]

fullDerpPlan = mkPlan
  [B_COLLECTION (VAR "?tags"), B_SCALAR (VAR "?amount")]
  fullDerpClauses
  [VAR "?derpid", VAR "?thumburl"]
