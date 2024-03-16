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
import qualified Data.Vector                as V

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

--  files :: [FilePath] <- listDirectory jsondir
  files :: [FilePath] <- take 250 <$> listDirectory jsondir
--  files :: [FilePath] <- take 10000 <$> listDirectory jsondir

  let z = zip [1..] files
      l = show $ length files

  flip evalStateT emptyBase $ do
    dbPlan <- case fullDerpPlan (database emptyBase) of
      Left err -> error $ "Error building query plan: " <> show err
      Right x  -> pure x

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

    -- BASE{..} <- get
    -- pPrint $ (eav database)
    -- putStrLn $ ":derp/id = " <> tshow derpIdAttr
    -- putStrLn $ ":derp/tags = " <> tshow derpTagsAttr
    -- putStrLn $ ":derp/thumbURL = " <> tshow derpThumbAttr
    -- pPrint $ (ave database)
    -- pPrint $ (vae database)

    repl dbPlan

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

repl :: PlanHolder -> StateT Base IO ()
repl derpQueryPlan = do
  raw <- read'

  BASE{database} <- get
  -- Chunk out on comma.
  let c = parse (sepBy (many (noneOf ",")) delim) "" raw
  case c of
    Left err -> do
      liftIO $ putStrLn $ tshow err
      repl derpQueryPlan
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
      traceM ("Plan: " <> show derpQueryPlan)
      let amount = 300
      let planOut = evalPlan
            [REL_SET $ RSET (VAR "?tags")
                            (HS.fromSet twoThreeConfig $ S.fromList $
                             map VAL_STR tags),
             REL_SCALAR $ RSCALAR (VAR "?min") (VAL_INT amount)]
            database
            derpQueryPlan
          naiveOut = naiveEvaluator
            database
            [ROWS [VAR "?tags"] [] (map toNaiveTag tags),
             ROWS [VAR "?min"] [] [V.fromList [VAL_INT amount]]]
            []
            fullDerpClauses
            [VAR "?derpid", VAR "?score", VAR "?thumburl"]
          toNaiveTag t = V.fromList [VAL_STR t]
      putStrLn $ "OUT: " <> tshow planOut
      putStrLn $ "NAIVE: " <> tshow naiveOut
      putStrLn $ "EQ TO NAIVE: " <> tshow (planOut == naiveOut)
      putStrLn $ "SORTED EQ TO NAIVE: " <> tshow (sort planOut.values == sort naiveOut.values)
      -- s <- search tags
      -- case s of
      --   Left tags -> liftIO $ putStrLn ("INVALID TAGS: " ++ tshow tags)
      --   Right s   -> do
      --     longResults <- lookupItems s
      --     forM_ longResults $ \(imgid, thumb) -> do
      --       putStrLn (" " ++ (tshow imgid) ++ ": " ++ pack thumb)
      repl derpQueryPlan

fullDerpClauses = [
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/tags") (VAR "?tags")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/score") (VAR "?score")),
  BiPredicateExpression (ARG_VAR (VAR "?score")) B_GTE
                              (ARG_VAR (VAR "?min")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/id") (VAR "?derpid")),
  DataPattern (LC_XAZ (VAR "?e") (ATTR ":derp/thumbURL") (VAR "?thumburl"))]

fullDerpPlan database = mkPlan
  [database]
  [B_COLLECTION (VAR "?tags"), B_SCALAR (VAR "?min")]
  fullDerpClauses
  [VAR "?derpid", VAR "?score", VAR "?thumburl"]
