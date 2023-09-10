module Main (main) where

import           ClassyPrelude             hiding (many)

import           Control.Monad.State       (MonadState, StateT, evalState,
                                            execStateT, get, gets, liftIO,
                                            modify', put, runStateT)
import           Data.Aeson                hiding (parse)

import           Criterion.Main
import           Criterion.Main.Options

import           Optics                    hiding (noneOf, (%%))
import           System.Directory
import           System.FilePath
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

import qualified MultiIntersectV2Vector

import           ImgJSON

-- OK, we're going to parse the

main = do
  -- Load the json into a series of Items
  x <- getArgs
  putStrLn $ "args: " <> tshow x
  [_, i] <- getArgs
  let datadir = unpack i
      jsondir = datadir </> "json"

  files :: [FilePath] <- take 10000 <$> listDirectory jsondir

  let z = zip [1..] files
      l = tshow $ length files

  model <- flip execStateT emptyModel $ do
    forM z $ \(i, file) -> do
      when (i `mod` 200 == 0) $
        putStrLn ("Loading " ++ tshow i ++ " of " ++ l)
      let path = jsondir </> file
      bs <- liftIO $ BS.readFile path
      case eitherDecode bs of
        Left err                 -> putStrLn $ pack err
        Right (ImagesJSON items) -> do
          mapM addEntry items
          pure ()

    modifying' #tags force

  putStrLn "Loading complete."
  putStrLn "-----------------"

  benchmark model

defaultMode :: Mode
defaultMode = Run defaultConfig Prefix []

mane6 :: [String]
mane6 = [
  "twilight sparkle",
  "rarity",
  "applejack",
  "pinkie pie",
  "fluttershy",
  "rainbow dash"
  ]

benchmark :: Model -> IO ()
benchmark model = runMode defaultMode [
  bgroup "Original Intersection" [
      bench "cute" $ (whnf (doLookup model) ["cute"]),
      bench "(mane 6)" $ (whnf (doLookup model) mane6)
      ],
  bgroup "Multi Intersection V2 Vector" [
      bench "(mane 6)" $ (whnf (multiIntersectV2Vector model) mane6)
      ]
  ]

doLookup :: Model -> [String] -> [Int]
doLookup model tags = evalState run model
  where
    run = do
      s <- search tags
      case s of
        Left tags -> error ("INVALID TAGS: " ++ show tags)
        Right s   -> pure $ force $ S.toList $ HS.toSet s

-- Uses multiintersectv2 on the data
multiIntersectV2Vector :: Model -> [String] -> [Int]
multiIntersectV2Vector model tags = evalState run model
  where
    run = do
      s <- searchWithCombiner comb tags
      case s of
        Left tags -> error ("INVALID TAGS: " ++ show tags)
        Right s   -> pure $ force $ join $ map S.toList s

    comb [] = []
    comb xs = let x = MultiIntersectV2Vector.nuIntersect xs
              in x --trace ("Show x: " ++ show x) x

