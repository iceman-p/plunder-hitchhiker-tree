module Main (main) where

import           ClassyPrelude           hiding (many)

import           Control.Monad.State     (MonadState, StateT, evalState,
                                          execStateT, get, gets, liftIO,
                                          modify', put, runStateT)
import           Data.Aeson              hiding (parse)

import           Criterion.Main
import           Criterion.Main.Options

import           Optics                  hiding (noneOf, (%%))
import           System.Directory
import           System.FilePath
import           Text.Parsec

import           Data.Sorted
import           Data.Sorted.Set

import           Impl.Tree
import           Types

import qualified Data.ByteString.Lazy    as BS

import qualified Data.Set                as S

import qualified HitchhikerMap           as HM
import qualified HitchhikerSet           as HS
import qualified HitchhikerSetMap        as SM

import qualified MultiIntersectV1List
import qualified MultiIntersectV2Vector
import qualified MultiIntersectV3Naive
import qualified MultiIntersectV4OnePass
import qualified MultiIntersectV5RHS
import qualified MultiIntersectV6Stack

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
      -- bench "cute" $ (whnf (doLookup model) ["cute"]),
      bench "(mane 6)" $ (whnf (doLookup model) mane6)
      ],
  -- bgroup "Multi Intersection V1 List" [
  --     bench "(mane 6)" $ (whnf (multiIntersectV1List model) mane6)
  --     ],
  -- bgroup "Multi Intersection V2 Vector" [
  --     bench "(mane 6)" $ (whnf (multiIntersectV2Vector model) mane6)
  --     ],
  bgroup "Single Intersection V3 Naive" [
      bench "(mane 6)" $ (whnf (singleIntersectV3Naive model) mane6)
      ],
  -- bgroup "Multi Intersection V4 One Pass" [
  --     bench "(mane 6)" $ (whnf (multiIntersectV4OnePass model) mane6)
  --     ],
  bgroup "Multi Intersection V5 ArrayVsTree" [
      bench "(mane 6)" $ (whnf (multiIntersectV5RHS model) mane6)
      ],
  bgroup "Multi Intersection V6 Stack" [
      bench "(mane 6)" $ (whnf (multiIntersectV6Stack model) mane6)
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

multiIntersectV1List :: Model -> [String] -> [Int]
multiIntersectV1List model tags = evalState run model
  where
    run = do
      s <- searchWithCombiner comb tags
      case s of
        Left tags -> error ("INVALID TAGS: " ++ show tags)
        Right s   -> pure $ force $ join $ map ssetToAscList s

    comb [] = []
    comb xs = MultiIntersectV1List.nuIntersect xs

multiIntersectV2Vector :: Model -> [String] -> [Int]
multiIntersectV2Vector model tags = evalState run model
  where
    run = do
      s <- searchWithCombiner comb tags
      case s of
        Left tags -> error ("INVALID TAGS: " ++ show tags)
        Right s   -> pure $ force $ join $ map ssetToAscList s

    comb [] = []
    comb xs = MultiIntersectV2Vector.nuIntersect xs

singleIntersectV3Naive :: Model -> [String] -> [Int]
singleIntersectV3Naive model tags = evalState run model
  where
    run = do
      s <- searchWithCombiner (comb . (map toSet)) tags
      case s of
        Left tags -> error ("INVALID TAGS: " ++ show tags)
        Right s   -> pure $ force $ join $ map ssetToAscList s

    toSet :: HitchhikerSet Int -> [ArraySet Int]
    toSet (HITCHHIKERSET _ Nothing)  = []
    toSet (HITCHHIKERSET _ (Just a)) = getLeafList HS.hhSetTF a

    comb :: [[ArraySet Int]] -> [ArraySet Int]
    comb []     = []
    comb (x:xs) = foldl' MultiIntersectV3Naive.setlistIntersect x xs

multiIntersectV4OnePass :: Model -> [String] -> [Int]
multiIntersectV4OnePass model tags = evalState run model
  where
    run = do
      s <- searchWithCombiner comb tags
      case s of
        Left tags -> error ("INVALID TAGS: " ++ show tags)
        Right s   -> pure $ force $ join $ map ssetToAscList s

    comb [] = []
    comb xs = MultiIntersectV4OnePass.onePassIntersection xs

multiIntersectV5RHS :: Model ->  [String] -> [Int]
multiIntersectV5RHS model tags = evalState run model
  where
    run = do
      s <- searchWithCombiner comb tags
      case s of
        Left tags -> error ("INVALID TAGS: " ++ show tags)
        Right s   -> pure $ force $ join $ map ssetToAscList s

    comb [] = []
    comb xs = MultiIntersectV5RHS.rhsIntersection xs

multiIntersectV6Stack :: Model ->  [String] -> [Int]
multiIntersectV6Stack model tags = evalState run model
  where
    run = do
      s <- searchWithCombiner comb tags
      case s of
        Left tags -> error ("INVALID TAGS: " ++ show tags)
        Right s   -> pure $ force $ join $ map ssetToAscList s

    comb [] = []
    comb xs = MultiIntersectV6Stack.stackIntersection xs

