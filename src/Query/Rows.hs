module Query.Rows where

import           ClassyPrelude

import           Query.Types
import           Types

import           Data.Containers.ListUtils (nubOrd)
import           Data.Maybe                (fromJust)

import qualified Data.List                 as L
import qualified Data.Set                  as S
import qualified Data.Vector               as V

import qualified HitchhikerMap             as HM
import qualified HitchhikerSet             as HS
import qualified HitchhikerSetMap          as HSM

-- Row handling functions

tabToRows :: Variable -> Variable -> HitchhikerSetMap Value Value -> Rows
tabToRows from to setmap = ROWS [from, to] [] asRows
  where
    asRows = concat $ map step $ HM.toList $ HSM.toHitchhikerMap setmap

    step :: (Value, HitchhikerSet Value) -> [Vector Value]
    step (k, tops) = map (\a -> V.fromList [k, a]) $ HS.toList tops

-- -----------------------------------------------------------------------

-- Given a set of `req` variables in a multitab, render that as a row
multiTabToRows :: [Variable]
               -> Variable
               -> [Variable]
               -> HitchhikerMap Value (Vector (HitchhikerSet Value))
               -> Rows
multiTabToRows req key vals hhmap =
  ROWS req req rowData
  where
    rowData = nubOrd $ concat $ map step $ HM.toList hhmap

    -- In order, what are the the indexes of |vals| in
    outIdxes :: [Either () Int]
    outIdxes = map getIndexOf req

    getIndexOf :: Variable -> Either () Int
    getIndexOf var
      | var == key = Left ()
      | otherwise = Right $ fromJust $ L.elemIndex var vals

    getValueList :: (Value, Vector (HitchhikerSet Value))
                 -> Either () Int
                 -> [Value]
    getValueList (keyVal, _) (Left ())  = [keyVal]
    getValueList (_, valVals) (Right i) = HS.toList $ valVals V.! i

    step :: (Value, Vector (HitchhikerSet Value)) -> [Vector Value]
    step mapItem =
      map V.fromList $ sequence $ map (getValueList mapItem) outIdxes

-- -----------------------------------------------------------------------

-- Change the internal relation type to the output rows.
relationToRows :: Relation -> Rows
relationToRows (REL_SCALAR RSCALAR{..}) = ROWS [sym] [sym] [V.singleton val]
relationToRows (REL_SET RSET{..}) = ROWS [sym] [sym] items
  where
    items = map V.singleton $ S.toList $ HS.toSet val
relationToRows (REL_TAB RTAB{..}) = tabToRows from to val
relationToRows (REL_MULTITAB RMTAB{..}) = multiTabToRows (from:to) from to val
relationToRows (REL_ROWS r) = r
