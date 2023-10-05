module MultiIntersectV6Stack where

import           ClassyPrelude

import           Impl.Tree
import           Impl.Types
import           Types

import           Data.Sorted

import qualified Data.Set      as S
import qualified Data.Vector   as V
import qualified HitchhikerSet as HS

-- Like V5, but uses an explicit stack instead of the continuation thing it was
-- doing.

data SearchStep k
  = Leaf (ArraySet k)
  | Tree Int Int (Vector k) (Vector (HitchhikerSetNode k)) -- idx len keys vals

{-# INLINE idx #-}
idx i v = V.unsafeIndex v i

{-# INLINE buildSubSearchStep #-}
buildSubSearchStep :: HitchhikerSetNode k -> SearchStep k
buildSubSearchStep (HitchhikerSetNodeLeaf leaf) = Leaf leaf
buildSubSearchStep (HitchhikerSetNodeIndex (TreeIndex ks vs) _) =
  Tree 0 (length ks) ks vs

incrementTree :: SearchStep k -> SearchStep k
incrementTree (Leaf _)       = error "Can't increment a leaf"
incrementTree (Tree i l k v) = Tree (i + 1) l k v

step :: Ord k => [ArraySet k] -> [SearchStep k] -> [ArraySet k]
step [] _ = []
step _ [] = []

step ao@(a:as) lo@(t@(Tree i len keys vals):ls)
  | i == 0 =
      step ao ((buildSubSearchStep $ idx 0 vals):(incrementTree t):ls)
  | i == len =
      step ao ((buildSubSearchStep $ idx len vals):ls)
  | ssetFindMax a < idx (i - 1) keys =
      step as lo
  | ssetFindMin a >= idx i keys =
      step ao ((incrementTree t):ls)
  | otherwise =
      step ao ((buildSubSearchStep $ idx i vals):(incrementTree t):ls)

step ao@(a:as) lo@((Leaf leaf):ls) =
  if overlap && (not $ ssetIsEmpty intersection)
  then intersection:rest
  else rest
  where
    aMin = ssetFindMin a
    aMax = ssetFindMax a
    bMin = ssetFindMin leaf
    bMax = ssetFindMax leaf

    overlap = aMin <= bMax && bMin <= aMax

    intersection = ssetIntersection a leaf

    rest = case aMax `compare` bMax of
      EQ -> step as ls
      GT -> step ao ls
      LT -> step as lo


stackIntersection :: forall k. (NFData k, Ord k, Show k)
                => [HitchhikerSet k] -> [ArraySet k]
stackIntersection [] = []
stackIntersection [x] = case HS.rawNode x of
  Nothing   -> []
  Just node -> getLeafList HS.hhSetTF node
stackIntersection sets@((HITCHHIKERSET config _):_) =
  let byWeight a b = HS.weightEstimate a `compare` HS.weightEstimate b
      orderedByWeight = sortBy byWeight sets
      nodesByWeight = catMaybes $ map HS.rawNode orderedByWeight
      -- minItems = minLeafItems config `div` 2
  in if any HS.null sets
     then []
     else case nodesByWeight of
       []     -> []
       [x]    -> getLeafList HS.hhSetTF x
       (x:xs) ->
         let a :: [ArraySet k] = getLeafList HS.hhSetTF x
             bs :: [HitchhikerSetNode k] = map (flushDownwards HS.hhSetTF) xs
         in foldl' (\a b -> force $ step a [buildSubSearchStep b]) a bs
