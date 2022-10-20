module Types where

import           Data.Hashable
import           Data.Map        (Map)
import           Data.Sequence   (Seq)

import           Control.DeepSeq
import           GHC.Generics    (Generic, Generic1)

-- TODO: For now, hash is just an int. Change this in production.
type Hash256 = Int

-- An Index is the main payload of an index node: the inner node of a B+ tree.
--
-- An index is a parallel array where each key is the smallest item in the
-- following value sequence.
--
-- (Contrast that with how the Clojure hitchhiker tree works where things are
-- right aligned instead.)
data Index k v = Index (Seq k) (Seq v)
  deriving (Show, Eq, Generic, NFData)

-- | The rightmost index of the node (for traversing back to it).
type KeyLoc k = Maybe k

-- The unsorted insertion log that tags along on index nodes, which might have
-- duplicates.
type Hitchhikers k v = Seq (k, v)  -- Unsorted

-- The sorted list in the leaves.
type LeafVector k v = Seq (k, v)   -- Sorted

-- ----------------------------------------------------------------------------

-- B+ and hitchhiker tree configuration.
data TreeConfig = TREECONFIG {
  minFanout      :: Int,
  maxFanout      :: Int,
  minIdxKeys     :: Int,
  maxIdxKeys     :: Int,
  minLeafItems   :: Int,
  maxLeafItems   :: Int,
  maxHitchhikers :: Int
  }
  deriving (Show, Generic, NFData)

-- The shared node between both FullTrees and PartialTrees.
data HitchhikerMapNode k v
  -- Inner B+ index with hitchhiker information.
  = HitchhikerNodeIndex (Index k (HitchhikerMapNode k v)) (Hitchhikers k v)
  -- Sorted list of leaf values. (in sire, rows access is O(1)).
  | HitchhikerNodeLeaf (LeafVector k v)
  deriving (Show, Eq, Generic, NFData)

-- A Hitchhiker tree where all the links are manual.
data HitchhikerMap k v = HITCHHIKERTREE {
  config :: TreeConfig,
  root   :: Maybe (HitchhikerMapNode k v)
  }
  deriving (Show, Generic, NFData)

-- -----------------------------------------------------------------------

-- The shared node between both FullTrees and PartialTrees.
data PublishTreeNode k v
  -- Inner B+ index with hitchhiker information.
  = PublishNodeIndex (Index k Hash256) (Hitchhikers k v)
  -- Sorted list of leaf values. (in sire, rows access is O(1)).
  | PublishNodeLeaf (LeafVector k v)
  deriving (Show, Eq, Generic, NFData)

-- Content addressed storage of all the nodes.
type NodeStorage k v = Map Hash256 (PublishTreeNode k v)

-- HitchhikerMap, but in the form that every node is content
-- addressed. Immutable.
data PublishTree k v = PUBLISHTREE {
  publishRoot :: Maybe Hash256,
  storage     :: NodeStorage k v
  }
  deriving (Show, Generic, NFData)

instance (Hashable k, Hashable v) => Hashable (PublishTreeNode k v) where
  hashWithSalt s (PublishNodeLeaf lv) =
    s `hashWithSalt` (0 :: Int) `hashWithSalt` lv
  hashWithSalt s (PublishNodeIndex (Index k hashes) hh) =
    s `hashWithSalt` (1 :: Int) `hashWithSalt` k `hashWithSalt`
    hashes `hashWithSalt` hh
