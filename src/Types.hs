module Types where

import           Data.Hashable
import           Data.Map        (Map)
import           Data.Sequence   (Seq)

import           Control.DeepSeq
import           GHC.Generics    (Generic, Generic1)

-- TODO: For now, hash is just an int. Change this in production.
type Hash256 = Int

-- Storage of all the nodes.
type NodeStorage k v = Map Hash256 (TreeNode k v)

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

-- The sorted list in the leaves.
type LeafVector k v = Seq (k, v)   -- Sorted

-- The unsorted insertion log that tags along on index nodes, which might have
-- duplicates.
type Hitchhikers k v = Seq (k, v)  -- Unsorted

-- The shared node between both FullTrees and PartialTrees.
data TreeNode k v
  -- Inner B+ index with hitchhiker information.
  = NodeIndex (Index k (TreeNode k v)) (Hitchhikers k v)
  -- Sorted list of leaf values. (in sire, rows access is O(1)).
  | NodeLeaf (LeafVector k v)
  deriving (Show, Eq, Generic, NFData)

instance (Hashable k, Hashable v) => Hashable (TreeNode k v) where
  hashWithSalt s (NodeLeaf lv) = s `hashWithSalt` (0 :: Int) `hashWithSalt` lv
  hashWithSalt s (NodeIndex (Index k hashes) hh) =
    s `hashWithSalt` (1 :: Int) `hashWithSalt` k `hashWithSalt`
    hashes `hashWithSalt` hh


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

-- A Hitchhiker tree where all the links are manual.
data HitchhikerTree k v = HITCHHIKERTREE {
  config :: TreeConfig,
  root   :: Maybe (TreeNode k v)
  }
  deriving (Show, Generic, NFData)


-- A full, content addressed, hash linked hitchhiker tree.
data FullTree k v = FULLTREE {
  config  :: TreeConfig,
  root    :: Maybe Hash256,
  storage :: NodeStorage k v
  }
  deriving (Show, Generic, NFData)
