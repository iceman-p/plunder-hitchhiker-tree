{-# LANGUAGE Strict     #-}
{-# LANGUAGE StrictData #-}

module Types where

import           ClassyPrelude

import           Data.Map      (Map)

-- For ArraySet
import           Data.Sorted

import           Impl.Types

-- TODO: For now, hash is just an int. Change this in production.
type Hash256 = Int

-- | The rightmost index of the node (for traversing back to it).
type KeyLoc k = Maybe k

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
  deriving (Eq, Generic, NFData)

instance Show TreeConfig where
  show _ = "TREECONFIG"

-- Stolen directly from hasky-btree for testing. Too small in practice to hold
-- up a real large channel database.
twoThreeConfig :: TreeConfig
twoThreeConfig = TREECONFIG {
    minFanout = minFanout'
  , maxFanout = maxFanout'
  , minIdxKeys = minFanout' - 1
  , maxIdxKeys = maxFanout' - 1
  , minLeafItems = minFanout'
  , maxLeafItems = 2*minFanout' - 1
  , maxHitchhikers = minFanout'
  }
  where
    minFanout' = 2
    maxFanout' = 2*minFanout' - 1

-- A config with larger corfficients for more realistic testing.
largeConfig :: TreeConfig
largeConfig = TREECONFIG {
    minFanout = minFanout'
  , maxFanout = maxFanout'
  , minIdxKeys = minFanout' - 1
  , maxIdxKeys = maxFanout' - 1
  , minLeafItems = minFanout'
  , maxLeafItems = 2*minFanout' - 1
  , maxHitchhikers = minFanout'
  }
  where
    minFanout' = 64
    maxFanout' = 2 * minFanout' - 1

-- -----------------------------------------------------------------------

-- The shared node between both FullTrees and PartialTrees.
data HitchhikerMapNode k v
  -- Inner B+ index with hitchhiker information.
  = HitchhikerMapNodeIndex (TreeIndex k (HitchhikerMapNode k v)) (Map k v)
  -- Sorted list of leaf values. (in sire, rows access is O(1)).
  | HitchhikerMapNodeLeaf (Map k v)
  deriving (Show, Generic, NFData)

-- A Hitchhiker tree where all the links are manual.
data HitchhikerMap k v = HITCHHIKERMAP {
  config :: TreeConfig,
  root   :: Maybe (HitchhikerMapNode k v)
  }
  deriving (Show, Generic, NFData)

-- -----------------------------------------------------------------------

data HitchhikerSetNode k
  = HitchhikerSetNodeIndex !(TreeIndex k (HitchhikerSetNode k)) !(Int, [k])
  | HitchhikerSetNodeLeaf !(ArraySet k)
  deriving (Show, Generic, NFData)

data HitchhikerSet k = HITCHHIKERSET {
  config :: TreeConfig,
  root   :: Maybe (HitchhikerSetNode k)
  }
  deriving (Show, Generic, NFData)

-- -----------------------------------------------------------------------

data NakedHitchhikerSet k = NAKEDSET (Maybe (HitchhikerSetNode k))
  deriving (Show, Generic, NFData)

data HitchhikerSetMapNode k v
  = HitchhikerSetMapNodeIndex (TreeIndex k (HitchhikerSetMapNode k v))
                              (Map k (ArraySet v))
  | HitchhikerSetMapNodeLeaf (Map k (NakedHitchhikerSet v))
  deriving (Show, Generic, NFData)

data HitchhikerSetMap k v = HITCHHIKERSETMAP {
  config :: TreeConfig,
  root   :: Maybe (HitchhikerSetMapNode k v)
  }
  deriving (Show, Generic, NFData)


-- -----------------------------------------------------------------------

-- The shared node between both FullTrees and PartialTrees.
data PublishTreeNode k v
  -- Inner B+ index with hitchhiker information.
  = PublishNodeIndex (TreeIndex k Hash256) (Map k v)
  -- Sorted list of leaf values. (in sire, rows access is O(1)).
  | PublishNodeLeaf (Map k v)
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
  hashWithSalt s (PublishNodeIndex (TreeIndex k hashes) hh) =
    s `hashWithSalt` (1 :: Int) `hashWithSalt` (toList k) `hashWithSalt`
    (toList hashes) `hashWithSalt` hh
