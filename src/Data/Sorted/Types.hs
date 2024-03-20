-- Copyright 2023 The Plunder Authors
-- Use of this source code is governed by a BSD-style license that can be
-- found in the LICENSE file.

{-# OPTIONS_GHC -Wall   #-}
-- {-# OPTIONS_GHC -Werror #-}  -- allow orphans for now
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE Strict      #-}
{-# LANGUAGE StrictData  #-}

module Data.Sorted.Types
    ( Row
    , Set(..)
    , Tab(..)
    )
where

import           Control.DeepSeq
import           Data.Dynamic
import           Data.Primitive.Array
import           GHC.Generics
import           NoThunks.Class
import           Prelude

--------------------------------------------------------------------------------

type Row a = Array a

-- deriving via InspectHeap (Array a) instance Typeable a => NoThunks (Array a)

instance NoThunks (Array a) where
  showTypeOf _ = "<never used since never fails>"
  noThunks _ _ = return Nothing
  wNoThunks    = noThunks

newtype Set k = SET (Row k)
  deriving newtype (Eq, Ord, Show, NFData, Functor, Foldable, NoThunks)

data Tab k v = TAB
    { keys :: {-# UNPACK #-} !(Row k)
    , vals :: {-# UNPACK #-} !(Row v)
    }
  deriving (Eq, Ord, Show, Generic, NFData)
