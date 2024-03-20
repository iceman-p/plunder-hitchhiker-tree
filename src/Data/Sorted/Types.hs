-- Copyright 2023 The Plunder Authors
-- Use of this source code is governed by a BSD-style license that can be
-- found in the LICENSE file.

{-# OPTIONS_GHC -Wall   #-}
{-# OPTIONS_GHC -Werror #-}
{-# LANGUAGE Strict     #-}
{-# LANGUAGE StrictData #-}

module Data.Sorted.Types
    ( Row
    , Set(..)
    , Tab(..)
    )
where

import           Control.DeepSeq
import           Data.Primitive.Array
import           GHC.Generics
import           Prelude

--------------------------------------------------------------------------------

type Row a = Array a

newtype Set k = SET (Row k)
  deriving newtype (Eq, Ord, Show, NFData, Functor, Foldable)

data Tab k v = TAB
    { keys :: {-# UNPACK #-} !(Row k)
    , vals :: {-# UNPACK #-} !(Row v)
    }
  deriving (Eq, Ord, Show, Generic, NFData)
