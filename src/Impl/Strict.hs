module Impl.Strict where

import           ClassyPrelude

import           NoThunks.Class

-- A minimal strict Maybe. I tried importing Data.Strict.Maybe from `strict`,
-- but the name collisions with the normal Data.Maybe types used with all the
-- 3rd party libraries made kind of a disaster.
data StrictMaybe a
  = SNothing
  | SJust !a
  deriving (Foldable, Traversable, Functor, Show, Generic)

deriving instance NoThunks a => NoThunks (StrictMaybe a)

sIsJust :: StrictMaybe a -> Bool
sIsJust (SJust _) = True
sIsJust _         = False

sIsNothing :: StrictMaybe a -> Bool
sIsNothing SNothing = True
sIsNothing _        = False

sCatMaybes :: [StrictMaybe a] -> [a]
sCatMaybes ls = [x | SJust x <- ls]

instance NFData a => NFData (StrictMaybe a) where
  rnf !SNothing  = ()
  rnf (SJust !a) = rnf a

data StrictEither a b
  = SLeft a
  | SRight b
  deriving (Foldable, Traversable, Functor, Show, Generic)

deriving instance (NoThunks a, NoThunks b) => NoThunks (StrictEither a b)
