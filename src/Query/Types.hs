module Query.Types where

import           ClassyPrelude

import           Types

import qualified HitchhikerSet    as HS
import qualified HitchhikerSetMap as HSM

import qualified Data.Set         as S

-- -----------------------------------------------------------------------

newtype EntityId = ENTID Int
  deriving (Show, Eq, Ord)

-- The attribute type
newtype Attr = ATTR Text
  deriving (Show, Eq, Ord)

data Value
  = VAL_ATTR Attr
  | VAL_ENTID EntityId
  | VAL_INT Int
  | VAL_STR String
  deriving (Show, Eq, Ord)

instance IsString Value where
  fromString = VAL_STR

-- instance Num Value where
--   fromInteger = VAL_INT . fromInteger

-- -----------------------------------------------------------------------


-- A variable like "?e"
newtype Variable = VAR Text
  deriving (Show, Ord, Eq)

-- Binding query input or function output to the right type.
data Binding
  = B_SCALAR Variable
  -- | B_TUPLE [Variable]
  | B_COLLECTION Variable
  -- | B_RELATION
  deriving (Show)

data DataSource
  = DataSourceDefault
  | DataSourceNamed Text
  deriving (Show)

-- -----------------------------------------------------------------------
-- Predicate definitions
-- -----------------------------------------------------------------------

data FnArg
  = ARG_VAR Variable
  | ARG_CONST Value
  | ARG_Source DataSource
  deriving (Show)

fnArgToVariable :: FnArg -> [Variable]
fnArgToVariable (ARG_VAR v) = [v]

data BuiltinPred
  = B_LT
  | B_LTE
  | B_EQ
  | B_GTE
  | B_GT
  deriving (Show)

data Pred
  = PredBuiltin BuiltinPred
  | PredFun Func
  deriving (Show)

data Func = FUNC -- TODO
  deriving (Show)

data Predicate = PREDICATE Pred [FnArg]
  deriving (Show)

-- -----------------------------------------------------------------------

data RowLookup
  = USE_EAV
  | USE_AEV
  | USE_AVE
  | USE_VAE
  deriving (Show)

-- -----------------------------------------------------------------------
-- Relations
-- -----------------------------------------------------------------------

-- A relation is a representation of

-- The most basic relation storage is ROWS: literally a list of column names,
-- the current sort order of columns and a list of rows. Several things cause
-- us to fall back to this most basic representation, though we try to avoid
-- it.
--
data Rows = ROWS
  { columns   :: [Variable]
  , sortOrder :: [Variable]
  , values    :: [Vector Value]
  }
  deriving (Show)

-- The more specific relation storage types:
data RelScalar = RSCALAR { sym :: Variable, val :: Value }
  deriving (Show)
data RelSet = RSET { sym :: Variable, val :: HitchhikerSet Value}
  deriving (Show)
data RelTab = RTAB { from :: Variable
                   , to   :: Variable
                   , val  :: HitchhikerSetMap Value Value}
  deriving (Show)

-- A table from one key symbol to multiple value symbols. Since so many queries
-- end with a series of lookups on a entity id, this often saves
-- operations. This should be read as a mapping from `from` to the Cartesian
-- product of the `to` sets.
data RelMultiTab = RMTAB
  { from :: Variable
  , to   :: [Variable]
  , val  :: HitchhikerMap Value (Vector (HitchhikerSet Value))
  }
  deriving (Show)

data Relation
  = REL_SCALAR RelScalar
  | REL_SET RelSet
  | REL_TAB RelTab
  | REL_MULTITAB RelMultiTab
  deriving (Show)

-- -----------------------------------------------------------------------

-- Type pattern EAV for binding value, XYZ for binding symbol
data LoadClause
  = LC_EAZ EntityId Attr Variable
  -- TODO: Possible in the model, but requires iteration.
  -- | C_EYV EntityId Variable Value
  | LC_XAV Variable Attr Value

  | LC_EYZ EntityId Variable Variable
  | LC_XAZ Variable Attr Variable
  | LC_XYV Variable Variable Value
  deriving (Show)

-- All symbols bound by a clause
loadClauseBinds :: LoadClause -> Set Variable
loadClauseBinds (LC_EAZ _ _ z) = S.fromList [z]
-- C_EYV
loadClauseBinds (LC_XAV x _ _) = S.fromList [x]
loadClauseBinds (LC_EYZ _ y z) = S.fromList [y, z]
loadClauseBinds (LC_XAZ x _ z) = S.fromList [x, z]
loadClauseBinds (LC_XYV x y _) = S.fromList [x, y]

-- "Input" Clause: A new type of clause with everything in it.
data Clause
  = NotClause DataSource [Clause]
  | NotJoinClause DataSource [Variable] [Clause]
  | OrClause DataSource [OrClauseBody]
  | OrJoinClause -- TODO: This interacts with rule-vars in a weird way?

  -- The ExpressionClause
  | DataPattern LoadClause
  | PredicateExpression Predicate
  | FunctionExpression Func [FnArg] [Binding]
  | RuleExpression -- big question mark.
  deriving (Show)

clauseUses :: Clause -> Set Variable
clauseUses (DataPattern load)                         = loadClauseBinds load
clauseUses (PredicateExpression (PREDICATE x fnArgs)) =
  S.fromList $ join $ map fnArgToVariable fnArgs

-- Or and OrJoin have different rules around clauses. Ironically, the only
-- place you must explicitly state 'and' is inside an 'or'.
data OrClauseBody
  = OCB_CLAUSE Clause
  | OCB_AND_CLAUSES [Clause]
  deriving (Show)
