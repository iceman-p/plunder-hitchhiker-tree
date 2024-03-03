module Query.Types where

import           ClassyPrelude

import           Impl.Types
import           Types

import           Data.Sorted

import qualified Data.Kind

import qualified HitchhikerSet    as HS
import qualified HitchhikerSetMap as HSM

import qualified Data.Set         as S

-- -----------------------------------------------------------------------
-- Hitchhiker set types
-- -----------------------------------------------------------------------
-- Our database is built on a trie of datoms stored in a hitchhiker tree.

-- What are we building here? What's the purpose? We're making a hitchhiker
-- tree variant where for any six tuple, such as [e a v tx o], we have an
-- efficient way to minimize churn in the upper levels of the tree.
--
-- You need to think of this as a way to map [e a] -> [%{v}  [tx o]], with
-- hitchhiker entries [d e a v tx o] pushed down (or in some situations used
-- for quick short circuits on cardinality once).

-- As a convention in this file, all types are of the form [e a v tx o] even
-- though the code is written so it can be reused for the ave or vea indexes,
-- too.

data EDatomRow e a v tx
  = ERowIndex (TreeIndex e (EDatomRow e a v tx))
              (Map e (ArraySet (a, v, tx, Bool)))
  | ELeaf (Map e (ADatomRow a v tx))
  deriving (Show, Generic, NFData)

-- TODO: Think about data locality. A lot. Right now VStorage's indirection
-- means that vs aren't stored next to each other when doing a simple a scan.
data ADatomRow a v tx
  = ARowIndex (TreeIndex a (ADatomRow a v tx))
              (Map a (ArraySet (v, tx, Bool)))
  | ALeaf (Map a (VStorage v tx))
  deriving (Show, Generic, NFData)

-- At the end is VStorage: a set copy of the current existing values, and a
-- separate log of transactions.
data VStorage v tx
  -- Many values are going to be a single value that doesn't change; don't
  -- allocate two hitchhiker trees to deal with them, just inline.
  = VSimple v tx
  -- We have multiple
  | VStorage (Maybe (HitchhikerSetNode v)) (HitchhikerSetMapNode tx (v, Bool))
  deriving (Show, Generic, NFData)

data EAVRows e a v tx = EAVROWS {
  config :: TreeConfig,
  root   :: Maybe (EDatomRow e a v tx)
  }
  deriving (Show)


type EAVStore = EAVRows Int Int Value Int

data Database = DATABASE {
  -- TODO: More restricted types.
  eav :: EAVRows Value Value Value Int,
  aev :: EAVRows Value Value Value Int,
  ave :: EAVRows Value Value Value Int,
  vae :: EAVRows Value Value Value Int

  -- eav :: EAVRows Int Attr Value Int,
  -- aev :: EAVRows Attr Int Value Int,
  -- ave :: EAVRows Attr Value Int Int,
  -- vae :: EAVRows Value Attr Int Int
  }
  deriving (Show)

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
fnArgToVariable _           = []

data BuiltinPred
  = B_LT
  | B_LTE
  | B_EQ
  | B_GTE
  | B_GT
  deriving (Show)

builtinPredToCompare :: Ord a => BuiltinPred -> (a -> a -> Bool)
builtinPredToCompare B_LT  = (<)
builtinPredToCompare B_LTE = (<=)
builtinPredToCompare B_EQ  = (==)
builtinPredToCompare B_GTE = (>=)
builtinPredToCompare B_GT  = (>)

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

-- A unipredicate takes one value
data PlanUniPredicate = PUPRED { arg :: Variable
                               , fun :: Value -> Bool }

instance Show PlanUniPredicate where
  show (PUPRED args _) = "PUPRED " <> show args

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
  | REL_ROWS Rows
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

  -- The simpler predicate for building things out.
  --
  -- TODO: Enable full blows PredicateExpression with unbounded arity later.
  | BiPredicateExpression BuiltinPred FnArg FnArg
  -- | PredicateExpression Predicate

  | FunctionExpression Func [FnArg] [Binding]
  | RuleExpression -- big question mark.
  deriving (Show)

clauseUses :: Clause -> Set Variable
clauseUses (DataPattern load)                         = loadClauseBinds load
clauseUses (BiPredicateExpression _ a b) =
  S.fromList $ join [fnArgToVariable a, fnArgToVariable b]
-- clauseUses (PredicateExpression (PREDICATE x fnArgs)) =
--   S.fromList $ join $ map fnArgToVariable fnArgs

-- Or and OrJoin have different rules around clauses. Ironically, the only
-- place you must explicitly state 'and' is inside an 'or'.
data OrClauseBody
  = OCB_CLAUSE Clause
  | OCB_AND_CLAUSES [Clause]
  deriving (Show)

data RulePack

-- -----------------------------------------------------------------------

data PlanBiPred
  = PBP_LEFT (Plan RelScalar) BuiltinPred
  | PBP_RIGHT BuiltinPred (Plan RelScalar)
  deriving (Show)

-- Plan the steps to evaluate the query. This is the intermediate form of a
-- query: it's dumpable to the console for debugging.
data Plan :: Data.Kind.Type -> Data.Kind.Type where
  -- Inputs: loads data into the rows.
  InputScalar :: Variable -> Int -> Plan RelScalar
  InputSet    :: Variable -> Int -> Plan RelSet
  InputConst :: Value -> Plan RelScalar
  LoadTab :: RowLookup -> Value -> Variable -> Variable -> Plan RelTab

  TabScalarLookup :: Variable -> Plan RelScalar
                  -> Variable -> Plan RelTab -> Plan RelSet
  TabSetUnionVals :: Variable -> Plan RelSet
                  -> Variable -> Plan RelTab -> Plan RelSet
  TabRestrictKeys :: Variable -> Variable
                  -> Plan RelTab -> Plan RelSet -> Plan RelTab

  -- The big hammer. Given a list of key preds and value preds, while
  -- performing the iteration to restrict a tab by a set of keys, also perform
  -- the following predicates.
  TabRestrictKeysVals :: Variable -> Variable
                      -> [PlanBiPred] {- filter vals -}
                      -> Plan RelTab -> Plan RelSet -> Plan RelTab

  -- TabRestrictKeysValPred :: Variable -> Variable
  --                        -
  --                        -> Plan RelTab -> Plan RelSet -> PlanRelTab

  TabKeySet :: Variable -> Variable -> Plan RelTab -> Plan RelSet

  -- TODO: Change these values into (Plan RelScalar)
  FilterPredTabKeysL :: Plan RelScalar -> BuiltinPred -> Plan RelTab
                     -> Plan RelTab
  FilterPredTabKeysR :: Plan RelTab -> BuiltinPred -> Plan RelScalar
                     -> Plan RelTab

  SetJoin :: Variable -> Plan RelSet -> Plan RelSet -> Plan RelSet
  SetScalarJoin :: Plan RelSet -> Plan RelScalar -> Plan RelSet

  MkMultiTab :: Plan RelTab -> Plan RelTab -> Plan RelMultiTab

  -- Sometimes, you can't do anything but fallback to stupid rows.
  SetToRows :: Variable -> Plan RelSet -> Plan Rows

instance Show (Plan a) where
  show (InputScalar sym int) = "InputScalar " <> show sym <> " " <> show int
  show (InputSet sym int) = "InputSet " <> show sym <> " " <> show int
  show (LoadTab rl val from to) = "LoadTab " <> show rl <> " (" <> show val <>
                                  ") (" <> show from <> ") (" <> show to <> ")"
  show (TabScalarLookup from set to tab) =
    "TabScalarLookup (" <> show from <> ") (" <> show set <> ") (" <>
    show to <> ") (" <> show tab <> ")"
  show (TabSetUnionVals from set to tab) =
    "TabSetUnionVals (" <> show from <> ") (" <> show set <> ") (" <>
    show to <> ") (" <> show tab <> ")"
  show (TabRestrictKeys from to tab set) =
    "TabRestrictKeys (" <> show from <> ") (" <> show to <> ") (" <>
    show tab <> ") (" <> show set <> ")"
  show (TabKeySet from to tab) = "TabKeySet (" <> show from <> ") (" <>
    show to <> ") (" <> show tab <> ")"

  show (TabRestrictKeysVals from to preds tab set) = "TabRestrictKeysVals (" <>
    show from <> ") (" <> show preds <> ") (" <>
    show to <> ") (" <> show tab <> ") (" <> show set <> ")"

  -- :: Value -> BuiltinPred -> Plan RelTab -> Plan RelTab
  show (FilterPredTabKeysL lConst pred rTab) =
    "FilterPredTabKeysL (" <> show lConst <> ") (" <> show pred <> ") (" <>
    show rTab <> ")"
--        :: Value -> BuiltinPred -> Plan RelTab -> Plan RelTab

  show (SetJoin key a b) = "SetJoin (" <> show key <> ") (" <> show a <>
    ") (" <> show b <> ")"
  show (SetScalarJoin a b) = "SetScalarJoin (" <> show a <> ") (" <> show b <>
                             ")"
  show (MkMultiTab a b) = "MkMultiTab (" <> show a <> ") (" <> show b <> ")"

-- Type to hold the different possible Plan types while maintaining what the
-- set of variables this plan operates under.
data PlanHolder
  = PH_SCALAR Variable (Plan RelScalar)
  | PH_SET Variable (Plan RelSet)
  | PH_TAB Variable Variable (Plan RelTab)
  | PH_ROWS [Variable] [Variable] (Plan Rows)
  | PH_MULTITAB Variable [Variable] (Plan RelMultiTab)
  deriving (Show)

planHolderBinds :: PlanHolder -> Set Variable
planHolderBinds (PH_SCALAR s _)          = S.singleton s
planHolderBinds (PH_SET s _)             = S.singleton s
planHolderBinds (PH_TAB from to _)       = S.fromList [from, to]
planHolderBinds (PH_ROWS vars _ _)       = S.fromList vars
planHolderBinds (PH_MULTITAB from tos _) = S.fromList (from:tos)

-- -----------------------------------------------------------------------
