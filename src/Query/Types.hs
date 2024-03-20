{-# LANGUAGE Strict     #-}
{-# LANGUAGE StrictData #-}
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
  = ERowIndex !(TreeIndex e (EDatomRow e a v tx))
              (Int, [(e, a, v, tx, Bool)])
  | ELeaf !(Map e (ADatomRow a v tx))
  deriving (Show, Generic, NFData)

-- TODO: Think about data locality. A lot. Right now VStorage's indirection
-- means that vs aren't stored next to each other when doing a simple a scan.
data ADatomRow a v tx
  = ARowIndex !(TreeIndex a (ADatomRow a v tx))
              (Int, [(a, v, tx, Bool)])
  | ALeaf !(Map a (VStorage v tx))
  deriving (Show, Generic, NFData)

-- An append only, backwards list that keeps track of the first transaction
-- number (the terminal item of the list)
data TxHistory v tx = TxHistory !tx ![(tx, v, Bool)]
  deriving (Show)

instance NFData (TxHistory v tx) where
  rnf !_ = ()

-- At the end is VStorage: a set copy of the current existing values, and a
-- separate log of transactions.
data VStorage v tx
  -- Many values are going to be a single value that doesn't change; don't
  -- allocate two hitchhiker trees to deal with them, just inline.
  = VSimple !v !tx
  -- We have had multiple transactions which have asserted or redacted values.
  -- Keep track of the current set, plus a historical log of all assertions or
  -- redactions that can be replayed.
  | VStorage !(Maybe (HitchhikerSetNode v)) !(TxHistory v tx)
  deriving (Show)

instance (NFData (HitchhikerSetNode v)) => NFData (VStorage v tx) where
  rnf !(VSimple !v !tx) = ()
  rnf !(VStorage x !y)  = rnf x


data EAVRows e a v tx = EAVROWS {
  config :: TreeConfig,
  root   :: Maybe (EDatomRow e a v tx)
  }
  deriving (Show, Generic, NFData)

-- TODO: Making this


data Database = DATABASE {
  nextTransaction :: Int,
  nextEntity      :: Int,
  attributes      :: Map Text EntityId,
  attributeProps  :: Map EntityId (Bool, Cardinality, ValueType),

  eav             :: !(EAVRows EntityId EntityId Value Int),
  aev             :: !(EAVRows EntityId EntityId Value Int),
  ave             :: !(EAVRows EntityId Value EntityId Int),
  vae             :: !(EAVRows Value EntityId EntityId Int)
  }
  deriving (Show, Generic, NFData)

-- -----------------------------------------------------------------------

newtype EntityId = ENTID Int
  deriving (Show, Eq, Ord)

instance NFData EntityId where
  rnf !_ = ()

-- The attribute type
newtype Attr = ATTR Text
  deriving (Show, Eq, Ord)

data Value
  = VAL_ENTID {-# UNPACK #-} !EntityId
  | VAL_INT {-# UNPACK #-} !Int
  | VAL_STR {-# UNPACK #-} !String
  deriving (Show, Eq, Ord)

-- Adding this consistently drops runtime from 10.5s to ~6s.
instance NFData Value where
  rnf !_ = ()

instance IsString Value where
  fromString = VAL_STR

data ValueType
  = VT_ENTITY
  | VT_INT
  | VT_STR
  deriving (Show, Eq, Generic, NFData)

data Cardinality
  = ONE
  | MANY
  deriving (Show, Generic, NFData)

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
  deriving (Show,Enum,Bounded)

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
  deriving (Show, Eq)

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
  -- TODO: Actually, in practice, queries use the two symbols one blank form.

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
  | NotJoinClause DataSource (Set Variable) [Clause]
  | OrClause DataSource [OrClauseBody]
  | OrJoinClause -- TODO: This interacts with rule-vars in a weird way?

  -- The ExpressionClause
  | DataPattern LoadClause

  -- The simpler predicate for building things out.
  --
  -- TODO: Enable full blows PredicateExpression with unbounded arity later.
  | BiPredicateExpression FnArg BuiltinPred FnArg
  -- | PredicateExpression Predicate

  | FunctionExpression Func [FnArg] [Binding]
  | RuleExpression -- big question mark.
  deriving (Show)

clauseUses :: Clause -> Set Variable
clauseUses (NotClause _ clauses) = S.unions $ map clauseUses clauses
clauseUses (OrClause _ orclauses) = S.unions $ map orClauseUses orclauses
clauseUses (DataPattern load)                         = loadClauseBinds load
clauseUses (BiPredicateExpression a _ b) =
  S.fromList $ join [fnArgToVariable a, fnArgToVariable b]
-- clauseUses (PredicateExpression (PREDICATE x fnArgs)) =
--   S.fromList $ join $ map fnArgToVariable fnArgs

-- Or and OrJoin have different rules around clauses. Ironically, the only
-- place you must explicitly state 'and' is inside an 'or'.
data OrClauseBody
  = OCB_CLAUSE Clause
  | OCB_AND_CLAUSES [Clause]
  deriving (Show)

orClauseUses :: OrClauseBody -> Set Variable
orClauseUses (OCB_CLAUSE clause)  = clauseUses clause
orClauseUses (OCB_AND_CLAUSES cs) = S.unions $ map clauseUses cs

orClauseToClauseList :: OrClauseBody -> [Clause]
orClauseToClauseList (OCB_CLAUSE clause)       = [clause]
orClauseToClauseList (OCB_AND_CLAUSES clauses) = clauses

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
  LoadSet :: RowLookup -> Value -> Value -> Variable -> Plan RelSet
  LoadTab :: RowLookup -> Value -> Variable -> Variable -> Plan RelTab

  -- Not operations
  SetDifference :: Variable -> Plan RelSet -> Plan RelSet -> Plan RelSet

  TabScalarLookup :: Variable -> Plan RelScalar
                  -> Variable -> Plan RelTab -> Plan RelSet
  TabSetUnionVals :: Variable -> Plan RelSet
                  -> Variable -> Plan RelTab -> Plan RelSet
  TabRestrictKeys :: Variable -> Variable
                  -> Plan RelTab -> Plan RelSet -> Plan RelTab

  FilterPredTabKeys :: [PlanBiPred] -> Plan RelTab -> Plan RelTab
  FilterPredTabVals :: [PlanBiPred] -> Plan RelTab -> Plan RelTab
  -- The big hammer. Given a list of key preds and value preds, while
  -- performing the iteration to restrict a tab by a set of keys, also perform
  -- the following predicates. Rolling these operations into a single traversal
  -- saves runtime; in this case, you're joining a restricted set with a almost
  -- complete tab from ?e to the number you're predicating on.
  TabRestrictKeysVals :: Variable -> Variable
                      -> [PlanBiPred] {- filter vals -}
                      -> Plan RelTab -> Plan RelSet -> Plan RelTab

  TabKeySet :: Variable -> Variable -> Plan RelTab -> Plan RelSet

  SetIntersect :: Variable -> Plan RelSet -> Plan RelSet -> Plan RelSet
  SetUnion :: Variable -> Plan RelSet -> Plan RelSet -> Plan RelSet
  SetScalarJoin :: Plan RelSet -> Plan RelScalar -> Plan RelSet

  MkMultiTab :: Plan RelTab -> Plan RelTab -> Plan RelMultiTab
  AddToMultiTab :: Plan RelTab -> Plan RelMultiTab -> Plan RelMultiTab

  -- Sometimes, you can't do anything but fallback to stupid rows...including
  -- in output.
  SetToRows :: Variable -> Plan RelSet -> Plan Rows
  TabToRows :: Variable -> Variable -> Plan RelTab -> Plan Rows

  MultiTabToRows :: [Variable] -> Plan RelMultiTab -> Plan Rows

instance Show (Plan a) where
  show (InputScalar sym int) = "InputScalar " <> show sym <> " " <> show int
  show (InputSet sym int) = "InputSet " <> show sym <> " " <> show int

  show (LoadSet rl val1 val2 var) =
    "LoadSet " <> show rl <> " (" <> show val1 <> ") (" <> show val2 <>
    ") (" <> show var <> ")"
  show (LoadTab rl val from to) = "LoadTab " <> show rl <> " (" <> show val <>
                                  ") (" <> show from <> ") (" <> show to <> ")"

  show (SetDifference var l r) = "SetDifference " <> show var <> " (" <> show l
                                 <> ") (" <> show r <> ")"

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
  show (FilterPredTabKeys preds rTab) =
    "FilterPredTabKeys (" <> show preds <> ") (" <>
    show rTab <> ")"
--        :: Value -> BuiltinPred -> Plan RelTab -> Plan RelTab

  show (FilterPredTabVals preds rTab) =
    "FilterPredTabVals (" <> show preds <> ") (" <> show rTab <> ")"

  show (SetIntersect key a b) = "SetIntersect (" <> show key <> ") (" <> show a <>
    ") (" <> show b <> ")"
  show (SetUnion key a b) = "SetUnion (" <> show key <> ") (" <> show a <>
    ") (" <> show b <> ")"

  show (SetScalarJoin a b) = "SetScalarJoin (" <> show a <> ") (" <> show b <>
                             ")"
  show (MkMultiTab a b) = "MkMultiTab (" <> show a <> ") (" <> show b <> ")"
  show (AddToMultiTab a b) = "AddToMultiTab (" <> show a <> ") (" <> show b <> ")"

  show (SetToRows key s) = "SetToRows (" <> show key <> ") (" <> show s <> ")"
  show (TabToRows key val t) = "TabToRows (" <> show key <> ") (" <> show val
                            <> ") (" <> show t <> ")"

  show (MultiTabToRows req mtab) = "MultiTabToRows ("
                                <> show req <> ") ("
                                <> show mtab <> ")"

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
