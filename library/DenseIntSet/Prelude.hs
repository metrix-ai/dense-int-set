module DenseIntSet.Prelude
(
  module Exports,
  UnboxedVector,
  BitVector,
)
where


-- base
-------------------------
import Control.Applicative as Exports
import Control.Arrow as Exports
import Control.Category as Exports
import Control.Concurrent as Exports
import Control.Exception as Exports
import Control.Monad as Exports hiding (mapM_, sequence_, forM_, msum, mapM, sequence, forM)
import Control.Monad.IO.Class as Exports
import Control.Monad.Fix as Exports hiding (fix)
import Control.Monad.ST as Exports
import Data.Bits as Exports
import Data.Bool as Exports
import Data.Char as Exports
import Data.Coerce as Exports
import Data.Complex as Exports
import Data.Data as Exports
import Data.Dynamic as Exports
import Data.Either as Exports
import Data.Fixed as Exports
import Data.Foldable as Exports
import Data.Function as Exports hiding (id, (.))
import Data.Functor as Exports
import Data.Int as Exports
import Data.IORef as Exports
import Data.Ix as Exports
import Data.List as Exports hiding (sortOn, isSubsequenceOf, uncons, concat, foldr, foldl1, maximum, minimum, product, sum, all, and, any, concatMap, elem, foldl, foldr1, notElem, or, find, maximumBy, minimumBy, mapAccumL, mapAccumR, foldl')
import Data.Maybe as Exports
import Data.Monoid as Exports hiding (Last(..), First(..))
import Data.Ord as Exports
import Data.Proxy as Exports
import Data.Ratio as Exports
import Data.STRef as Exports
import Data.String as Exports
import Data.Traversable as Exports
import Data.Tuple as Exports
import Data.Unique as Exports
import Data.Version as Exports
import Data.Word as Exports
import Debug.Trace as Exports
import Foreign.ForeignPtr as Exports
import Foreign.Ptr as Exports
import Foreign.StablePtr as Exports
import Foreign.Storable as Exports
import GHC.Conc as Exports hiding (withMVar, threadWaitWriteSTM, threadWaitWrite, threadWaitReadSTM, threadWaitRead)
import GHC.Exts as Exports (lazy, inline, sortWith, groupWith)
import GHC.Generics as Exports (Generic)
import GHC.IO.Exception as Exports
import Numeric as Exports
import Prelude as Exports hiding (concat, foldr, mapM_, sequence_, foldl1, maximum, minimum, product, sum, all, and, any, concatMap, elem, foldl, foldr1, notElem, or, mapM, sequence, id, (.))
import System.Environment as Exports
import System.Exit as Exports
import System.IO as Exports
import System.IO.Error as Exports
import System.IO.Unsafe as Exports
import System.Mem as Exports
import System.Mem.StableName as Exports
import System.Timeout as Exports
import Text.ParserCombinators.ReadP as Exports (ReadP, ReadS, readP_to_S, readS_to_P)
import Text.ParserCombinators.ReadPrec as Exports (ReadPrec, readPrec_to_P, readP_to_Prec, readPrec_to_S, readS_to_Prec)
import Text.Printf as Exports (printf, hPrintf)
import Text.Read as Exports (Read(..), readMaybe, readEither)
import Unsafe.Coerce as Exports

-- deferred-folds
-------------------------
import DeferredFolds.Unfoldl as Exports (Unfoldl(..))
import DeferredFolds.UnfoldlM as Exports (UnfoldlM(..))
import DeferredFolds.Unfoldr as Exports (Unfoldr(..))

-- hashable
-------------------------
import Data.Hashable as Exports (Hashable(..))

-- vector
-------------------------
import Data.Vector as Exports (Vector)

-- cereal
-------------------------
import Data.Serialize as Exports (Serialize)

-- cereal-vector
-------------------------
import Data.Vector.Serialize ()

-- bitvec
-------------------------
import Data.Bit as Exports (Bit)

-- 
-------------------------
import qualified Data.Vector.Unboxed.Bit as BitVec
import qualified Data.Vector.Unboxed as UnboxedVector


type UnboxedVector = UnboxedVector.Vector

type BitVector = UnboxedVector Bit
