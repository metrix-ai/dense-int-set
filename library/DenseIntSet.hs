module DenseIntSet
where

import DenseIntSet.Prelude
import qualified Data.Vector.Unboxed as UnboxedVector
import qualified Data.Vector.Unboxed.Bit as BitVector


-- * IntSet
-------------------------

{-|
Since there's multiple ways to implement a monoid for this data-structure,
the instances are provided for "IntSetComposition", which
is open for interpretation of how to compose.
-}
newtype IntSet = IntSet BitVector

intersection :: IntSetComposition -> IntSet
intersection (IntSetComposition minLength vecs) = IntSet (BitVector.intersections minLength vecs)

union :: IntSetComposition -> IntSet
union (IntSetComposition minLength vecs) = IntSet (BitVector.unions minLength vecs)


-- * Composition
-------------------------

data IntSetComposition = IntSetComposition !Int [BitVector]

instance Semigroup IntSetComposition where
  (<>) (IntSetComposition leftMinLength leftVecs) =
    if null leftVecs
      then id
      else \ (IntSetComposition rightMinLength rightVecs) -> if null rightVecs
        then IntSetComposition leftMinLength leftVecs
        else IntSetComposition (min leftMinLength rightMinLength) (leftVecs <> rightVecs)

instance Monoid IntSetComposition where
  mempty = IntSetComposition 0 []
  mappend = (<>)

composed :: IntSet -> IntSetComposition
composed (IntSet vec) = IntSetComposition (BitVector.length vec) (pure vec)

composedList :: [IntSet] -> IntSetComposition
composedList list = if null list
  then mempty
  else let
    bitVecList = fmap (\ (IntSet x) -> x) list
    in IntSetComposition (foldr1 min (fmap BitVector.length bitVecList)) bitVecList
