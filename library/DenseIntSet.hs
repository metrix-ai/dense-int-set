module DenseIntSet
where

import DenseIntSet.Prelude
import qualified DeferredFolds.Unfoldr as Unfoldr
import qualified Data.Vector.Unboxed as UnboxedVector
import qualified Data.Vector.Generic as GenericVector
import qualified Data.Vector.Generic.Mutable as MutableGenericVector
import qualified Data.Vector.Algorithms.Intro as IntroVectorAlgorithm


-- * IntSet
-------------------------

{-|
Since there's multiple ways to implement a monoid for this data-structure,
the instances are provided for "IntSetComposition", which
is open for interpretation of how to compose.
-}
newtype IntSet = IntSet (UnboxedVector Word64)


-- * Constructors
-------------------------

intersection :: IntSetComposition -> IntSet
intersection (IntSetComposition minLength vecs) = undefined

union :: IntSetComposition -> IntSet
union (IntSetComposition minLength vecs) = undefined


-- * Accessors
-------------------------

size :: IntSet -> Int
size (IntSet vec) = getSum (foldMap (Sum . popCount) (Unfoldr.vector vec))


-- ** Vectors
-------------------------

presentElementsVector :: GenericVector.Vector vector Int => IntSet -> vector Int
presentElementsVector intSet = let
  sizeVal = size intSet
  unfoldr = Unfoldr.zipWithIndex (presentElementsUnfoldr intSet)
  in runST $ do
    mv <- MutableGenericVector.unsafeNew sizeVal
    forM_ unfoldr $ \ (index, element) -> MutableGenericVector.unsafeWrite mv index element
    GenericVector.unsafeFreeze mv


-- ** Unfoldr
-------------------------

presentElementsUnfoldr :: IntSet -> Unfoldr Int
presentElementsUnfoldr (IntSet vec) = do
  (wordIndex, word) <- Unfoldr.vectorWithIndices vec
  bitIndex <- Unfoldr.setBitIndices word
  return (wordIndex * 64 + bitIndex)

absentElementsUnfoldr :: IntSet -> Unfoldr Int
absentElementsUnfoldr (IntSet vec) = do
  (wordIndex, word) <- Unfoldr.vectorWithIndices vec
  bitIndex <- Unfoldr.unsetBitIndices word
  return (wordIndex * 64 + bitIndex)


-- * Composition
-------------------------

data IntSetComposition = IntSetComposition !Int [UnboxedVector Word64]

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
composed (IntSet vec) = IntSetComposition (UnboxedVector.length vec) (pure vec)

composedList :: [IntSet] -> IntSetComposition
composedList list = if null list
  then mempty
  else let
    bitVecList = fmap (\ (IntSet x) -> x) list
    in IntSetComposition (foldr1 min (fmap UnboxedVector.length bitVecList)) bitVecList
