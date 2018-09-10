module DenseIntSet
where

import DenseIntSet.Prelude
import qualified DeferredFolds.Unfoldr as Unfoldr
import qualified Data.Vector.Unboxed as UnboxedVector
import qualified Data.Vector.Generic as GenericVector
import qualified Data.Vector.Generic.Mutable as MutableGenericVector
import qualified Data.Vector.Algorithms.Intro as IntroVectorAlgorithm


-- * DenseIntSet
-------------------------

{-|
Since there's multiple ways to implement a monoid for this data-structure,
the instances are provided for "DenseIntSetComposition", which
is open for interpretation of how to compose.
-}
newtype DenseIntSet = DenseIntSet (UnboxedVector Word64)


-- * Constructors
-------------------------

intersection :: DenseIntSetComposition -> DenseIntSet
intersection (DenseIntSetComposition minLength vecs) = undefined

union :: DenseIntSetComposition -> DenseIntSet
union (DenseIntSetComposition minLength vecs) = undefined

topValueIndices :: (Ord a) => (GenericVector.Vector vector a, GenericVector.Vector vector (a, Int)) => Int -> vector a -> DenseIntSet
topValueIndices amount valueVec = let
  valuesAmount = GenericVector.length valueVec
  limitedAmount = min amount valuesAmount
  wordsAmount = divCeiling valuesAmount 64
  in runST $ do
    pairMVec <- GenericVector.unsafeThaw (GenericVector.imap (\ index count -> (count, index)) valueVec)
    IntroVectorAlgorithm.selectBy (\ a b -> compare (fst b) (fst a)) pairMVec limitedAmount
    indexSetMVec <- MutableGenericVector.new wordsAmount
    forM_ (Unfoldr.intsInRange 0 (pred limitedAmount)) $ \ pairIndex -> do
      (_, index) <- MutableGenericVector.unsafeRead pairMVec pairIndex
      let (wordIndex, bitIndex) = divMod index 64
      MutableGenericVector.modify indexSetMVec (flip setBit bitIndex) wordIndex
    DenseIntSet <$> GenericVector.unsafeFreeze indexSetMVec


-- * Accessors
-------------------------

size :: DenseIntSet -> Int
size (DenseIntSet vec) = getSum (foldMap (Sum . popCount) (Unfoldr.vector vec))


-- ** Vectors
-------------------------

presentElementsVector :: GenericVector.Vector vector Int => DenseIntSet -> vector Int
presentElementsVector intSet = let
  sizeVal = size intSet
  unfoldr = Unfoldr.zipWithIndex (presentElementsUnfoldr intSet)
  in runST $ do
    mv <- MutableGenericVector.unsafeNew sizeVal
    forM_ unfoldr $ \ (index, element) -> MutableGenericVector.unsafeWrite mv index element
    GenericVector.unsafeFreeze mv


-- ** Unfoldr
-------------------------

presentElementsUnfoldr :: DenseIntSet -> Unfoldr Int
presentElementsUnfoldr (DenseIntSet vec) = do
  (wordIndex, word) <- Unfoldr.vectorWithIndices vec
  bitIndex <- Unfoldr.setBitIndices word
  return (wordIndex * 64 + bitIndex)

absentElementsUnfoldr :: DenseIntSet -> Unfoldr Int
absentElementsUnfoldr (DenseIntSet vec) = do
  (wordIndex, word) <- Unfoldr.vectorWithIndices vec
  bitIndex <- Unfoldr.unsetBitIndices word
  return (wordIndex * 64 + bitIndex)


-- * Composition
-------------------------

data DenseIntSetComposition = DenseIntSetComposition !Int [UnboxedVector Word64]

instance Semigroup DenseIntSetComposition where
  (<>) (DenseIntSetComposition leftMinLength leftVecs) =
    if null leftVecs
      then id
      else \ (DenseIntSetComposition rightMinLength rightVecs) -> if null rightVecs
        then DenseIntSetComposition leftMinLength leftVecs
        else DenseIntSetComposition (min leftMinLength rightMinLength) (leftVecs <> rightVecs)

instance Monoid DenseIntSetComposition where
  mempty = DenseIntSetComposition 0 []
  mappend = (<>)

composed :: DenseIntSet -> DenseIntSetComposition
composed (DenseIntSet vec) = DenseIntSetComposition (UnboxedVector.length vec) (pure vec)

composedList :: [DenseIntSet] -> DenseIntSetComposition
composedList list = if null list
  then mempty
  else let
    bitVecList = fmap (\ (DenseIntSet x) -> x) list
    in DenseIntSetComposition (foldr1 min (fmap UnboxedVector.length bitVecList)) bitVecList
