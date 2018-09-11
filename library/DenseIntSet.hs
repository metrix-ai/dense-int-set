module DenseIntSet
(
  -- * DenseIntSet
  DenseIntSet,
  -- ** Constructors
  foldable,
  topValueIndices,
  filteredIndices,
  -- *** Composition
  intersection,
  union,
  -- ** Accessors
  size,
  lookup,
  -- *** Vectors
  presentElementsVector,
  -- *** Unfoldrs
  presentElementsUnfoldr,
  absentElementsUnfoldr,
  -- * Composition
  DenseIntSetComposition,
  compose,
  composeList,
)
where

import DenseIntSet.Prelude hiding (intersection, union, lookup)
import qualified DeferredFolds.Unfoldr as Unfoldr
import qualified Data.Vector as Vector
import qualified Data.Vector.Unboxed as UnboxedVector
import qualified Data.Vector.Generic as GenericVector
import qualified Data.Vector.Generic.Mutable as MutableGenericVector
import qualified Data.Vector.Algorithms.Intro as IntroVectorAlgorithm


-- * DenseIntSet
-------------------------

{-|
Set of integer values represented as a space-optimized dense array of booleans,
where an entry only occupies 1 bit.
Compared to IntSet of the \"containers\" library,
it trades off the the ability for modification for much better lookup performance.
Hence it best fits the usage patterns, where you first create the set and then only use it for lookups.

Since there's multiple ways to implement a monoid for this data-structure,
the instances are provided for "DenseIntSetComposition", which
is open for interpretation of how to compose.
-}
newtype DenseIntSet = DenseIntSet (UnboxedVector Word64)

deriving instance Eq DenseIntSet

deriving instance Ord DenseIntSet

instance Show DenseIntSet where 
  show = show . toList

deriving instance Serialize DenseIntSet

instance Hashable DenseIntSet where
  hashWithSalt salt (DenseIntSet vec) = hashWithSalt salt (GenericVector.toList vec)

instance IsList DenseIntSet where
  type Item DenseIntSet = Int
  fromList list = foldable (foldl' max 0 list) list
  toList = toList . presentElementsUnfoldr


-- * Constructors
-------------------------

{-|
Given a maximum int, construct from a foldable of ints, which are smaller or equal to it.

It is your responsibility to ensure that the values match this contract.
-}
foldable :: Foldable foldable => Int -> foldable Int -> DenseIntSet
foldable size foldable = let
  !wordsAmount = divCeiling size 64
  in DenseIntSet $ runST $ do
    indexSetMVec <- MutableGenericVector.new wordsAmount
    forM_ foldable $ \ index -> let
      (wordIndex, bitIndex) = divMod index 64
      in MutableGenericVector.modify indexSetMVec (flip setBit bitIndex) wordIndex
    GenericVector.unsafeFreeze indexSetMVec

{-|
Interpret a composition as an intersection of sets.
-}
intersection :: DenseIntSetComposition -> DenseIntSet
intersection = zipWords (.&.) maxBound

{-|
Interpret a composition as a union of sets.
-}
union :: DenseIntSetComposition -> DenseIntSet
union = zipWords (.|.) 0

zipWords :: (Word64 -> Word64 -> Word64) -> Word64 -> DenseIntSetComposition -> DenseIntSet
zipWords append empty (DenseIntSetComposition minLength vecs) = DenseIntSet $ runST $ let
  wordIndexUnfoldr = Unfoldr.intsInRange 0 (pred minLength)
  vecVec = Vector.fromList vecs
  vecUnfoldr = Unfoldr.foldable vecVec
  wordUnfoldrAt wordIndex = fmap (flip GenericVector.unsafeIndex wordIndex) vecUnfoldr
  finalWordAt = foldr append empty . wordUnfoldrAt
  in do
    indexSetMVec <- MutableGenericVector.new minLength
    forM_ wordIndexUnfoldr $ \ index -> MutableGenericVector.unsafeWrite indexSetMVec index (finalWordAt index)
    GenericVector.unsafeFreeze indexSetMVec

{-|
Using the provided ordering function, select the indices of the specified amount of top-ordered elements of a generic vector.
-}
topValueIndices :: (GenericVector.Vector vector a, GenericVector.Vector vector (a, Int)) => (a -> a -> Ordering) -> Int -> vector a -> DenseIntSet
topValueIndices compare amount valueVec = let
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

{-|
Select the indices of vector elements, which match the predicate.
-}
filteredIndices :: GenericVector.Vector vector a => (a -> Bool) -> vector a -> DenseIntSet
filteredIndices predicate valueVec = DenseIntSet $ let
  valuesAmount = GenericVector.length valueVec
  wordsAmount = divCeiling valuesAmount 64
  indexUnfoldr = do
    (index, a) <- Unfoldr.vectorWithIndices valueVec
    guard (predicate a)
    return (divMod index 64)
  in runST $ do
    indexSetMVec <- MutableGenericVector.new wordsAmount
    forM_ indexUnfoldr $ \ (wordIndex, bitIndex) -> MutableGenericVector.modify indexSetMVec (flip setBit bitIndex) wordIndex
    GenericVector.unsafeFreeze indexSetMVec


-- * Accessors
-------------------------

{-|
/O(log n)/. Count the amount of present elements in the set.
-}
size :: DenseIntSet -> Int
size (DenseIntSet vec) = getSum (foldMap (Sum . popCount) (Unfoldr.vector vec))

{-|
/O(1)/. Check whether an int is a member of the set.
-}
lookup :: Int -> DenseIntSet -> Bool
lookup index (DenseIntSet vec) = let
  (wordIndex, bitIndex) = divMod index 64
  in vec GenericVector.!? wordIndex & maybe False (flip testBit bitIndex)


-- ** Vectors
-------------------------

{-|
Extract the present elements into a vector.
-}
presentElementsVector :: GenericVector.Vector vector Int => DenseIntSet -> vector Int
presentElementsVector intSet = let
  sizeVal = size intSet
  unfoldr = Unfoldr.zipWithIndex (presentElementsUnfoldr intSet)
  in runST $ do
    mv <- MutableGenericVector.unsafeNew sizeVal
    forM_ unfoldr $ \ (index, element) -> MutableGenericVector.unsafeWrite mv index element
    GenericVector.unsafeFreeze mv

{-|
Filter a vector, leaving only the entries, under the indices, which are in the set.

It is your responsibility to ensure that the indices in the set don't exceed the original vector's bounds.
-}
filterVector :: GenericVector.Vector vector a => DenseIntSet -> vector a -> vector a
filterVector set vector = let
  !newVectorSize = size set
  in runST $ do
    newVector <- MutableGenericVector.unsafeNew newVectorSize
    forM_ (Unfoldr.zipWithIndex (vectorElementsUnfoldr vector set)) $ \ (newIndex, a) -> do
      MutableGenericVector.unsafeWrite newVector newIndex a
    GenericVector.unsafeFreeze newVector


-- ** Unfoldr
-------------------------

{-|
Unfold the present elements.
-}
presentElementsUnfoldr :: DenseIntSet -> Unfoldr Int
presentElementsUnfoldr (DenseIntSet vec) = do
  (wordIndex, word) <- Unfoldr.vectorWithIndices vec
  bitIndex <- Unfoldr.setBitIndices word
  return (wordIndex * 64 + bitIndex)

{-|
Unfold the absent elements.
-}
absentElementsUnfoldr :: DenseIntSet -> Unfoldr Int
absentElementsUnfoldr (DenseIntSet vec) = do
  (wordIndex, word) <- Unfoldr.vectorWithIndices vec
  bitIndex <- Unfoldr.unsetBitIndices word
  return (wordIndex * 64 + bitIndex)

{-|
Unfold the elements of a vector by indices in the set.

It is your responsibility to ensure that the indices in the set don't exceed the vector's bounds.
-}
vectorElementsUnfoldr :: (GenericVector.Vector vector a) => vector a -> DenseIntSet -> Unfoldr a
vectorElementsUnfoldr vec = fmap (vec GenericVector.!) . presentElementsUnfoldr


-- * Composition
-------------------------

{-|
Abstraction over the composition of sets,
which is cheap to append and can be used for interpreted merging of sets.
-}
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

{-|
Lift a set into composition.
-}
compose :: DenseIntSet -> DenseIntSetComposition
compose (DenseIntSet vec) = DenseIntSetComposition (UnboxedVector.length vec) (pure vec)

{-|
Lift a list of sets into composition.
-}
composeList :: [DenseIntSet] -> DenseIntSetComposition
composeList list = if null list
  then mempty
  else let
    unboxedVec = fmap (\ (DenseIntSet x) -> x) list
    in DenseIntSetComposition (foldr1 min (fmap UnboxedVector.length unboxedVec)) unboxedVec
