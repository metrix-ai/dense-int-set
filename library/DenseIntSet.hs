module DenseIntSet
(
  -- * DenseIntSet
  DenseIntSet,
  -- ** Constructors
  foldable,
  topValueIndices,
  filteredIndices,
  invert,
  -- *** Composition
  intersections,
  unions,
  -- ** Accessors
  capacity,
  population,
  lookup,
  -- *** Vectors
  presentElementsVector,
  indexVector,
  filterVector,
  -- *** Unfoldrs
  presentElementsUnfoldr,
  absentElementsUnfoldr,
  vectorElementsUnfoldr,
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
data DenseIntSet = DenseIntSet {-# UNPACK #-} !Int {-# UNPACK #-} !(UnboxedVector Word64)

deriving instance Eq DenseIntSet

deriving instance Ord DenseIntSet

instance Show DenseIntSet where 
  show = show . toList

deriving instance Generic DenseIntSet

instance Serialize DenseIntSet

instance Hashable DenseIntSet where
  hashWithSalt salt (DenseIntSet capacity vec) = hashWithSalt (hashWithSalt salt capacity) (GenericVector.toList vec)

instance IsList DenseIntSet where
  type Item DenseIntSet = Int
  fromList list = foldable (succ (foldl' max 0 list)) list
  toList = toList . presentElementsUnfoldr


-- * Constructors
-------------------------

{-|
Given a maximum int, construct from a foldable of ints, which are smaller or equal to it and larger than 0.

It is your responsibility to ensure that the values match this contract.
-}
foldable :: Foldable foldable => Int -> foldable Int -> DenseIntSet
foldable capacity foldable = let
  !wordsAmount = divCeiling capacity 64
  in DenseIntSet capacity $ runST $ do
    indexSetMVec <- MutableGenericVector.new wordsAmount
    forM_ foldable $ \ index -> let
      (wordIndex, bitIndex) = divMod index 64
      in MutableGenericVector.modify indexSetMVec (flip setBit bitIndex) wordIndex
    GenericVector.unsafeFreeze indexSetMVec

{-| Intersect multiple sets -}
intersections :: [DenseIntSet] -> DenseIntSet
intersections list = if null list
  then DenseIntSet 0 mempty
  else let
    cap = foldl1' min (fmap capacity list)
    vecs = fmap (\ (DenseIntSet _ vec) -> vec) list
    in compositions (.&.) maxBound cap vecs

{-| Unite multiple sets -}
unions :: [DenseIntSet] -> DenseIntSet
unions list = let
  cap = foldl' max 0 (fmap capacity list)
  vecs = fmap (\ (DenseIntSet _ vec) -> vec) list
  in compositions (.|.) 0 cap vecs

compositions :: (Word64 -> Word64 -> Word64) -> Word64 -> Int -> [UnboxedVector Word64] -> DenseIntSet
compositions append empty capacity vecs = DenseIntSet capacity $ runST $ let
  wordsAmount = divCeiling capacity 64
  wordIndexUnfoldr = Unfoldr.intsInRange 0 (pred wordsAmount)
  vecVec = Vector.fromList vecs
  vecUnfoldr = Unfoldr.foldable vecVec
  wordUnfoldrAt wordIndex = vecUnfoldr >>= Unfoldr.foldable . flip (GenericVector.!?) wordIndex
  finalWordAt = foldr append empty . wordUnfoldrAt
  in do
    indexSetMVec <- MutableGenericVector.new wordsAmount
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
    DenseIntSet valuesAmount <$> GenericVector.unsafeFreeze indexSetMVec

{-|
Select the indices of vector elements, which match the predicate.
-}
filteredIndices :: GenericVector.Vector vector a => (a -> Bool) -> vector a -> DenseIntSet
filteredIndices predicate valueVec = let
  valuesAmount = GenericVector.length valueVec
  wordsAmount = divCeiling valuesAmount 64
  indexUnfoldr = do
    (index, a) <- Unfoldr.vectorWithIndices valueVec
    guard (predicate a)
    return (divMod index 64)
  in DenseIntSet valuesAmount $ runST $ do
    indexSetMVec <- MutableGenericVector.new wordsAmount
    forM_ indexUnfoldr $ \ (wordIndex, bitIndex) -> MutableGenericVector.modify indexSetMVec (flip setBit bitIndex) wordIndex
    GenericVector.unsafeFreeze indexSetMVec

{-|
Invert the set.
-}
invert :: DenseIntSet -> DenseIntSet
invert (DenseIntSet capacity vec) = DenseIntSet capacity $ let
  invertedVec = GenericVector.map complement vec
  (lastWordIndex, claimedBitsOfLastWord) = divMod capacity 64
  unclaimedBitsOfLastWord = 64 - claimedBitsOfLastWord
  in if claimedBitsOfLastWord == 0
    then invertedVec
    else runST $ do
      mv <- GenericVector.unsafeThaw invertedVec
      flip (MutableGenericVector.modify mv) lastWordIndex $ flip shiftR unclaimedBitsOfLastWord . flip shiftL unclaimedBitsOfLastWord
      GenericVector.unsafeFreeze mv


-- * Accessors
-------------------------

{-|
/O(1)/. Size of the value space.
-}
capacity :: DenseIntSet -> Int
capacity (DenseIntSet x _) = x

{-|
/O(log n)/. Count the amount of present elements in the set.
-}
population :: DenseIntSet -> Int
population (DenseIntSet _ vec) = getSum (foldMap (Sum . popCount) (Unfoldr.vector vec))

{-|
/O(1)/. Check whether an int is a member of the set.
-}
lookup :: Int -> DenseIntSet -> Bool
lookup index (DenseIntSet _ vec) = let
  (wordIndex, bitIndex) = divMod index 64
  in vec GenericVector.!? wordIndex & maybe False (flip testBit bitIndex)


-- ** Vectors
-------------------------

{-|
Extract the present elements into a vector.
-}
presentElementsVector :: GenericVector.Vector vector element => DenseIntSet -> (Int -> element) -> vector element
presentElementsVector intSet intToIndex = let
  vectorPop = population intSet
  unfoldr = Unfoldr.zipWithIndex (presentElementsUnfoldr intSet)
  in runST $ do
    mv <- MutableGenericVector.unsafeNew vectorPop
    forM_ unfoldr $ \ (index, element) -> MutableGenericVector.unsafeWrite mv index (intToIndex element)
    GenericVector.unsafeFreeze mv

{-|
Construct a vector, which maps from the original ints into their indices amongst the ones present in the set.
-}
indexVector :: GenericVector.Vector vector (Maybe index) => DenseIntSet -> (Int -> index) -> vector (Maybe index)
indexVector set@(DenseIntSet capacity setVec) intToIndex = runST $ do
  v <- MutableGenericVector.replicate capacity Nothing
  forM_ (Unfoldr.zipWithIndex (presentElementsUnfoldr set)) $ \ (index, element) -> do
    MutableGenericVector.unsafeWrite v element (Just (intToIndex index))
  GenericVector.unsafeFreeze v

{-|
Filter a vector, leaving only the entries, under the indices, which are in the set.

It is your responsibility to ensure that the indices in the set don't exceed the original vector's bounds.
-}
filterVector :: GenericVector.Vector vector a => DenseIntSet -> vector a -> vector a
filterVector set vector = let
  !newVectorPop = population set
  in runST $ do
    newVector <- MutableGenericVector.unsafeNew newVectorPop
    forM_ (Unfoldr.zipWithIndex (vectorElementsUnfoldr vector set)) $ \ (newIndex, a) -> do
      MutableGenericVector.unsafeWrite newVector newIndex a
    GenericVector.unsafeFreeze newVector


-- ** Unfoldr
-------------------------

{-|
Unfold the present elements.
-}
presentElementsUnfoldr :: DenseIntSet -> Unfoldr Int
presentElementsUnfoldr (DenseIntSet capacity vec) = do
  (wordIndex, word) <- Unfoldr.vectorWithIndices vec
  bitIndex <- Unfoldr.setBitIndices word
  return (wordIndex * 64 + bitIndex)

{-|
Unfold the absent elements.
-}
absentElementsUnfoldr :: DenseIntSet -> Unfoldr Int
absentElementsUnfoldr (DenseIntSet capacity vec) = let
  isLastWordIndex = let
    !maxWordIndex = pred (divCeiling capacity 64)
    in \ wordIndex -> wordIndex == maxWordIndex
  in do
    (wordIndex, word) <- Unfoldr.vectorWithIndices vec
    let
      wordElemIndex = wordIndex * 64
      in if isLastWordIndex wordIndex
        then let
          !bitsLeft = capacity - wordElemIndex
          predicate bitIndex = bitIndex < bitsLeft
          in fmap (wordElemIndex +) (Unfoldr.takeWhile predicate (Unfoldr.unsetBitIndices word))
        else fmap (wordElemIndex +) (Unfoldr.unsetBitIndices word)

{-|
Unfold the elements of a vector by indices in the set.

It is your responsibility to ensure that the indices in the set don't exceed the vector's bounds.
-}
vectorElementsUnfoldr :: (GenericVector.Vector vector a) => vector a -> DenseIntSet -> Unfoldr a
vectorElementsUnfoldr vec = fmap (vec GenericVector.!) . presentElementsUnfoldr

