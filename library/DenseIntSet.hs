module DenseIntSet
where

import DenseIntSet.Prelude
import qualified Data.Vector.Unboxed as UnboxedVector
import qualified Data.Vector.Unboxed.Bit as BitVec


-- * IntSet
-------------------------

{-|
Since there's multiple ways to implement a monoid for this data-structure,
the instances are provided for the specific wrappers like "IntSetIntersection" or "IntSetUnion".
All of those types are interconvertible.
-}
newtype IntSet = IntSet BitVector

intersection :: IntSetIntersection -> IntSet
intersection (IntSetIntersection minLength vecs) = IntSet (BitVec.intersections minLength vecs)

union :: IntSetUnion -> IntSet
union (IntSetUnion minLength vecs) = IntSet (BitVec.unions minLength vecs)


-- * Intersection composition
-------------------------

data IntSetIntersection = IntSetIntersection !Int [BitVector]

instance Semigroup IntSetIntersection where
  (<>) (IntSetIntersection leftMinLength leftVecs) =
    if null leftVecs
      then id
      else \ (IntSetIntersection rightMinLength rightVecs) -> if null rightVecs
        then IntSetIntersection leftMinLength leftVecs
        else IntSetIntersection (min leftMinLength rightMinLength) (leftVecs <> rightVecs)

instance Monoid IntSetIntersection where
  mempty = IntSetIntersection 0 []
  mappend = (<>)

intersected :: IntSet -> IntSetIntersection
intersected (IntSet vec) = IntSetIntersection (BitVec.length vec) (pure vec)


-- * Union composition
-------------------------

data IntSetUnion = IntSetUnion !Int [BitVector]


