module Main where

import Prelude hiding (toList, choose)
import GHC.Exts
import Test.QuickCheck.Instances
import Test.Tasty
import Test.Tasty.Runners
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck
import qualified Test.QuickCheck as QuickCheck
import qualified Test.QuickCheck.Property as QuickCheck
import qualified DenseIntSet
import qualified Data.HashSet as HashSet


main =
  defaultMain $
  testGroup "All" $ let
    setGen = listOf (choose (0, 99)) <&> HashSet.fromList
    listSetGen = setGen <&> HashSet.toList
    nonEmptyListOf gen = do
      length <- choose (1, 99)
      replicateM length gen
    in [
      testProperty "List roundtrip" $ forAll listSetGen $ \ list ->
      sort list === sort (toList (fromList @DenseIntSet.DenseIntSet list))
      ,
      testProperty "Inversion" $ forAll listSetGen $ \ list -> let
        invertedList = enumFromTo 0 (foldl' max 0 list) \\ list
        in sort invertedList === sort (toList (DenseIntSet.invert (fromList list)))
      ,
      testProperty "Union" $ forAll (listOf listSetGen) $ \ lists ->
      sort (foldr union [] lists) === sort (toList (DenseIntSet.unions (fmap fromList lists)))
      ,
      testProperty "Intersection" $ forAll (nonEmptyListOf listSetGen) $ \ lists ->
      sort (foldl1' intersect lists) === sort (toList (DenseIntSet.intersections (fmap fromList lists)))
    ]
