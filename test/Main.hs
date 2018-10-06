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
    listSetGen = listOf (choose (0, 9999)) <&> HashSet.fromList <&> HashSet.toList
    listSetProperty name property = testProperty name $ forAll listSetGen $ property
    in [
      listSetProperty "List roundtrip" $ \ list ->
      sort list === sort (toList (fromList @DenseIntSet.DenseIntSet list))
      ,
      listSetProperty "Inversion" $ \ list -> let
        invertedList = enumFromTo 0 (foldl' max 0 list) \\ list
        in sort invertedList === sort (toList (DenseIntSet.invert (fromList list)))
    ]
