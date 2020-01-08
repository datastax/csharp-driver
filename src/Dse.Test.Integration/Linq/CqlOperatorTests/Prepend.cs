//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.CqlOperatorTests
{
    public class Prepend : SimulacronTest
    {
        private readonly string _tableName = "EntityWithListType_" + Randomm.RandomAlphaNum(12);

        /// <summary>
        /// Validate that the List can be prepended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Prepend_ToList()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);

            var listToAdd = new List<int> { -1, 0, 5, 6 };
            var listReversed = new List<int>(listToAdd);
            listReversed.Reverse();
            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.InsertRange(0, listReversed);
            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType { ListType = CqlOperator.Prepend(listToAdd) })
                 .Update().Execute();
            
            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ? + ListType WHERE Id = ?",
                1,
                listToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that a List can be pre-pended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Prepend_ToList_StartsOutEmpty()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);

            // overwrite the row we're querying with empty list
            var singleEntity = expectedEntities.First();
            singleEntity.ListType.Clear();
            table.Insert(singleEntity).Execute();

            var listToAdd = new List<int> { -1, 0, 5, 6 };
            var listReversed = new List<int>(listToAdd);
            listReversed.Reverse();
            var expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.InsertRange(0, listReversed);
            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType { ListType = CqlOperator.Prepend(listToAdd) })
                 .Update().Execute();
            
            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ? + ListType WHERE Id = ?",
                1,
                listToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that prepending an empty list to a list type does not cause any unexpected behavior
        /// </summary>
        [Test]
        public void Prepend_ToList_PrependEmptyList()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);

            var listToAdd = new List<int>();
            var singleEntity = expectedEntities.First();
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType { ListType = CqlOperator.Prepend(listToAdd) })
                 .Update().Execute();
            
            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ? + ListType WHERE Id = ?",
                1,
                listToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that the a List can be prepended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Prepend_ToArray()
        {
            var tupleArrayType = EntityWithArrayType.GetDefaultTable(Session, _tableName);
            var table = tupleArrayType.Item1;
            var expectedEntities = tupleArrayType.Item2;

            var arrToAdd = new string[]
            {
                "random_" + Randomm.RandomAlphaNum(10), 
                "random_" + Randomm.RandomAlphaNum(10), 
                "random_" + Randomm.RandomAlphaNum(10),
            };
            var singleEntity = expectedEntities.First();
            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Prepend(arrToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ? + ArrayType WHERE Id = ?",
                1,
                arrToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that the a List can be prepended to, then validate that the expected data exists in Cassandra
        /// This test exists as an extension of Prepend_ToArray so that the Append functionality could be tested
        /// </summary>
        [Test]
        public void Prepend_ToArray_QueryUsingCql()
        {
            var (table, expectedEntities) = EntityWithArrayType.GetDefaultTable(Session, _tableName);

            var arrToAdd = new string[]
            {
                "random_" + Randomm.RandomAlphaNum(10), 
                "random_" + Randomm.RandomAlphaNum(10), 
                "random_" + Randomm.RandomAlphaNum(10),
            };
            var listReversed = arrToAdd.ToList();
            listReversed.Reverse();
            var arrReversed = listReversed.ToArray();

            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            var strValsAsList = new List<string>();
            strValsAsList.AddRange(expectedEntity.ArrayType);
            strValsAsList.InsertRange(0, arrReversed);
            expectedEntity.ArrayType = strValsAsList.ToArray();

            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Prepend(arrToAdd) })
                 .Update().Execute();
            
            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ? + ArrayType WHERE Id = ?",
                1,
                arrToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that when prepending an empty array, the array remains unchanged in C*
        /// </summary>
        [Test]
        public void Prepend_ToArray_AppendEmptyArray_QueryUsingCql()
        {
            var (table, expectedEntities) = EntityWithArrayType.GetDefaultTable(Session, _tableName);

            var arrToAdd = new string[] { };
            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            var strValsAsList = new List<string>();
            strValsAsList.AddRange(expectedEntity.ArrayType);
            strValsAsList.AddRange(arrToAdd);
            expectedEntity.ArrayType = strValsAsList.ToArray();
            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Prepend(arrToAdd) })
                 .Update().Execute();
            
            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ? + ArrayType WHERE Id = ?",
                1,
                arrToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that we cannot prepend to a Dictionary (Map) data type
        /// </summary>
        [Test]
        public void Prepend_Dictionary()
        {
            var (table, expectedEntities) = EntityWithDictionaryType.GetDefaultTable(Session, _tableName);

            var dictToAdd = new Dictionary<string, string> {
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)},
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)},
            };

            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            expectedEntity.DictionaryType.Clear();
            var dictToAddReversed = new Dictionary<string, string>(dictToAdd).Reverse();
            foreach (var keyValPair in dictToAddReversed)
            {
                expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);
            }

            foreach (var keyValPair in singleEntity.DictionaryType)
            {
                expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);
            }

            // Append the values
            var expectedErrMsg = "Invalid operation (dictionarytype = ? - dictionarytype) for non list column dictionarytype";
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"UPDATE {_tableName} SET DictionaryType = ? + DictionaryType WHERE Id = ?",
                          when => when.WithParams(dictToAdd, singleEntity.Id))
                      .ThenServerError(ServerError.Invalid, expectedErrMsg));
            var err = Assert.Throws<InvalidQueryException>(
                () => table.Where(t => t.Id == singleEntity.Id)
                           .Select(t => new EntityWithDictionaryType
                           {
                               DictionaryType = CqlOperator.Prepend(dictToAdd)
                           })
                           .Update().Execute());
            Assert.AreEqual(expectedErrMsg, err.Message);
        }
    }
}