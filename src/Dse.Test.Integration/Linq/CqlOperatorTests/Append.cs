//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.CqlOperatorTests
{
    public class Append : SimulacronTest
    {
        private readonly string _tableName = "EntityWithListType_" + Randomm.RandomAlphaNum(12);

        /// <summary>
        /// Validate that the a List can be appended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Append_ToList()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);

            var listToAdd = new List<int> { -1, 0, 5, 6 };
            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.AddRange(listToAdd);
            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType { ListType = CqlOperator.Append(listToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType + ? WHERE Id = ?",
                1,
                listToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that the a List can be appended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Append_ToList_StartsOutEmpty()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);

            // overwrite the row we're querying with empty list
            var singleEntity = expectedEntities.First();
            singleEntity.ListType.Clear();
            var expectedEntity = singleEntity.Clone();

            var listToAdd = new List<int> { -1, 0, 5, 6 };
            expectedEntity.ListType.AddRange(listToAdd);

            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType { ListType = CqlOperator.Append(listToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType + ? WHERE Id = ?",
                1,
                listToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that appending an empty list to a list type does not cause any unexpected behavior
        /// </summary>
        [Test]
        public void Append_ToList_AppendEmptyList()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);

            var listToAdd = new List<int>();
            var singleEntity = expectedEntities.First();
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType { ListType = CqlOperator.Append(listToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType + ? WHERE Id = ?",
                1,
                listToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that the a List can be appended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Append_ToArray()
        {
            var (table, expectedEntities) = EntityWithArrayType.GetDefaultTable(Session, _tableName);

            var arrToAdd = new string[]
            {
                "random_" + Randomm.RandomAlphaNum(10),
                "random_" + Randomm.RandomAlphaNum(10),
                "random_" + Randomm.RandomAlphaNum(10),
            };
            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            var strValsAsList = new List<string>();
            strValsAsList.AddRange(expectedEntity.ArrayType);
            strValsAsList.AddRange(arrToAdd);
            expectedEntity.ArrayType = strValsAsList.ToArray();
            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Append(arrToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType + ? WHERE Id = ?",
                1,
                arrToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that when appending an empty array, the array remains unchanged in C*
        /// </summary>
        [Test]
        public void Append_ToArray_AppendEmptyArray_QueryUsingCql()
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
                 .Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Append(arrToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType + ? WHERE Id = ?",
                1,
                arrToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that the a Dictionary (or Map in C*) can be appended to, then validate that the expected data exists after the Update
        /// </summary>
        [Test]
        public void Append_ToDictionary()
        {
            var (table, expectedEntities) = EntityWithDictionaryType.GetDefaultTable(Session, _tableName);

            var dictToAdd = new Dictionary<string, string>() {
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)},
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)},
            };
            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            foreach (var keyValPair in dictToAdd)
            {
                expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);
            }

            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.Append(dictToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET DictionaryType = DictionaryType + ? WHERE Id = ?",
                1,
                dictToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that Map data does not change after appending an empty dictionary to that C* value
        /// </summary>
        [Test]
        public void Append_ToDictionary_EmptyDictionary()
        {
            var (table, expectedEntities) = EntityWithDictionaryType.GetDefaultTable(Session, _tableName);

            var dictToAdd = new Dictionary<string, string>() { };
            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            foreach (var keyValPair in dictToAdd)
                expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);

            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.Append(dictToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET DictionaryType = DictionaryType + ? WHERE Id = ?",
                1,
                dictToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that, in a mix of key-value pairs to insert, only non-duplicate keys are inserted.
        /// </summary>
        [Test]
        public void Append_ToDictionary_DuplicateAndNonDuplicateKey()
        {
            var (table, expectedEntities) = EntityWithDictionaryType.GetDefaultTable(Session, _tableName);

            var singleEntity = expectedEntities.First();
            var dictToAdd = new Dictionary<string, string>() {
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)},
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)},
                { singleEntity.DictionaryType.First().Key, singleEntity.DictionaryType.First().Value }
            };
            var expectedEntity = singleEntity.Clone();
            foreach (var keyValPair in dictToAdd)
            {
                if (!expectedEntity.DictionaryType.ContainsKey(keyValPair.Key))
                {
                    expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);
                }
            }

            // Append the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.Append(dictToAdd) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET DictionaryType = DictionaryType + ? WHERE Id = ?",
                1,
                dictToAdd, singleEntity.Id);
        }

        /// <summary>
        /// Validate that the a List can be appended to and then queried, using a table that contains all collection types
        /// </summary>
        [Test]
        public void Append_ToList_TableWithAllCollectionTypes()
        {
            var (table, expectedEntities) = EntityWithAllCollectionTypes.GetDefaultTable(Session, _tableName);

            var singleEntity = expectedEntities.First();
            var toAppend = new List<int> { 5, 6 };
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithAllCollectionTypes { ListType = CqlOperator.Append(toAppend) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType + ? WHERE Id = ?",
                1,
                toAppend, singleEntity.Id);

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT ArrayType, DictionaryType, Id, ListType FROM {_tableName} WHERE Id = ?",
                          when => when.WithParam(singleEntity.Id))
                      .ThenRowsSuccess(
                          new[] { "ArrayType", "DictionaryType", "Id", "ListType" },
                          r => r.WithRow(
                              singleEntity.ArrayType,
                              singleEntity.DictionaryType,
                              singleEntity.Id,
                              singleEntity.ListType.Concat(toAppend))));

            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            CollectionAssert.AreEqual(singleEntity.ListType.Concat(toAppend), entityList.First().ListType);
        }

        /// <summary>
        /// Validate that the an Array can be appended to and then queried
        /// </summary>
        [Test]
        public void Append_ToArray_TableWithAllCollectionTypes()
        {
            var (table, expectedEntities) = EntityWithAllCollectionTypes.GetDefaultTable(Session, _tableName);

            var singleEntity = expectedEntities.First();
            var toAppend = new string[] { "tag1", "tag2", "tag3" };
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithAllCollectionTypes { ArrayType = CqlOperator.Append(toAppend) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType + ? WHERE Id = ?",
                1,
                toAppend, singleEntity.Id);

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT ArrayType, DictionaryType, Id, ListType FROM {_tableName} WHERE Id = ?",
                          when => when.WithParam(singleEntity.Id))
                      .ThenRowsSuccess(
                          new[] { "ArrayType", "DictionaryType", "Id", "ListType" },
                          r => r.WithRow(
                              singleEntity.ArrayType.Concat(toAppend),
                              singleEntity.DictionaryType,
                              singleEntity.Id,
                              singleEntity.ListType)));

            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            CollectionAssert.AreEqual(singleEntity.ArrayType.Concat(toAppend), entityList.First().ArrayType);
        }

        /// <summary>
        /// Validate that the a Dictionary can be appended to and then queried
        /// </summary>
        [Test]
        public void Append_ToDictionary_TableWithAllCollectionTypes()
        {
            var (table, expectedEntities) = EntityWithAllCollectionTypes.GetDefaultTable(Session, _tableName);

            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            expectedEntity.DictionaryType.Add("key1", "val1");
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithAllCollectionTypes { DictionaryType = CqlOperator.Append(expectedEntity.DictionaryType) })
                 .Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET DictionaryType = DictionaryType + ? WHERE Id = ?",
                1,
                expectedEntity.DictionaryType, singleEntity.Id);

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT ArrayType, DictionaryType, Id, ListType FROM {_tableName} WHERE Id = ?",
                          when => when.WithParam(singleEntity.Id))
                      .ThenRowsSuccess(
                          new[] { "ArrayType", "DictionaryType", "Id", "ListType" },
                          r => r.WithRow(
                              expectedEntity.ArrayType,
                              expectedEntity.DictionaryType,
                              expectedEntity.Id,
                              expectedEntity.ListType)));

            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            CollectionAssert.AreEqual(expectedEntity.ArrayType, singleEntity.ArrayType);
            entityList.First().AssertEquals(expectedEntity);
        }
    }
}