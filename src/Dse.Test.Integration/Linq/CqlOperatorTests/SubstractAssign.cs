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
    public class SubstractAssign : SimulacronTest
    {
        private readonly string _tableName = "EntityWithListType_" + Randomm.RandomAlphaNum(12);

        /// <summary>
        /// Use SubtractAssign to remove values from a list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_AllValues()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);

            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.Clear();

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType
                 {
                     ListType = CqlOperator.SubstractAssign(singleEntity.ListType)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType - ? WHERE Id = ?",
                1,
                singleEntity.ListType, singleEntity.Id);
        }

        /// <summary>
        /// Use SubtractAssign to remove all values from a list since they are all the same value
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_Duplicates()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);
            var singleEntity = expectedEntities.First();
            Assert.AreEqual(1, singleEntity.ListType.Count); // make sure there's only one value in the list
            var indexToRemove = 0;
            singleEntity.ListType.AddRange(new[] { singleEntity.ListType[indexToRemove], singleEntity.ListType[indexToRemove], singleEntity.ListType[indexToRemove] });

            // Get single value to remove
            var valsToDelete = new List<int>() { singleEntity.ListType[indexToRemove] };

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType
                 {
                     ListType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        /// <summary>
        /// Use SubtractAssign to remove a single value from the beginning of the list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_OneValueOfMany_IndexZero()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);
            var singleEntity = expectedEntities.First();
            singleEntity.ListType.AddRange(new[] { 999, 9999, 99999, 999999 });

            // Get value to remove
            var indexToRemove = 0;
            var valsToDelete = new List<int>() { singleEntity.ListType[indexToRemove] };
            var expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.RemoveAt(indexToRemove);
            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType
                 {
                     ListType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        /// <summary>
        /// Use SubtractAssign to remove a single value from the middle of the list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_OneValueOfMany_IndexNonZero()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);
            var singleEntity = expectedEntities.First();
            singleEntity.ListType.AddRange(new[] { 999, 9999, 99999, 999999 });

            // Get Value to remove
            var indexToRemove = 2;
            var valsToDelete = new List<int>() { singleEntity.ListType[indexToRemove] };
            var expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.RemoveAt(indexToRemove);

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType
                 {
                     ListType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        /// <summary>
        /// Validate that SubractAssign does not change the list when attempting to remove a value that is not contained in the list
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_ValNotInList()
        {
            var tupleListType = EntityWithListType.GetDefaultTable(Session, _tableName);
            var table = tupleListType.Item1;
            var expectedEntities = tupleListType.Item2;
            var singleEntity = expectedEntities.First();
            var valsToDelete = new List<int>() { 9999 };

            // make sure this value is not in the list
            Assert.IsFalse(singleEntity.ListType.Contains(valsToDelete.First()));

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType
                 {
                     ListType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        /// <summary>
        /// Validate that SubractAssign does not change the list when an empty list of vals to delete is passed in
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_EmptyList()
        {
            var (table, expectedEntities) = EntityWithListType.GetDefaultTable(Session, _tableName);
            var singleEntity = expectedEntities.First();
            var valsToDelete = new List<int>();

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithListType
                 {
                     ListType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ListType = ListType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        ////////////////////////////////////////////////////////////////////////
        /// Begin Array Cases
        ////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Use SubtractAssign to remove values from an array, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_AllValues_QueryUsingCql()
        {
            var (table, expectedEntities) = EntityWithArrayType.GetDefaultTable(Session, _tableName);

            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            expectedEntity.ArrayType = new string[] { };

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType
                 {
                     ArrayType = CqlOperator.SubstractAssign(singleEntity.ArrayType)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType - ? WHERE Id = ?",
                1,
                singleEntity.ArrayType, singleEntity.Id);
        }

        /// <summary>
        /// Use SubtractAssign to remove all values from a list since they are all the same value
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_Duplicates()
        {
            var tupleArrayType = EntityWithArrayType.GetDefaultTable(Session, _tableName);
            var table = tupleArrayType.Item1;
            var expectedEntities = tupleArrayType.Item2;
            var singleEntity = expectedEntities.First();
            Assert.AreEqual(1, singleEntity.ArrayType.Length); // make sure there's only one value in the list
            var indexToRemove = 0;
            singleEntity.ArrayType.ToList().AddRange(new[] { singleEntity.ArrayType[indexToRemove], singleEntity.ArrayType[indexToRemove], singleEntity.ArrayType[indexToRemove] });

            // Get single value to remove
            var valsToDelete = new[] { singleEntity.ArrayType[indexToRemove] };
            var expectedEntity = singleEntity.Clone();
            expectedEntity.ArrayType = new string[] { };

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType
                 {
                     ArrayType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        /// <summary>
        /// Use SubtractAssign to remove a single value from the beginning of the list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_OneValueOfMany_IndexZero()
        {
            var tupleArrayType = EntityWithArrayType.GetDefaultTable(Session, _tableName);
            var table = tupleArrayType.Item1;
            var expectedEntities = tupleArrayType.Item2;
            var singleEntity = expectedEntities.First();
            var tempList = singleEntity.ArrayType.ToList();
            tempList.AddRange(new[] { "999", "9999", "99999", "999999" });
            singleEntity.ArrayType = tempList.ToArray();

            // Get value to remove
            var indexToRemove = 0;
            string[] valsToDelete = { singleEntity.ArrayType[indexToRemove] };
            var expectedEntity = singleEntity.Clone();
            tempList = expectedEntity.ArrayType.ToList();
            tempList.RemoveAt(indexToRemove);
            expectedEntity.ArrayType = tempList.ToArray();

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType
                 {
                     ArrayType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        /// <summary>
        /// Use SubtractAssign to remove a single value from the middle of the list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_OneValueOfMany_IndexNonZero()
        {
            var tupleArrayType = EntityWithArrayType.GetDefaultTable(Session, _tableName);
            var table = tupleArrayType.Item1;
            var expectedEntities = tupleArrayType.Item2;
            var singleEntity = expectedEntities.First();
            var tempList = singleEntity.ArrayType.ToList();
            tempList.AddRange(new[] { "999", "9999", "99999", "999999" });
            singleEntity.ArrayType = tempList.ToArray();

            // Get Value to remove
            var indexToRemove = 2;
            string[] valsToDelete = { singleEntity.ArrayType[indexToRemove] };
            var expectedEntity = singleEntity.Clone();
            tempList = expectedEntity.ArrayType.ToList();
            tempList.RemoveAt(indexToRemove);
            expectedEntity.ArrayType = tempList.ToArray();

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType
                 {
                     ArrayType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        /// <summary>
        /// Validate that SubractAssign does not change the list when attempting to remove a value that is not contained in the list
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_ValNotInArray()
        {
            var tupleArrayType = EntityWithArrayType.GetDefaultTable(Session, _tableName);
            var table = tupleArrayType.Item1;
            var expectedEntities = tupleArrayType.Item2;
            var singleEntity = expectedEntities.First();
            string[] valsToDelete = { "9999" };

            // make sure this value is not in the array
            Assert.AreEqual(1, singleEntity.ArrayType.Length);
            Assert.AreNotEqual(valsToDelete[0], singleEntity.ArrayType[0]);

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType
                 {
                     ArrayType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        /// <summary>
        /// Validate that SubractAssign does not change the list when an empty list of vals to delete is passed in
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_EmptyArray()
        {
            var tupleArrayType = EntityWithArrayType.GetDefaultTable(Session, _tableName);
            var table = tupleArrayType.Item1;
            var expectedEntities = tupleArrayType.Item2;
            var singleEntity = expectedEntities.First();
            string[] valsToDelete = { };

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id)
                 .Select(t => new EntityWithArrayType
                 {
                     ArrayType = CqlOperator.SubstractAssign(valsToDelete)
                 }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET ArrayType = ArrayType - ? WHERE Id = ?",
                1,
                valsToDelete, singleEntity.Id);
        }

        ////////////////////////////////////////////////////////////////////////
        /// Begin Dictionary / Map Cases
        ////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Attempt to use SubtractAssign to remove a single value from a dictionary that contains a single value
        /// Validate Error response
        /// </summary>
        [Test]
        public void SubtractAssign_FromDictionary_NotAllowed()
        {
            var tupleDictionaryType = EntityWithDictionaryType.GetDefaultTable(Session, _tableName);
            var table = tupleDictionaryType.Item1;
            var expectedEntities = tupleDictionaryType.Item2;

            var singleEntity = expectedEntities.First();
            var expectedEntity = singleEntity.Clone();
            expectedEntity.DictionaryType.Clear();
            var dictToDelete = new Dictionary<string, string>() {
                { singleEntity.DictionaryType.First().Key, singleEntity.DictionaryType.First().Value },
            };

            // Attempt to remove the data
            var updateStatement =
                table.Where(t => t.Id == singleEntity.Id)
                     .Select(t => new EntityWithDictionaryType
                     {
                         // Use incorrect substract assign overload
                         DictionaryType = CqlOperator.SubstractAssign(dictToDelete)
                     }).Update();

            Assert.Throws<InvalidOperationException>(() => updateStatement.Execute(),
                "Use dedicated method to substract assign keys only for maps");
        }

        /// <summary>
        /// Remove keys from map.
        ///
        /// This test creates a map with 3 keys (firstey, secondkey, lastkey) whitin an entity, and removes keys from this map,
        /// and check the remaining keys result
        /// </summary>
        [TestCase(2, "firstKey")]
        [TestCase(1, "firstKey", "secondKey")]
        [TestCase(0, "firstKey", "secondKey", "lastkey")]
        [TestCase(3, "unexistentKey", "unexistentKey2", "unexistentKey3")]
        [TestCase(3, new string[0])]
        [TestCassandraVersion(2, 1)]
        public void SubtractAssign_FromDictionary(int remainingKeysCount, params string[] keysToRemove)
        {
            var tupleDictionaryType = EntityWithDictionaryType.GetDefaultTable(Session, _tableName);
            var table = tupleDictionaryType.Item1;

            var newdict = new Dictionary<string, string>
            {
                { "firstKey", "firstvalue" },
                { "secondKey", "secondvalue" },
                { "lastkey", "lastvalue" },
            };
            var id = Guid.NewGuid().ToString();
            var newEntity = new EntityWithDictionaryType
            {
                Id = id,
                DictionaryType = newdict
            };

            table.Where(t => t.Id == newEntity.Id).Select(x => new EntityWithDictionaryType
            {
                DictionaryType = x.DictionaryType.SubstractAssign(keysToRemove)
            }).Update().Execute();

            VerifyBoundStatement(
                $"UPDATE {_tableName} SET DictionaryType = DictionaryType - ? WHERE Id = ?",
                1,
                keysToRemove, newEntity.Id);
        }
    }
}