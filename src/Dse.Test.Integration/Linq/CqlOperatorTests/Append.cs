//
//  Copyright (C) 2017 DataStax, Inc.
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
using Dse.Mapping;
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.CqlOperatorTests
{
    [Category("short")]
    public class Append : SharedClusterTest
    {
        private ISession _session;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
        }

        /// <summary>
        /// Validate that the a List can be appended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Append_ToList()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(Session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;

            List<int> listToAdd = new List<int> { -1, 0, 5, 6 };
            EntityWithListType singleEntity = expectedEntities.First();
            EntityWithListType expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.AddRange(listToAdd);
            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.Append(listToAdd) }).Update().Execute();
            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ListType, singleEntity.ListType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that the a List can be appended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Append_ToList_StartsOutEmpty()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;

            // overwrite the row we're querying with empty list
            EntityWithListType singleEntity = expectedEntities.First();
            singleEntity.ListType.Clear();
            table.Insert(singleEntity).Execute();
            EntityWithListType expectedEntity = singleEntity.Clone();

            List<int> listToAdd = new List<int> { -1, 0, 5, 6 };
            expectedEntity.ListType.AddRange(listToAdd);
            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.Append(listToAdd) }).Update().Execute();
            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ListType, singleEntity.ListType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that appending an empty list to a list type does not cause any unexpected behavior
        /// </summary>
        [Test]
        public void Append_ToList_AppendEmptyList()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;

            List<int> listToAdd = new List<int> ();
            EntityWithListType singleEntity = expectedEntities.First();
            EntityWithListType expectedEntity = singleEntity.Clone();
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.Append(listToAdd) }).Update().Execute();
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that the a List can be appended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Append_ToArray()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;

            string[] arrToAdd = new string[] { "random_" + Randomm.RandomAlphaNum(10), "random_" + Randomm.RandomAlphaNum(10), "random_" + Randomm.RandomAlphaNum(10), };
            EntityWithArrayType singleEntity = expectedEntities.First();
            EntityWithArrayType expectedEntity = singleEntity.Clone();
            List<string> strValsAsList = new List<string>();
            strValsAsList.AddRange(expectedEntity.ArrayType);
            strValsAsList.AddRange(arrToAdd);
            expectedEntity.ArrayType = strValsAsList.ToArray();
            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Append(arrToAdd) }).Update().Execute();
            // Validate the final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ArrayType, singleEntity.ArrayType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that the a List can be appended to, then validate that the expected data exists in Cassandra
        /// This test exists as an extension of Append_ToArray so that the Append functionality could be tested
        /// </summary>
        [Test]
        public void Append_ToArray_QueryUsingCql()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;

            string[] arrToAdd = new string[] { "random_" + Randomm.RandomAlphaNum(10), "random_" + Randomm.RandomAlphaNum(10), "random_" + Randomm.RandomAlphaNum(10), };
            EntityWithArrayType singleEntity = expectedEntities.First();
            EntityWithArrayType expectedEntity = singleEntity.Clone();
            List<string> strValsAsList = new List<string>();
            strValsAsList.AddRange(expectedEntity.ArrayType);
            strValsAsList.AddRange(arrToAdd);
            expectedEntity.ArrayType = strValsAsList.ToArray();
            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Append(arrToAdd) }).Update().Execute();
            // Validate the final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + expectedEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreNotEqual(expectedEntity.ArrayType, singleEntity.ArrayType);
            string[] actualArr = rows[0].GetValue<string[]>("arraytype");
            Assert.AreEqual(expectedEntity.ArrayType, actualArr);
        }

        /// <summary>
        /// Validate that when appending an empty array, the array remains unchanged in C*
        /// </summary>
        [Test]
        public void Append_ToArray_AppendEmptyArray_QueryUsingCql()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;

            string[] arrToAdd = new string[] { };
            EntityWithArrayType singleEntity = expectedEntities.First();
            EntityWithArrayType expectedEntity = singleEntity.Clone();
            List<string> strValsAsList = new List<string>();
            strValsAsList.AddRange(expectedEntity.ArrayType);
            strValsAsList.AddRange(arrToAdd);
            expectedEntity.ArrayType = strValsAsList.ToArray();
            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Append(arrToAdd) }).Update().Execute();
            // Validate the final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + expectedEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            string[] actualArr = rows[0].GetValue<string[]>("arraytype");
            Assert.AreEqual(expectedEntity.ArrayType, actualArr);
        }

        /// <summary>
        /// Validate that the a Dictionary (or Map in C*) can be appended to, then validate that the expected data exists after the Update
        /// </summary>
        [Test]
        public void Append_ToDictionary()
        {
            Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>> tupleDictionaryType = EntityWithDictionaryType.SetupDefaultTable(_session);
            Table<EntityWithDictionaryType> table = tupleDictionaryType.Item1;
            List<EntityWithDictionaryType> expectedEntities = tupleDictionaryType.Item2;

            Dictionary<string, string> dictToAdd = new Dictionary<string, string>() { 
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)}, 
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)}, 
            };
            EntityWithDictionaryType singleEntity = expectedEntities.First();
            EntityWithDictionaryType expectedEntity = singleEntity.Clone();
            foreach (var keyValPair in dictToAdd)
                expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);

            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.Append(dictToAdd) }).Update().Execute();
            // Validate the final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.DictionaryType, singleEntity.DictionaryType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that Map data does not change after appending an empty dictionary to that C* value
        /// </summary>
        [Test]
        public void Append_ToDictionary_EmptyDictionary()
        {
            Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>> tupleDictionaryType = EntityWithDictionaryType.SetupDefaultTable(_session);
            Table<EntityWithDictionaryType> table = tupleDictionaryType.Item1;
            List<EntityWithDictionaryType> expectedEntities = tupleDictionaryType.Item2;

            Dictionary<string, string> dictToAdd = new Dictionary<string, string>() {};
            EntityWithDictionaryType singleEntity = expectedEntities.First();
            EntityWithDictionaryType expectedEntity = singleEntity.Clone();
            foreach (var keyValPair in dictToAdd)
                expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);

            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.Append(dictToAdd) }).Update().Execute();
            // Validate the final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that Map data does not change after attempting to append a duplicate dictionary key
        /// </summary>
        [Test]
        public void Append_ToDictionary_DuplicateKey()
        {
            Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>> tupleDictionaryType = EntityWithDictionaryType.SetupDefaultTable(_session);
            Table<EntityWithDictionaryType> table = tupleDictionaryType.Item1;
            List<EntityWithDictionaryType> expectedEntities = tupleDictionaryType.Item2;

            EntityWithDictionaryType singleEntity = expectedEntities.First();
            EntityWithDictionaryType expectedEntity = singleEntity.Clone();
            Assert.AreEqual(expectedEntity.DictionaryType, singleEntity.DictionaryType);

            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.Append(singleEntity.DictionaryType) }).Update().Execute();

            // Validate the final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that, in a mix of key-value pairs to insert, only non-duplicate keys are inserted.
        /// </summary>
        [Test]
        public void Append_ToDictionary_DuplicateAndNonDuplicateKey()
        {
            Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>> tupleDictionaryType = EntityWithDictionaryType.SetupDefaultTable(_session);
            Table<EntityWithDictionaryType> table = tupleDictionaryType.Item1;
            List<EntityWithDictionaryType> expectedEntities = tupleDictionaryType.Item2;

            EntityWithDictionaryType singleEntity = expectedEntities.First();
            Dictionary<string, string> dictToAdd = new Dictionary<string, string>() { 
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)}, 
                {"randomKey_" + Randomm.RandomAlphaNum(10), "randomVal_" + Randomm.RandomAlphaNum(10)}, 
                { singleEntity.DictionaryType.First().Key, singleEntity.DictionaryType.First().Value }
            };
            EntityWithDictionaryType expectedEntity = singleEntity.Clone();
            foreach (var keyValPair in dictToAdd)
            {
                if (!expectedEntity.DictionaryType.ContainsKey(keyValPair.Key))
                    expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);
            }

            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.Append(dictToAdd) }).Update().Execute();
            // Validate the final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.DictionaryType, singleEntity.DictionaryType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that the a List can be appended to and then queried, using a table that contains all collection types
        /// </summary>
        [Test]
        public void Append_ToList_TableWithAllCollectionTypes()
        {
            Tuple<Table<EntityWithAllCollectionTypes>, List<EntityWithAllCollectionTypes>> tupleAllCollectionTypes = EntityWithAllCollectionTypes.SetupDefaultTable(_session);
            Table<EntityWithAllCollectionTypes> table = tupleAllCollectionTypes.Item1;
            List<EntityWithAllCollectionTypes> expectedEntities = tupleAllCollectionTypes.Item2;

            EntityWithAllCollectionTypes singleEntity = expectedEntities.First();
            var toAppend = new List<int> { 5, 6 };
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithAllCollectionTypes { ListType = CqlOperator.Append(toAppend) }).Update().Execute();
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
            Tuple<Table<EntityWithAllCollectionTypes>, List<EntityWithAllCollectionTypes>> tupleAllCollectionTypes = EntityWithAllCollectionTypes.SetupDefaultTable(_session);
            Table<EntityWithAllCollectionTypes> table = tupleAllCollectionTypes.Item1;
            List<EntityWithAllCollectionTypes> expectedEntities = tupleAllCollectionTypes.Item2;

            EntityWithAllCollectionTypes singleEntity = expectedEntities.First();
            var toAppend = new string[] { "tag1", "tag2", "tag3" };
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithAllCollectionTypes { ArrayType = CqlOperator.Append(toAppend) }).Update().Execute();
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
            Tuple<Table<EntityWithAllCollectionTypes>, List<EntityWithAllCollectionTypes>> tupleAllCollectionTypes = EntityWithAllCollectionTypes.SetupDefaultTable(_session);
            Table<EntityWithAllCollectionTypes> table = tupleAllCollectionTypes.Item1;
            List<EntityWithAllCollectionTypes> expectedEntities = tupleAllCollectionTypes.Item2;

            EntityWithAllCollectionTypes singleEntity = expectedEntities.First();
            EntityWithAllCollectionTypes expectedEntity = singleEntity.Clone();
            expectedEntity.DictionaryType.Add("key1","val1");
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithAllCollectionTypes { DictionaryType = CqlOperator.Append(expectedEntity.DictionaryType) }).Update().Execute();
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            CollectionAssert.AreEqual(expectedEntity.ArrayType, singleEntity.ArrayType);
            entityList.First().AssertEquals(expectedEntity);
        }
    }


}
