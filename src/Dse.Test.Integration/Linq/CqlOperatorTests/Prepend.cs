//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.CqlOperatorTests
{
    [Category("short")]
    public class Prepend : SharedClusterTest
    {
        private ISession _session;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
        }

        /// <summary>
        /// Validate that the List can be prepended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Prepend_ToList()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;

            List<int> listToAdd = new List<int> { -1, 0, 5, 6 };
            List<int> listReversed = new List<int>(listToAdd);
            listReversed.Reverse();
            EntityWithListType singleEntity = expectedEntities.First();
            EntityWithListType expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.InsertRange(0, listReversed);
            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.Prepend(listToAdd) }).Update().Execute();
            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ListType, singleEntity.ListType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that a List can be pre-pended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Prepend_ToList_StartsOutEmpty()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;

            // overwrite the row we're querying with empty list
            EntityWithListType singleEntity = expectedEntities.First();
            singleEntity.ListType.Clear();
            table.Insert(singleEntity).Execute();

            List<int> listToAdd = new List<int> { -1, 0, 5, 6 };
            List<int> listReversed = new List<int>(listToAdd);
            listReversed.Reverse();
            EntityWithListType expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.InsertRange(0, listReversed);
            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.Prepend(listToAdd) }).Update().Execute();
            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ListType, singleEntity.ListType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that prepending an empty list to a list type does not cause any unexpected behavior
        /// </summary>
        [Test]
        public void Prepend_ToList_PrependEmptyList()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;

            List<int> listToAdd = new List<int> ();
            EntityWithListType singleEntity = expectedEntities.First();
            EntityWithListType expectedEntity = singleEntity.Clone();
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.Prepend(listToAdd) }).Update().Execute();
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that the a List can be prepended to, then validate that the expected data exists in Cassandra
        /// </summary>
        [Test]
        public void Prepend_ToArray()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;

            string[] arrToAdd = new string[] { "random_" + Randomm.RandomAlphaNum(10), "random_" + Randomm.RandomAlphaNum(10), "random_" + Randomm.RandomAlphaNum(10), };
            EntityWithArrayType singleEntity = expectedEntities.First();
            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Prepend(arrToAdd) }).Update().Execute();
            // Validate the final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            CollectionAssert.AreEqual(arrToAdd.Concat(singleEntity.ArrayType).OrderBy(v => v), entityList[0].ArrayType.OrderBy(v => v));
        }

        /// <summary>
        /// Validate that the a List can be prepended to, then validate that the expected data exists in Cassandra
        /// This test exists as an extension of Prepend_ToArray so that the Append functionality could be tested
        /// </summary>
        [Test]
        public void Prepend_ToArray_QueryUsingCql()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;

            string[] arrToAdd = new string[] { "random_" + Randomm.RandomAlphaNum(10), "random_" + Randomm.RandomAlphaNum(10), "random_" + Randomm.RandomAlphaNum(10), };
            List<string> listReversed = arrToAdd.ToList();
            listReversed.Reverse();
            string[] arrReversed = listReversed.ToArray();

            EntityWithArrayType singleEntity = expectedEntities.First();
            EntityWithArrayType expectedEntity = singleEntity.Clone();
            List<string> strValsAsList = new List<string>();
            strValsAsList.AddRange(expectedEntity.ArrayType);
            strValsAsList.InsertRange(0, arrReversed);
            expectedEntity.ArrayType = strValsAsList.ToArray();

            // Append the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Prepend(arrToAdd) }).Update().Execute();

            // Validate the final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + expectedEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreNotEqual(expectedEntity.ArrayType, singleEntity.ArrayType);
            string[] actualArr = rows[0].GetValue<string[]>("arraytype");
            CollectionAssert.AreEquivalent(expectedEntity.ArrayType, actualArr);
        }

        /// <summary>
        /// Validate that when prepending an empty array, the array remains unchanged in C*
        /// </summary>
        [Test]
        public void Prepend_ToArray_AppendEmptyArray_QueryUsingCql()
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
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.Prepend(arrToAdd) }).Update().Execute();
            // Validate the final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + expectedEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            string[] actualArr = rows[0].GetValue<string[]>("arraytype");
            Assert.AreEqual(expectedEntity.ArrayType, actualArr);
        }

        /// <summary>
        /// Validate that we cannot prepend to a Dictionary (Map) data type
        /// </summary>
        [Test]
        public void Prepend_Dictionary()
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
            expectedEntity.DictionaryType.Clear();
            var dictToAddReversed = new Dictionary<string, string>(dictToAdd).Reverse();
            foreach (var keyValPair in dictToAddReversed)
                expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);
            foreach (var keyValPair in singleEntity.DictionaryType)
                expectedEntity.DictionaryType.Add(keyValPair.Key, keyValPair.Value);

            // Append the values
            string expectedErrMsg = "Invalid operation (dictionarytype = ? - dictionarytype) for non list column dictionarytype";
            var err = Assert.Throws<InvalidQueryException>(() => table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.Prepend(dictToAdd) }).Update().Execute());
            Assert.AreEqual(expectedErrMsg, err.Message);
        }

    }


}
