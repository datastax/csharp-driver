using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.CqlOperatorTests
{
    [Category("short")]
    public class SubstractAssign : TestGlobals
    {
        private ISession _session;
        
        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
        }

        /// <summary>
        /// Use SubtractAssign to remove values from a list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_AllValues()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;

            EntityWithListType singleEntity = expectedEntities.First();
            EntityWithListType expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.Clear();

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.SubstractAssign(singleEntity.ListType) }).Update().Execute();

            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ListType, singleEntity.ListType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Use SubtractAssign to remove all values from a list since they are all the same value
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_Duplicates()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;
            EntityWithListType singleEntity = expectedEntities.First();
            Assert.AreEqual(1, singleEntity.ListType.Count); // make sure there's only one value in the list
            int indexToRemove = 0;
            singleEntity.ListType.AddRange(new[] { singleEntity.ListType[indexToRemove], singleEntity.ListType[indexToRemove], singleEntity.ListType[indexToRemove] });

            // Overwrite one of the rows, validate the data got there
            table.Insert(singleEntity).Execute();
            var entityListPreTest = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(singleEntity.ListType.Count, entityListPreTest[0].ListType.Count);
            singleEntity.AssertEquals(entityListPreTest[0]);

            // Get single value to remove
            List<int> valsToDelete = new List<int>() { singleEntity.ListType[indexToRemove] };
            EntityWithListType expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.Clear();
            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ListType, singleEntity.ListType);
            expectedEntity.AssertEquals(entityList[0]);
        }


        /// <summary>
        /// Use SubtractAssign to remove a single value from the beginning of the list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_OneValueOfMany_IndexZero()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;
            EntityWithListType singleEntity = expectedEntities.First();
            singleEntity.ListType.AddRange(new[] { 999, 9999, 99999, 999999 });

            // Overwrite one of the rows
            table.Insert(singleEntity).Execute();

            // Get value to remove
            int indexToRemove = 0;
            List<int> valsToDelete = new List<int>() { singleEntity.ListType[indexToRemove] };
            EntityWithListType expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.RemoveAt(indexToRemove);
            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ListType, singleEntity.ListType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Use SubtractAssign to remove a single value from the middle of the list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_OneValueOfMany_IndexNonZero()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;
            EntityWithListType singleEntity = expectedEntities.First();
            singleEntity.ListType.AddRange(new[] { 999, 9999, 99999, 999999 });

            // Overwrite one of the rows
            table.Insert(singleEntity).Execute();

            // Get Value to remove
            int indexToRemove = 2;
            List<int> valsToDelete = new List<int>() { singleEntity.ListType[indexToRemove] };
            EntityWithListType expectedEntity = singleEntity.Clone();
            expectedEntity.ListType.RemoveAt(indexToRemove);

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            Assert.AreNotEqual(expectedEntity.ListType, singleEntity.ListType);
            expectedEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that SubractAssign does not change the list when attempting to remove a value that is not contained in the list
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_ValNotInList()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;
            EntityWithListType singleEntity = expectedEntities.First();
            List<int> valsToDelete = new List<int>() { 9999 };

            // make sure this value is not in the list
            Assert.IsFalse(singleEntity.ListType.Contains(valsToDelete.First()));

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            singleEntity.AssertEquals(entityList[0]);
        }

        /// <summary>
        /// Validate that SubractAssign does not change the list when an empty list of vals to delete is passed in
        /// </summary>
        [Test]
        public void SubtractAssign_FromList_EmptyList()
        {
            Tuple<Table<EntityWithListType>, List<EntityWithListType>> tupleListType = EntityWithListType.SetupDefaultTable(_session);
            Table<EntityWithListType> table = tupleListType.Item1;
            List<EntityWithListType> expectedEntities = tupleListType.Item2;
            EntityWithListType singleEntity = expectedEntities.First();
            List<int> valsToDelete = new List<int>() { };

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithListType { ListType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            var entityList = table.Where(m => m.Id == singleEntity.Id).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, entityList.Count);
            singleEntity.AssertEquals(entityList[0]);
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
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleListType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleListType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleListType.Item2;

            EntityWithArrayType singleEntity = expectedEntities.First();
            EntityWithArrayType expectedEntity = singleEntity.Clone();
            expectedEntity.ArrayType = new string[] {};

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.SubstractAssign(singleEntity.ArrayType) }).Update().Execute();

            // Validate final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + expectedEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreNotEqual(expectedEntity.ArrayType, singleEntity.ArrayType);
            object actualArr = rows[0].GetValue<object>("arraytype");
            Assert.AreEqual(null, actualArr);
        }

        /// <summary>
        /// Use SubtractAssign to remove all values from a list since they are all the same value
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_Duplicates()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;
            EntityWithArrayType singleEntity = expectedEntities.First();
            Assert.AreEqual(1, singleEntity.ArrayType.Length); // make sure there's only one value in the list
            int indexToRemove = 0;
            singleEntity.ArrayType.ToList().AddRange(new[] { singleEntity.ArrayType[indexToRemove], singleEntity.ArrayType[indexToRemove], singleEntity.ArrayType[indexToRemove] });

            // Overwrite one of the rows, validate the data got there
            table.Insert(singleEntity).Execute();
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + singleEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            string[] actualArr = rows[0].GetValue<string[]>("arraytype");
            Assert.AreEqual(singleEntity.ArrayType, actualArr);

            // Get single value to remove
            string[] valsToDelete = new string[] { singleEntity.ArrayType[indexToRemove] };
            EntityWithArrayType expectedEntity = singleEntity.Clone();
            expectedEntity.ArrayType = new string[] {};
            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            rows = _session.Execute("SELECT * from " + table.Name + " where id='" + expectedEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            object probablyNullArr = rows[0].GetValue<object>("arraytype");
            Assert.AreEqual(null, probablyNullArr);
        }


        /// <summary>
        /// Use SubtractAssign to remove a single value from the beginning of the list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_OneValueOfMany_IndexZero()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;
            EntityWithArrayType singleEntity = expectedEntities.First();
            List<string> tempList = singleEntity.ArrayType.ToList();
            tempList.AddRange(new[] { "999", "9999", "99999", "999999" });
            singleEntity.ArrayType = tempList.ToArray();

            // Overwrite one of the rows
            table.Insert(singleEntity).Execute();

            // Get value to remove
            int indexToRemove = 0;
            string[] valsToDelete = { singleEntity.ArrayType[indexToRemove] };
            EntityWithArrayType expectedEntity = singleEntity.Clone();
            tempList = expectedEntity.ArrayType.ToList();
            tempList.RemoveAt(indexToRemove);
            expectedEntity.ArrayType = tempList.ToArray();

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + expectedEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreNotEqual(expectedEntity.ArrayType, singleEntity.ArrayType);
            Assert.AreEqual(expectedEntity.ArrayType, rows[0].GetValue<string[]>("arraytype"));
        }

        /// <summary>
        /// Use SubtractAssign to remove a single value from the middle of the list, then validate that the expected data remains in Cassandra
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_OneValueOfMany_IndexNonZero()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;
            EntityWithArrayType singleEntity = expectedEntities.First();
            List<string> tempList = singleEntity.ArrayType.ToList();
            tempList.AddRange(new[] { "999", "9999", "99999", "999999" });
            singleEntity.ArrayType = tempList.ToArray();

            // Overwrite one of the rows
            table.Insert(singleEntity).Execute();

            // Get Value to remove
            int indexToRemove = 2;
            string[] valsToDelete = { singleEntity.ArrayType[indexToRemove] };
            EntityWithArrayType expectedEntity = singleEntity.Clone();
            tempList = expectedEntity.ArrayType.ToList();
            tempList.RemoveAt(indexToRemove);
            expectedEntity.ArrayType = tempList.ToArray();

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + expectedEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreNotEqual(expectedEntity.ArrayType, singleEntity.ArrayType);
            Assert.AreEqual(expectedEntity.ArrayType, rows[0].GetValue<string[]>("arraytype"));
        }

        /// <summary>
        /// Validate that SubractAssign does not change the list when attempting to remove a value that is not contained in the list
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_ValNotInArray()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;
            EntityWithArrayType singleEntity = expectedEntities.First();
            string[] valsToDelete = { "9999" };

            // make sure this value is not in the array 
            Assert.AreEqual(1, singleEntity.ArrayType.Length);
            Assert.AreNotEqual(valsToDelete[0], singleEntity.ArrayType[0]);

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + singleEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            object actualArr = rows[0].GetValue<object>("arraytype");
            Assert.AreEqual(singleEntity.ArrayType, actualArr);
        }

        /// <summary>
        /// Validate that SubractAssign does not change the list when an empty list of vals to delete is passed in
        /// </summary>
        [Test]
        public void SubtractAssign_FromArray_EmptyArray()
        {
            Tuple<Table<EntityWithArrayType>, List<EntityWithArrayType>> tupleArrayType = EntityWithArrayType.SetupDefaultTable(_session);
            Table<EntityWithArrayType> table = tupleArrayType.Item1;
            List<EntityWithArrayType> expectedEntities = tupleArrayType.Item2;
            EntityWithArrayType singleEntity = expectedEntities.First();
            string[] valsToDelete = { };

            // SubstractAssign the values
            table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithArrayType { ArrayType = CqlOperator.SubstractAssign(valsToDelete) }).Update().Execute();

            // Validate final state of the data
            List<Row> rows = _session.Execute("SELECT * from " + table.Name + " where id='" + singleEntity.Id + "'").GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            object actualArr = rows[0].GetValue<object>("arraytype");
            Assert.AreEqual(singleEntity.ArrayType, actualArr);
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
            Tuple<Table<EntityWithDictionaryType>, List<EntityWithDictionaryType>> tupleDictionaryType = EntityWithDictionaryType.SetupDefaultTable(_session);
            Table<EntityWithDictionaryType> table = tupleDictionaryType.Item1;
            List<EntityWithDictionaryType> expectedEntities = tupleDictionaryType.Item2;

            EntityWithDictionaryType singleEntity = expectedEntities.First();
            EntityWithDictionaryType expectedEntity = singleEntity.Clone();
            expectedEntity.DictionaryType.Clear();
            Dictionary<string, string> dictToDelete = new Dictionary<string, string>() { 
                { singleEntity.DictionaryType.First().Key, singleEntity.DictionaryType.First().Value }, 
            };

            // Attempt to remove the data
            var updateStatement = table.Where(t => t.Id == singleEntity.Id).Select(t => new EntityWithDictionaryType { DictionaryType = CqlOperator.SubstractAssign(dictToDelete) }).Update();
            Assert.Throws<InvalidQueryException>(() => updateStatement.Execute());
        }


    }


}
