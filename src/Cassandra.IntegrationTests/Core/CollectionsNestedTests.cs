﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class CollectionsNestedTests : TestGlobals
    {
        private string _contactPoint = DefaultInitialContactPoint;

        [TestFixtureSetUp]
        public void SetupFixture()
        {
            if (CassandraVersion < Version.Parse("2.1.3"))
            {
                Assert.Ignore("Nested frozen collections are supported in 2.1.3 and above");
                return;
            }

            ITestCluster testCluster = TestClusterManager.GetTestCluster(1);
            _contactPoint = testCluster.InitialContactPoint;
        }

        [Test, TestCassandraVersion(2, 1)]
        public void NestedCollections_Upsert()
        {
            using (var session = Cluster.Builder().AddContactPoint(_contactPoint).Build().Connect())
            {
                string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                string fqTableName = keyspaceName + "." + TestUtils.GetUniqueKeyspaceName().ToLower();
                SetupForFrozenNestedCollectionTest(session, keyspaceName, fqTableName);
                var cqlUpdateStr = String.Format("UPDATE {0} set map1=?, map2=?, list1=? where id=?", fqTableName);
                int id = 1;
                var map1Value = GetMap1Val();
                var map2Value = GetMap2Val();
                var list1Value = GetList1Val();

                // Insert data
                session.Execute(new SimpleStatement(cqlUpdateStr).Bind(map1Value, map2Value, list1Value, id));

                // Validate the end state of data in C*
                string cqlSelectStr = String.Format("SELECT * FROM {0} WHERE id = 1", fqTableName);
                var row = session.Execute(new SimpleStatement(cqlSelectStr)).First();
                ValidateSelectedNestedFrozenRow(row);
            }
        }

        [Test, TestCassandraVersion(2, 1)]
        public void NestedCollections_Update()
        {
            using (var session = Cluster.Builder().AddContactPoint(_contactPoint).Build().Connect())
            {
                string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                string fqTableName = keyspaceName + "." + TestUtils.GetUniqueKeyspaceName().ToLower();
                SetupForFrozenNestedCollectionTest(session, keyspaceName, fqTableName);
                var cqlInsertStr = String.Format("INSERT INTO {0} (id, map1, map2, list1) VALUES (?, ?, ?, ?)", fqTableName);
                var cqlUpdateStr = String.Format("UPDATE {0} set map1=?, map2=?, list1=? where id=?", fqTableName);
                int id = 1;
                Dictionary<string, IEnumerable<string>> map1Value = GetMap1Val();
                Dictionary<string, IEnumerable<string>> map1ValueUpdated = GetMap1Val();
                map1ValueUpdated.Add("somethingdifferent_k", new List<string> { "somethingdifferent_v1", "somethingdifferent_v2" });
                Dictionary<int, IDictionary<string, long>> map2Value = GetMap2Val();
                Dictionary<int, IDictionary<string, long>> map2ValueUpdated = GetMap2Val();
                map2ValueUpdated.Clear();
                map2ValueUpdated.Add(97654, new Dictionary<string, long> { { "somethingdifferent_n1", 1L }, { "somethingdifferent_n2", 2L } });
                List<IDictionary<string, float>> list1Value = GetList1Val();
                List<IDictionary<string, float>> list1ValueUpdated = GetList1Val();
                list1ValueUpdated.Add(new SortedDictionary<string, float> { { "somethingdifferent_m1", 199898.9899898F } });

                // Insert data
                session.Execute(new SimpleStatement(cqlInsertStr).Bind(1, map1Value, map2Value, list1Value));
                // Validate Data Initial state
                string cqlSelectStr = String.Format("SELECT * FROM {0} WHERE id = 1", fqTableName);
                var row = session.Execute(new SimpleStatement(cqlSelectStr)).First();
                ValidateSelectedNestedFrozenRow(row);

                // Upate data
                session.Execute(new SimpleStatement(cqlUpdateStr).Bind(map1ValueUpdated, map2ValueUpdated, list1ValueUpdated, id));
                // Validate the end state of data in C*
                cqlSelectStr = String.Format("SELECT * FROM {0} WHERE id = 1", fqTableName);
                row = session.Execute(new SimpleStatement(cqlSelectStr)).First();
                ValidateSelectedNestedFrozenRow(row, map1ValueUpdated, map2ValueUpdated, list1ValueUpdated);
            }
        }

        [Test, TestCassandraVersion(2, 1)]
        public void NestedCollections_Update_SpecificMapValByKey()
        {
            using (var session = Cluster.Builder().AddContactPoint(_contactPoint).Build().Connect())
            {
                string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                string fqTableName = keyspaceName + "." + TestUtils.GetUniqueKeyspaceName().ToLower();
                SetupForFrozenNestedCollectionTest(session, keyspaceName, fqTableName);
                var cqlInsertStr = String.Format("INSERT INTO {0} (id, map1, map2, list1) VALUES (?, ?, ?, ?)", fqTableName);
                Dictionary<string, IEnumerable<string>> map1Default = GetMap1Val();
                var cqlUpdateSingleMapValueStr = String.Format("UPDATE {0} set map1['{1}'] =? where id=?", fqTableName, map1Default.First().Key);

                Dictionary<string, IEnumerable<string>> map1Value = GetMap1Val();
                List<string> differentMapValue = new List<string> { "somethingdifferent_v1", "somethingdifferent_v2" };
                Dictionary<string, IEnumerable<string>> map1Expected = GetMap1Val();
                map1Expected[map1Expected.First().Key] = differentMapValue;

                Dictionary<int, IDictionary<string, long>> map2Value = GetMap2Val();
                List<IDictionary<string, float>> list1Value = GetList1Val();

                // Insert original data
                session.Execute(new SimpleStatement(cqlInsertStr).Bind(1, map1Value, map2Value, list1Value));
                // Validate Data Initial state
                string cqlSelectStr = String.Format("SELECT * FROM {0} WHERE id = 1", fqTableName);
                var row = session.Execute(new SimpleStatement(cqlSelectStr)).First();
                ValidateSelectedNestedFrozenRow(row);

                // Update data
                session.Execute(session.Prepare(cqlUpdateSingleMapValueStr).Bind(differentMapValue, 1));
                // Validate the end state of data in C*
                cqlSelectStr = String.Format("SELECT * FROM {0} WHERE id = 1", fqTableName);
                row = session.Execute(new SimpleStatement(cqlSelectStr)).First();
                ValidateSelectedNestedFrozenRow(row, map1Expected, GetMap2Val(), GetList1Val());
            }
        }

        [Test, TestCassandraVersion(2, 1)]
        public void NestedCollections_Upsert_IdFoundInSetPart()
        {
            using (var session = Cluster.Builder().AddContactPoint(_contactPoint).Build().Connect())
            {
                string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                string fqTableName = keyspaceName + "." + TestUtils.GetUniqueKeyspaceName().ToLower();
                SetupForFrozenNestedCollectionTest(session, keyspaceName, fqTableName);
                var cqlInsertStr = String.Format("UPDATE {0} set id=?, map1=?, map2=?, list1=? where id=?", fqTableName);
                int id = 1;
                var map1Value = GetMap1Val();
                var map2Value = GetMap2Val();
                var list1Value = GetList1Val();

                // Insert data
                var err = Assert.Throws<InvalidQueryException>(() => session.Execute(new SimpleStatement(cqlInsertStr).Bind(id, map1Value, map2Value, list1Value, id)));
                Assert.AreEqual("PRIMARY KEY part id found in SET part", err.Message);
            }
        }

        [Test, TestCassandraVersion(2, 1)]
        public void NestedCollections_SimpleStatements()
        {
            using (var session = Cluster.Builder().AddContactPoint(_contactPoint).Build().Connect())
            {
                string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                string fqTableName = keyspaceName + "." + TestUtils.GetUniqueKeyspaceName().ToLower();
                SetupForFrozenNestedCollectionTest(session, keyspaceName, fqTableName);
                var cqlInsertStr = String.Format("INSERT INTO {0} (id, map1, map2, list1) VALUES (?, ?, ?, ?)", fqTableName);
                var map1Value = GetMap1Val();
                var map2Value = GetMap2Val();
                var list1Value = GetList1Val();

                // Insert
                session.Execute(new SimpleStatement(cqlInsertStr).Bind(1, map1Value, map2Value, list1Value));

                // Validate the end state of data in C*
                string cqlSelectStr = String.Format("SELECT * FROM {0} WHERE id = 1", fqTableName);
                var row = session.Execute(new SimpleStatement(cqlSelectStr)).First();
                ValidateSelectedNestedFrozenRow(row);
            }
        }

        [Test, TestCassandraVersion(2, 1)]
        public void NestedCollections_PreparedStatements()
        {
            using (var session = Cluster.Builder().AddContactPoint(_contactPoint).Build().Connect())
            {
                string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                string fqTableName = keyspaceName + "." + TestUtils.GetUniqueKeyspaceName().ToLower();
                SetupForFrozenNestedCollectionTest(session, keyspaceName, fqTableName);
                var cqlInsertStr = String.Format("INSERT INTO {0} (id, map1, map2, list1) VALUES (?, ?, ?, ?)", fqTableName);
                PreparedStatement preparedStatement = session.Prepare(cqlInsertStr);
                var map1Value = GetMap1Val();
                var map2Value = GetMap2Val();
                var list1Value = GetList1Val();

                // Insert
                session.Execute(preparedStatement.Bind(1, map1Value, map2Value, list1Value));

                // Validate the end state of data in C*
                string cqlSelectStr = String.Format("SELECT id, map1, map2, list1 FROM {0} WHERE id = 1", fqTableName);
                PreparedStatement preparedSelect = session.Prepare(cqlSelectStr);
                var row = session.Execute(preparedSelect.Bind(new object[] { })).First();

                ValidateSelectedNestedFrozenRow(row);
            }
        }

        [Test, TestCassandraVersion(2, 1)]
        [Ignore("Pending question -- CQL doesn't allow collections with null, but this seems to be failing on a client-parsing level, is this OK?")]
        public void NestedCollections_PreparedStatements_ListWithNullValue()
        {
            using (var session = Cluster.Builder().AddContactPoint(_contactPoint).Build().Connect())
            {
                string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                string fqTableName = keyspaceName + "." + TestUtils.GetUniqueKeyspaceName().ToLower();
                SetupForFrozenNestedCollectionTest(session, keyspaceName, fqTableName);
                var cqlInsertStr = String.Format("INSERT INTO {0} (id, map1, map2, list1) VALUES (?, ?, ?, ?)", fqTableName);
                PreparedStatement preparedStatement = session.Prepare(cqlInsertStr);
                Dictionary<string, IEnumerable<string>> map1Value = GetMap1Val();
                var map2Value = GetMap2Val();
                var list1Value = GetList1Val();
                list1Value.Add(null);

                // Insert
                session.Execute(preparedStatement.Bind(1, map1Value, map2Value, list1Value));

                // Validate the end state of data in C*
                string cqlSelectStr = String.Format("SELECT id, map1, map2, list1 FROM {0} WHERE id = 1", fqTableName);
                PreparedStatement preparedSelect = session.Prepare(cqlSelectStr);
                var row = session.Execute(preparedSelect.Bind(new object[] { })).First();

                ValidateSelectedNestedFrozenRow(row, map1Value, GetMap2Val(), GetList1Val());
            }
        }

        [Test, TestCassandraVersion(2, 1)]
        public void NestedCollections_BatchStatements()
        {
            using (var session = Cluster.Builder().AddContactPoint(_contactPoint).Build().Connect())
            {
                string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                string fqTableName = keyspaceName + "." + TestUtils.GetUniqueKeyspaceName().ToLower();
                SetupForFrozenNestedCollectionTest(session, keyspaceName, fqTableName);
                var cqlInsertStr = String.Format("INSERT INTO {0} (id, map1, map2, list1) VALUES (?, ?, ?, ?)", fqTableName);
                PreparedStatement preparedStatement = session.Prepare(cqlInsertStr);
                BatchStatement batchStatement = new BatchStatement();
                var map1Value = GetMap1Val();
                var map2Value = GetMap2Val();
                var list1Value = GetList1Val();

                // Insert
                batchStatement.Add(preparedStatement.Bind(1, map1Value, map2Value, list1Value));
                session.Execute(batchStatement);

                // Validate the end state of data in C*
                string cqlSelectStr = String.Format("SELECT id, map1, map2, list1 FROM {0} WHERE id = 1", fqTableName);
                PreparedStatement preparedSelect = session.Prepare(cqlSelectStr);
                var row = session.Execute(preparedSelect.Bind(new object[] { })).First();

                ValidateSelectedNestedFrozenRow(row);
            }
        }

        /**************************************
        * Nested Collection Helpers
        **************************************/

        private static void ValidateSelectedNestedFrozenRow(Row row, 
            Dictionary<string, IEnumerable<string>> expectedMap1 = null, 
            Dictionary<int, IDictionary<string, long>> expectedMap2 = null,
            List<IDictionary<string, float>> expectedList1 = null)
        {
            if (expectedMap1 == null)
                expectedMap1 = GetMap1Val();
            if (expectedMap2 == null)
                expectedMap2 = GetMap2Val();
            if (expectedList1 == null)
                expectedList1 = GetList1Val();

            var actualMap1 = row.GetValue<IDictionary<string, IEnumerable<string>>>("map1");
            var actualMap2 = row.GetValue<IDictionary<int, IDictionary<string, long>>>("map2");
            var actualList1 = row.GetValue<IList<IDictionary<string, float>>>("list1");

            Assert.NotNull(actualMap1);
            Assert.NotNull(actualMap2);
            Assert.NotNull(actualList1);
            Assert.AreEqual(expectedMap1.Count, actualMap1.Count);
            Assert.AreEqual(expectedMap2.Count, actualMap2.Count);
            Assert.AreEqual(expectedList1.Count, actualList1.Count);

            CollectionAssert.AreEqual(expectedMap1, actualMap1);
            CollectionAssert.AreEqual(expectedMap2, actualMap2);
            CollectionAssert.AreEqual(expectedList1, actualList1);

        }

        private static Dictionary<string, IEnumerable<string>> GetMap1Val()
        {
            var map = new Dictionary<string, IEnumerable<string>>
                {
                    {"km1_1", new List<string> {"v1", "v2"}},
                    {"km1_2", new List<string> {"a1", "a2"}},
                    {"km1_3", new List<string> {} }, // empty List
                    {"", new List<string> {"", ""}} // strings equal to blank space
                };
            return map;
        }

        private static Dictionary<int, IDictionary<string, long>> GetMap2Val()
        {
            var map = new Dictionary<int, IDictionary<string, long>>
                {
                    {100, new Dictionary<string, long> {{"n1", 1L}, {"n2", 2L}}},
                    {-1, new Dictionary<string, long> {{"n1", -1L}, {"n2", -2L}}}
                };
            return map;
        }

        private static List<IDictionary<string, float>> GetList1Val()
        {
            var list = new List<IDictionary<string, float>>
            {
                new SortedDictionary<string, float> {{"m1", 1.123F}},
                new SortedDictionary<string, float> {} // empty map
            };
            return list;
        }

        private void SetupForFrozenNestedCollectionTest(ISession session, string keyspaceName, string fqTableName)
        {
            session.Execute(String.Format("CREATE KEYSPACE IF NOT EXISTS {0} WITH replication = {1};", keyspaceName, "{'class': 'SimpleStrategy', 'replication_factor' : 3}"));
            session.Execute(String.Format("CREATE TABLE IF NOT EXISTS {0} " +
                                          "(id int primary key, " +
                                          "map1 map<text, frozen<list<text>>>," +
                                          "map2 map<int, frozen<map<text, bigint>>>," +
                                          "list1 list<frozen<map<text, float>>>)", fqTableName));
        }

    }
}
