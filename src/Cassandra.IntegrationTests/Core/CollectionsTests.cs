//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class CollectionsTests : SharedClusterTest
    {
        private const string AllTypesTableName = "all_types_table_collections";

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            Session.Execute(String.Format(TestUtils.CreateTableAllTypes, AllTypesTableName));
        }

        [Test]
        public void DecodeCollectionTest()
        {
            var id = "c9850ed4-c139-4b75-affe-098649f9de93";
            var insertQuery = String.Format("INSERT INTO {0} (id, map_sample, list_sample, set_sample) VALUES ({1}, {2}, {3}, {4})", 
                AllTypesTableName, 
                id, 
                "{'fruit': 'apple', 'band': 'Beatles'}", 
                "['one', 'two']", 
                "{'set_1one', 'set_2two'}");

            Session.Execute(insertQuery);
            var row = Session.Execute(String.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, id)).First();
            var expectedMap = new SortedDictionary<string, string> { { "fruit", "apple" }, { "band", "Beatles" } };
            var expectedList = new List<string> { "one", "two" };
            var expectedSet = new List<string> { "set_1one", "set_2two" };
            Assert.AreEqual(expectedMap, row.GetValue<IDictionary<string, string>>("map_sample"));
            Assert.AreEqual(expectedList, row.GetValue<List<string>>("list_sample"));
            Assert.AreEqual(expectedSet, row.GetValue<List<string>>("set_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void TimeUuid_Collection_Insert_Get_Test()
        {
            var session = GetNewSession(KeyspaceName);
            session.Execute("CREATE TABLE tbl_timeuuid_collections (id int PRIMARY KEY, set_value set<timeuuid>, list_value list<timeuuid>)");
            const string selectQuery = "SELECT * FROM tbl_timeuuid_collections WHERE id = ?";
            const string insertQuery = "INSERT INTO tbl_timeuuid_collections (id, set_value, list_value) VALUES (?, ?, ?)";
            var psInsert = session.Prepare(insertQuery);
            var set1 = new SortedSet<TimeUuid> { TimeUuid.NewId() };
            var list1 = new List<TimeUuid> { TimeUuid.NewId() };
            session.Execute(psInsert.Bind(1, set1, list1));
            var row1 = session.Execute(new SimpleStatement(selectQuery, 1)).First();
            CollectionAssert.AreEqual(set1, row1.GetValue<SortedSet<TimeUuid>>("set_value"));
            CollectionAssert.AreEqual(set1, row1.GetValue<ISet<TimeUuid>>("set_value"));
            CollectionAssert.AreEqual(set1, row1.GetValue<TimeUuid[]>("set_value"));
            CollectionAssert.AreEqual(list1, row1.GetValue<List<TimeUuid>>("list_value"));
            CollectionAssert.AreEqual(list1, row1.GetValue<TimeUuid[]>("list_value"));
        }

        [Test]
        public void Encode_Map_With_NullValue_Should_Throw()
        {
            var id = Guid.NewGuid();
            var localSession = GetNewSession(KeyspaceName);
            var insertQuery = localSession.Prepare(string.Format("INSERT INTO {0} (id, map_sample) VALUES (?, ?)",
                AllTypesTableName));

            var map = new SortedDictionary<string, string> { { "fruit", "apple" }, { "band", null } };
            var stmt = insertQuery.Bind(id, map);
            Assert.Throws<ArgumentNullException>(() => localSession.Execute(stmt));
        }

        [Test]
        public void Encode_List_With_NullValue_Should_Throw()
        {
            var id = Guid.NewGuid();
            var localSession = GetNewSession(KeyspaceName);
            var insertQuery = localSession.Prepare(string.Format("INSERT INTO {0} (id, list_sample) VALUES (?, ?)",
                AllTypesTableName));
            var map = new List<string> { "fruit", null };
            var stmt = insertQuery.Bind(id, map);
            Assert.Throws<ArgumentNullException>(() => localSession.Execute(stmt));
        }

        public void CheckingOrderOfCollection(string CassandraCollectionType, Type TypeOfDataToBeInputed, Type TypeOfKeyForMap = null,string pendingMode = "")
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfDataToBeInputed);
            string openBracket = CassandraCollectionType == "list" ? "[" : "{";
            string closeBracket = CassandraCollectionType == "list" ? "]" : "}";
            string mapSyntax = "";

            string randomKeyValue = string.Empty;

            if (TypeOfKeyForMap != null)
            {
                string cassandraKeyDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfKeyForMap);
                mapSyntax = cassandraKeyDataTypeName + ",";

                if (TypeOfKeyForMap == typeof (DateTimeOffset))
                    randomKeyValue =
                        Randomm.RandomVal(typeof (DateTimeOffset))
                               .GetType()
                               .GetMethod("ToString", new[] {typeof (string)})
                               .Invoke(Randomm.RandomVal(typeof (DateTimeOffset)), new object[1] {"yyyy-MM-dd H:mm:sszz00"}) + "' : '";
                else
                    randomKeyValue = Randomm.RandomVal(TypeOfDataToBeInputed) + "' : '";
            }


            var tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
                    tweet_id uuid PRIMARY KEY,
                    some_collection {1}<{2}{3}>
                    );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }
            Guid tweet_id = Guid.NewGuid();

            var longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int collectionElementsNo = 100;
            var orderedAsInputed = new List<Int32>(collectionElementsNo);

            string inputSide = "some_collection + {1}";
            if (CassandraCollectionType == "list" && pendingMode == "prepending")
                inputSide = "{1} + some_collection";

            for (int i = 0; i < collectionElementsNo; i++)
            {
                int data = i*(i%2);
                longQ.AppendFormat(@"UPDATE {0} SET some_collection = " + inputSide + " WHERE tweet_id = {2};"
                                   , tableName, openBracket + randomKeyValue + data + closeBracket, tweet_id);
                orderedAsInputed.Add(data);
            }

            longQ.AppendLine("APPLY BATCH;");
            QueryTools.ExecuteSyncNonQuery(Session, longQ.ToString(), "Inserting...");

            if (CassandraCollectionType == "set")
            {
                orderedAsInputed.Sort();
                orderedAsInputed.RemoveRange(0, orderedAsInputed.LastIndexOf(0));
            }
            else if (CassandraCollectionType == "list" && pendingMode == "prepending")
                orderedAsInputed.Reverse();

            var rs = Session.Execute(string.Format("SELECT * FROM {0};", tableName),
                                            Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
            {
                int ind = 0;
                foreach (Row row in rs.GetRows())
                    foreach (object value in row[1] as IEnumerable)
                    {
                        Assert.True(orderedAsInputed[ind] == (int) value);
                        ind++;
                    }
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }

        public void InsertingSingleCollection(string cassandraCollectionType, Type typeOfDataToBeInputed, Type typeOfKeyForMap = null)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(typeOfDataToBeInputed);
            string cassandraKeyDataTypeName = "";

            string openBracket = cassandraCollectionType == "list" ? "[" : "{";
            string closeBracket = cassandraCollectionType == "list" ? "]" : "}";
            string mapSyntax = "";

            object randomValue = Randomm.RandomVal(typeOfDataToBeInputed);
            if (typeOfDataToBeInputed == typeof (string))
                randomValue = "'" + randomValue.ToString().Replace("'", "''") + "'";

            string randomKeyValue = string.Empty;

            if (typeOfKeyForMap != null)
            {
                cassandraKeyDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(typeOfKeyForMap);
                mapSyntax = cassandraKeyDataTypeName + ",";

                if (typeOfKeyForMap == typeof (DateTimeOffset))
                    randomKeyValue = "'" +
                                     (Randomm.RandomVal(typeof (DateTimeOffset))
                                             .GetType()
                                             .GetMethod("ToString", new[] {typeof (string)})
                                             .Invoke(Randomm.RandomVal(typeof (DateTimeOffset)), new object[1] {"yyyy-MM-dd H:mm:sszz00"}) + "'");
                else if (typeOfKeyForMap == typeof (string))
                    randomKeyValue = "'" + Randomm.RandomVal(typeOfDataToBeInputed).ToString().Replace("'", "''") + "'";
                else
                    randomKeyValue = Randomm.RandomVal(typeOfDataToBeInputed).ToString();
            }

            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         some_collection {1}<{2}{3}>
         );", tableName, cassandraCollectionType, mapSyntax, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();


            QueryTools.ExecuteSyncNonQuery(Session,
                                           string.Format("INSERT INTO {0}(tweet_id,some_collection) VALUES ({1}, {2});", tableName, tweet_id,
                                                         openBracket + randomKeyValue + (string.IsNullOrEmpty(randomKeyValue) ? "" : " : ") +
                                                         randomValue + closeBracket));

            var longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int CollectionElementsNo = 100;
            object rval = Randomm.RandomVal(typeOfDataToBeInputed);
            for (int i = 0; i < CollectionElementsNo; i++)
            {
                object val = rval;
                if (typeOfDataToBeInputed == typeof (string))
                    val = "'" + val.ToString().Replace("'", "''") + "'";

                longQ.AppendFormat(@"UPDATE {0} SET some_collection = some_collection + {1} WHERE tweet_id = {2};"
                                   , tableName,
                                   openBracket + randomKeyValue + (string.IsNullOrEmpty(randomKeyValue) ? "" : " : ") + val + closeBracket, tweet_id);
            }
            longQ.AppendLine("APPLY BATCH;");
            QueryTools.ExecuteSyncNonQuery(Session, longQ.ToString(), "Inserting...");

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }

        public void insertingSingleCollectionPrepared(string CassandraCollectionType, Type TypeOfDataToBeInputed, Type TypeOfKeyForMap = null)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfDataToBeInputed);
            string cassandraKeyDataTypeName = "";
            string mapSyntax = "";

            object valueCollection = null;

            int Cnt = 10;

            if (CassandraCollectionType == "list" || CassandraCollectionType == "set")
            {
                Type openType = CassandraCollectionType == "list" ? typeof (List<>) : typeof (HashSet<>);
                Type listType = openType.MakeGenericType(TypeOfDataToBeInputed);
                valueCollection = Activator.CreateInstance(listType);
                MethodInfo addM = listType.GetMethod("Add");
                for (int i = 0; i < Cnt; i++)
                {
                    object randomValue = Randomm.RandomVal(TypeOfDataToBeInputed);
                    addM.Invoke(valueCollection, new[] {randomValue});
                }
            }
            else if (CassandraCollectionType == "map")
            {
                cassandraKeyDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfKeyForMap);
                mapSyntax = cassandraKeyDataTypeName + ",";

                Type openType = typeof (SortedDictionary<,>);
                Type dicType = openType.MakeGenericType(TypeOfKeyForMap, TypeOfDataToBeInputed);
                valueCollection = Activator.CreateInstance(dicType);
                MethodInfo addM = dicType.GetMethod("Add");
                for (int i = 0; i < Cnt; i++)
                {
                    RETRY:
                    try
                    {
                        object randomKey = Randomm.RandomVal(TypeOfKeyForMap);
                        object randomValue = Randomm.RandomVal(TypeOfDataToBeInputed);
                        addM.Invoke(valueCollection, new[] {randomKey, randomValue});
                    }
                    catch
                    {
                        goto RETRY;
                    }
                }
            }

            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
                 tweet_id uuid PRIMARY KEY,
                 some_collection {1}<{2}{3}>
                 );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();
            PreparedStatement prepInsert = QueryTools.PrepareQuery(Session,
                                                                   string.Format("INSERT INTO {0}(tweet_id,some_collection) VALUES (?, ?);", tableName));
            Session.Execute(prepInsert.Bind(tweet_id, valueCollection).SetConsistencyLevel(ConsistencyLevel.Quorum));
            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }

        [Test]
        public void ListOrderPrepending()
        {
            CheckingOrderOfCollection("list", typeof (Int32), null, "prepending");
        }

        [Test]
        public void ListOrderAppending()
        {
            CheckingOrderOfCollection("list", typeof (Int32), null, "appending");
        }

        [Test]
        public void SetOrder()
        {
            CheckingOrderOfCollection("set", typeof (Int32));
        }

        [Test]
        public void Map()
        {
            InsertingSingleCollection("map", typeof (string), typeof (DateTimeOffset));
        }

        [Test]
        public void MapDouble()
        {
            InsertingSingleCollection("map", typeof (Double), typeof (DateTimeOffset));
        }

        [Test]
        public void MapInt32()
        {
            InsertingSingleCollection("map", typeof (Int32), typeof (DateTimeOffset));
        }

        [Test]
        public void MapInt64()
        {
            InsertingSingleCollection("map", typeof (Int64), typeof (DateTimeOffset));
        }

        [Test]
        public void ListDouble()
        {
            InsertingSingleCollection("list", typeof (Double));
        }

        [Test]
        public void ListInt64()
        {
            InsertingSingleCollection("list", typeof (Int64));
        }

        [Test]
        public void ListInt32()
        {
            InsertingSingleCollection("list", typeof (Int32));
        }

        [Test]
        public void ListString()
        {
            InsertingSingleCollection("list", typeof (string));
        }

        [Test]
        public void SetString()
        {
            InsertingSingleCollection("set", typeof (string));
        }

        [Test]
        public void SetDouble()
        {
            InsertingSingleCollection("set", typeof (Double));
        }

        [Test]
        public void SetInt32()
        {
            InsertingSingleCollection("set", typeof (Int32));
        }

        [Test]
        public void SetInt64()
        {
            InsertingSingleCollection("set", typeof (Int64));
        }

        [Test]
        public void MapPrepared()
        {
            insertingSingleCollectionPrepared("map", typeof (string), typeof (DateTimeOffset));
        }

        [Test]
        public void MapDoublePrepared()
        {
            insertingSingleCollectionPrepared("map", typeof (Double), typeof (DateTimeOffset));
        }

        [Test]
        public void MapInt32Prepared()
        {
            insertingSingleCollectionPrepared("map", typeof (Int32), typeof (DateTimeOffset));
        }

        [Test]
        public void MapInt64Prepared()
        {
            insertingSingleCollectionPrepared("map", typeof (Int64), typeof (DateTimeOffset));
        }

        [Test]
        public void ListDoublePrepared()
        {
            insertingSingleCollectionPrepared("list", typeof (Double));
        }

        [Test]
        public void ListInt64Prepared()
        {
            insertingSingleCollectionPrepared("list", typeof (Int64));
        }

        [Test]
        public void ListInt32Prepared()
        {
            insertingSingleCollectionPrepared("list", typeof (Int32));
        }

        [Test]
        public void ListStringPrepared()
        {
            insertingSingleCollectionPrepared("list", typeof (string));
        }

        [Test]
        public void SetStringPrepared()
        {
            insertingSingleCollectionPrepared("set", typeof (string));
        }

        [Test]
        public void SetDoublePrepared()
        {
            insertingSingleCollectionPrepared("set", typeof (Double));
        }

        [Test]
        public void SetInt32Prepared()
        {
            insertingSingleCollectionPrepared("set", typeof (Int32));
        }

        [Test]
        public void SetInt64Prepared()
        {
            insertingSingleCollectionPrepared("set", typeof (Int64));
        }
    }
}
