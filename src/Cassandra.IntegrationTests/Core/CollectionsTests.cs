//
//      Copyright (C) 2012 DataStax Inc.
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
#if MYTEST

#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;

namespace Cassandra.IntegrationTests.Core
{
    [TestClass]
    public class CollectionsTests
    {
        public CollectionsTests()
        {
        }

        Session Session;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

        public void checkingOrderOfCollection(string CassandraCollectionType, Type TypeOfDataToBeInputed, Type TypeOfKeyForMap = null, string pendingMode = "")
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfDataToBeInputed);
            string cassandraKeyDataTypeName = "";

            string openBracket = CassandraCollectionType == "list" ? "[" : "{";
            string closeBracket = CassandraCollectionType == "list" ? "]" : "}";
            string mapSyntax = "";

            string randomKeyValue = string.Empty;

            if (TypeOfKeyForMap != null)
            {
                cassandraKeyDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfKeyForMap);
                mapSyntax = cassandraKeyDataTypeName + ",";

                if (TypeOfKeyForMap == typeof(DateTimeOffset))
                    randomKeyValue = (string)(Randomm.RandomVal(typeof(DateTimeOffset)).GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(Randomm.RandomVal(typeof(DateTimeOffset)), new object[1] { "yyyy-MM-dd H:mm:sszz00" }) + "' : '");
                else
                    randomKeyValue = Randomm.RandomVal(TypeOfDataToBeInputed) + "' : '";
            }


            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                Session.WaitForSchemaAgreement(
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         some_collection {1}<{2}{3}>
         );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName)));
            }
            catch (AlreadyExistsException)
            {
            }
            Guid tweet_id = Guid.NewGuid();

            StringBuilder longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int CollectionElementsNo = 100;
            List<Int32> orderedAsInputed = new List<Int32>(CollectionElementsNo);

            string inputSide = "some_collection + {1}";
            if (CassandraCollectionType == "list" && pendingMode == "prepending")
                inputSide = "{1} + some_collection";

            for (int i = 0; i < CollectionElementsNo; i++)
            {
                var data = i * (i % 2);
                longQ.AppendFormat(@"UPDATE {0} SET some_collection = " + inputSide + " WHERE tweet_id = {2};"
                    , tableName, openBracket + randomKeyValue + data + closeBracket, tweet_id.ToString());
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

            using (var rs = Session.Execute(string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.Default))
            {
                int ind = 0;
                foreach (var row in rs.GetRows())
                    foreach (var value in row[1] as System.Collections.IEnumerable)
                    {
                        Assert.True(orderedAsInputed[ind] == (int)value);
                        ind++;
                    }
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName));
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void insertingSingleCollection(string CassandraCollectionType, Type TypeOfDataToBeInputed, Type TypeOfKeyForMap = null)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfDataToBeInputed);
            string cassandraKeyDataTypeName = "";

            string openBracket = CassandraCollectionType == "list" ? "[" : "{";
            string closeBracket = CassandraCollectionType == "list" ? "]" : "}";
            string mapSyntax = "";

            var randomValue = Randomm.RandomVal(TypeOfDataToBeInputed);
            if (TypeOfDataToBeInputed == typeof(string))
                randomValue = "'" + randomValue.ToString().Replace("'", "''") + "'";

            string randomKeyValue = string.Empty;

            if (TypeOfKeyForMap != null)
            {
                cassandraKeyDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfKeyForMap);
                mapSyntax = cassandraKeyDataTypeName + ",";

                if (TypeOfKeyForMap == typeof(DateTimeOffset))
                    randomKeyValue = "'" + (string)(Randomm.RandomVal(typeof(DateTimeOffset)).GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(Randomm.RandomVal(typeof(DateTimeOffset)), new object[1] { "yyyy-MM-dd H:mm:sszz00" }) + "'");
                else if (TypeOfKeyForMap == typeof(string))
                    randomKeyValue = "'" + Randomm.RandomVal(TypeOfDataToBeInputed).ToString().Replace("'", "''") + "'";
                else
                    randomKeyValue = Randomm.RandomVal(TypeOfDataToBeInputed).ToString();
            }

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.WaitForSchemaAgreement(
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         some_collection {1}<{2}{3}>
         );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName))
                    );
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();


            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,some_collection) VALUES ({1}, {2});", tableName, tweet_id.ToString(), openBracket + randomKeyValue + (string.IsNullOrEmpty(randomKeyValue) ? "" : " : ") + randomValue + closeBracket));

            StringBuilder longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int CollectionElementsNo = 100;
            var rval = Randomm.RandomVal(TypeOfDataToBeInputed);
            for (int i = 0; i < CollectionElementsNo; i++)
            {
                var val = rval;
                if (TypeOfDataToBeInputed == typeof(string))
                    val = "'" + val.ToString().Replace("'", "''") + "'";

                longQ.AppendFormat(@"UPDATE {0} SET some_collection = some_collection + {1} WHERE tweet_id = {2};"
                    , tableName, openBracket + randomKeyValue + (string.IsNullOrEmpty(randomKeyValue) ? "" : " : ") + val + closeBracket, tweet_id.ToString());
            }
            longQ.AppendLine("APPLY BATCH;");
            QueryTools.ExecuteSyncNonQuery(Session, longQ.ToString(), "Inserting...");

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName));
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void insertingSingleCollectionPrepared(string CassandraCollectionType, Type TypeOfDataToBeInputed, Type TypeOfKeyForMap = null)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfDataToBeInputed);
            string cassandraKeyDataTypeName = "";
            string mapSyntax = "";

            object valueCollection = null;

            int Cnt = 10;

            if(CassandraCollectionType=="list" || CassandraCollectionType=="set")
            {
                var openType = CassandraCollectionType=="list"? typeof(List<>) : typeof(HashSet<>);
                var listType = openType.MakeGenericType(TypeOfDataToBeInputed);
                valueCollection = Activator.CreateInstance(listType);
                var addM = listType.GetMethod("Add");
                for (int i = 0; i < Cnt; i++)
                {
                    var randomValue = Randomm.RandomVal(TypeOfDataToBeInputed);
                    addM.Invoke(valueCollection, new object[] { randomValue });
                }
            }
            else if (CassandraCollectionType=="map")
            {
                cassandraKeyDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(TypeOfKeyForMap);
                mapSyntax = cassandraKeyDataTypeName + ",";

                var openType = typeof(SortedDictionary<,>);
                var dicType = openType.MakeGenericType(TypeOfKeyForMap,TypeOfDataToBeInputed);
                valueCollection = Activator.CreateInstance(dicType);
                var addM = dicType.GetMethod("Add");
                for (int i = 0; i < Cnt; i++)
                {
                    RETRY:
                    try
                    {
                        var randomKey = Randomm.RandomVal(TypeOfKeyForMap);
                        var randomValue = Randomm.RandomVal(TypeOfDataToBeInputed);
                        addM.Invoke(valueCollection, new object[] { randomKey, randomValue });
                    }
                    catch
                    {
                        goto RETRY;
                    }
                }
            }

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.WaitForSchemaAgreement(
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         some_collection {1}<{2}{3}>
         );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName))
                    );
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();
            var prepInsert = QueryTools.PrepareQuery(Session, string.Format("INSERT INTO {0}(tweet_id,some_collection) VALUES (?, ?);", tableName));
            Session.Execute(prepInsert.Bind(tweet_id, valueCollection).SetConsistencyLevel(ConsistencyLevel.Quorum));
            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName));
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        [TestMethod]
        [WorksForMe]
        public void testListOrderPrepending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "prepending");
        }

        [TestMethod]
        [WorksForMe]
        public void testListOrderAppending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "appending");
        }

        [TestMethod]
        [WorksForMe]
        public void testSetOrder()
        {
            checkingOrderOfCollection("set", typeof(System.Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void testMap()
        {
            insertingSingleCollection("map", typeof(string), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapDouble()
        {
            insertingSingleCollection("map", typeof(Double), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapInt32()
        {
            insertingSingleCollection("map", typeof(Int32), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapInt64()
        {
            insertingSingleCollection("map", typeof(Int64), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testListDouble()
        {
            insertingSingleCollection("list", typeof(Double));
        }

        [TestMethod]
        [WorksForMe]
        public void testListInt64()
        {
            insertingSingleCollection("list", typeof(Int64));
        }

        [TestMethod]
        [WorksForMe]
        public void testListInt32()
        {
            insertingSingleCollection("list", typeof(Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void testListString()
        {
            insertingSingleCollection("list", typeof(string));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetString()
        {
            insertingSingleCollection("set", typeof(string));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetDouble()
        {
            insertingSingleCollection("set", typeof(Double));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetInt32()
        {
            insertingSingleCollection("set", typeof(Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetInt64()
        {
            insertingSingleCollection("set", typeof(Int64));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapPrepared()
        {
            insertingSingleCollectionPrepared("map", typeof(string), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapDoublePrepared()
        {
            insertingSingleCollectionPrepared("map", typeof(Double), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapInt32Prepared()
        {
            insertingSingleCollectionPrepared("map", typeof(Int32), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapInt64Prepared()
        {
            insertingSingleCollectionPrepared("map", typeof(Int64), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testListDoublePrepared()
        {
            insertingSingleCollectionPrepared("list", typeof(Double));
        }

        [TestMethod]
        [WorksForMe]
        public void testListInt64Prepared()
        {
            insertingSingleCollectionPrepared("list", typeof(Int64));
        }

        [TestMethod]
        [WorksForMe]
        public void testListInt32Prepared()
        {
            insertingSingleCollectionPrepared("list", typeof(Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void testListStringPrepared()
        {
            insertingSingleCollectionPrepared("list", typeof(string));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetStringPrepared()
        {
            insertingSingleCollectionPrepared("set", typeof(string));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetDoublePrepared()
        {
            insertingSingleCollectionPrepared("set", typeof(Double));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetInt32Prepared()
        {
            insertingSingleCollectionPrepared("set", typeof(Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetInt64Prepared()
        {
            insertingSingleCollectionPrepared("set", typeof(Int64));
        }
    }
}
