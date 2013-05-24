using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif
using System.Text;
using System.Threading;
using System.Globalization;

namespace Cassandra.MSTest
{    
    public partial class CollectionsTests
    {

        string Keyspace = "tester";
        Cluster Cluster;
        Session Session;
        CCMBridge.CCMCluster CCMCluster;


        public CollectionsTests()
        {
        }

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            Session = CCMCluster.Session;
            Cluster = CCMCluster.Cluster;
            Session.CreateKeyspaceIfNotExists(Keyspace);
            Thread.Sleep(1000);
            Session.ChangeKeyspace(Keyspace);
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMCluster.Discard();
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
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         some_collection {1}<{2}{3}>
         );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName));
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

            CqlRowSet rs = Session.Execute(string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.Default);

            using (rs)
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
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         some_collection {1}<{2}{3}>
         );", tableName, CassandraCollectionType, mapSyntax, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();

            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,some_collection) VALUES ({1}, {2});", tableName, tweet_id.ToString(), openBracket + randomKeyValue + (string.IsNullOrEmpty(randomKeyValue) ? "" : " : ") + randomValue + closeBracket));

            StringBuilder longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int CollectionElementsNo = 100;
            for (int i = 0; i < CollectionElementsNo; i++)
            {
                var val = Randomm.RandomVal(TypeOfDataToBeInputed);
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
    }
}
