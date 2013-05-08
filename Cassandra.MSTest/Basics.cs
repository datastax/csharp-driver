using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using System.Threading;
using System.Globalization;
using System.Threading.Tasks;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif
using System.Text;

namespace Cassandra.MSTest
{    
    public partial class BasicTests
    {
        string Keyspace = "tester";
        Cluster Cluster;
        Session Session;

        public BasicTests()
        {
        }

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            var clusterb = Cluster.Builder().AddContactPoint("cassi.cloudapp.net");
            clusterb.WithDefaultKeyspace(Keyspace);

            if (Cassandra.MSTest.Properties.Settings.Default.Compression)
            {
                clusterb.WithCompression(CompressionType.Snappy);
                Console.WriteLine("Compression: Snappy Compression");
            }
            else Console.WriteLine("Compression: No Compression");

            Cluster = clusterb.Build();
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Verbose;
            Diagnostics.CassandraStackTraceIncluded = true;
            Diagnostics.CassandraPerformanceCountersEnabled = true;
            Session = Cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(ReplicationStrategies.CreateSimpleStrategyReplicationProperty(1), true);
        }

        [TestCleanup]
        public void Dispose()
        {
            Session.DeleteKeyspace(Keyspace);
            Session.Dispose();
            Cluster.Shutdown();
        }    

        public void ExceedingCassandraType(Type toExceed, Type toExceedWith, bool sameOutput = true)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(toExceed);
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         label text,
         number {1}
         );", tableName, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }


            var Minimum = toExceedWith.GetField("MinValue").GetValue(this);
            var Maximum = toExceedWith.GetField("MaxValue").GetValue(this);


            object[] row1 = new object[3] { Guid.NewGuid(), "Minimum", Minimum };
            object[] row2 = new object[3] { Guid.NewGuid(), "Maximum", Maximum };
            List<object[]> toInsert_and_Check = new List<object[]>(2) { row1, row2 };

            if (toExceedWith == typeof(Double) || toExceedWith == typeof(Single))
            {
                Minimum = Minimum.GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(Minimum, new object[1] { "r" });
                Maximum = Maximum.GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(Maximum, new object[1] { "r" });

                if (!sameOutput) //for ExceedingCassandra_FLOAT() test case
                {
                    toInsert_and_Check[0][2] = Single.NegativeInfinity;
                    toInsert_and_Check[1][2] = Single.PositiveInfinity;
                }
            }

            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName, toInsert_and_Check[0][0], toInsert_and_Check[0][1], Minimum), null);
                QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName, toInsert_and_Check[1][0].ToString(), toInsert_and_Check[1][1], Maximum), null);
            }
            catch (InvalidException)
            {
                if (!sameOutput && toExceed == typeof(Int32)) //for ExceedingCassandra_INT() test case
                {
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
                    Assert.True(true);
                    return;
                }
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), toInsert_and_Check);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }


        public void testCounters()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         incdec counter
         );", tableName));
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();

            Parallel.For(0, 100, i =>
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"UPDATE {0} SET incdec = incdec {2}  WHERE tweet_id = {1};", tableName, tweet_id, (i % 2 == 0 ? "-" : "+") + i));
            });

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), new List<object[]> { new object[2] { tweet_id, (Int64)50 } });
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void insertingSingleValue(Type tp)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(tp);
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         value {1}
         );", tableName, cassandraDataTypeName));
            }
            catch (AlreadyExistsException)
            {
            }

            List<object[]> toInsert = new List<object[]>(1);
            var val = Randomm.RandomVal(tp);
            if (tp == typeof(string))
                val = "'" + val.ToString().Replace("'", "''") + "'";
            object[] row1 = new object[2] { Guid.NewGuid(), val };
            toInsert.Add(row1);

            bool isFloatingPoint = false;

            if (row1[1].GetType() == typeof(string) || row1[1].GetType() == typeof(byte[]))
                QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0].ToString(), row1[1].GetType() == typeof(byte[]) ? "0x" + Cassandra.CqlQueryTools.ToHex((byte[])toInsert[0][1]) : "'" + toInsert[0][1] + "'"), null); // rndm.GetType().GetMethod("Next" + tp.Name).Invoke(rndm, new object[] { })
            else
            {
                if (tp == typeof(Single) || tp == typeof(Double))
                    isFloatingPoint = true;
                QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0].ToString(), !isFloatingPoint ? toInsert[0][1] : toInsert[0][1].GetType().GetMethod("ToString", new Type[] { typeof(string) }).Invoke(toInsert[0][1], new object[] { "r" })), null);
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), toInsert);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }





        public void TimestampTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");
            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         ts timestamp
         );", tableName));

            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), "2011-02-03 04:05+0000"), null);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), 220898707200000), null);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), 0), null);

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName));
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void createSecondaryIndexTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");
            string columns = "tweet_id uuid, name text, surname text";

            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         {1},
PRIMARY KEY(tweet_id)
         );", tableName, columns));
            }
            catch (AlreadyExistsException)
            {
            }

            object[] row1 = new object[3] { Guid.NewGuid(), "Adam", "Małysz" };
            object[] row2 = new object[3] { Guid.NewGuid(), "Adam", "Miałczyński" };

            List<object[]> toReturn = new List<object[]>(2) { row1, row2 };
            List<object[]> toInsert = new List<object[]>(2) { row1, row2 };

            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, name, surname) VALUES({1},'{2}','{3}');", tableName, toInsert[0][0], toInsert[0][1], toInsert[0][2]));
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, name, surname) VALUES({1},'{2}','{3}');", tableName, toInsert[1][0], toInsert[1][1], toInsert[1][2]));

            int RowsNb = 0;

            for (int i = 0; i < RowsNb; i++)
            {
                toInsert.Add(new object[3] { Guid.NewGuid(), i.ToString(), "Miałczyński" });
                QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id, name, surname) VALUES({1},'{2}','{3}');", tableName, toInsert[i][0], toInsert[i][1], toInsert[i][2]));
            }

            QueryTools.ExecuteSyncNonQuery(Session, string.Format("CREATE INDEX ON {0}(name);", tableName));
            Thread.Sleep(50);
            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0} WHERE name = 'Adam';", tableName), toReturn);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }


        public void checkMetadata(string TableName = null, string KeyspaceName = null, TableOptions tableOptions = null)
        {
            Dictionary<string, ColumnTypeCode> columns = new Dictionary
                <string, ColumnTypeCode>()
                {
                    {"q0uuid", ColumnTypeCode.Uuid},
                    {"q1timestamp", ColumnTypeCode.Timestamp},
                    {"q2double", ColumnTypeCode.Double},
                    {"q3int32", ColumnTypeCode.Int},
                    {"q4int64", ColumnTypeCode.Bigint},
                    {"q5float", ColumnTypeCode.Float},
                    {"q6inet", ColumnTypeCode.Inet},
                    {"q7boolean", ColumnTypeCode.Boolean},
                    {"q8inet", ColumnTypeCode.Inet},
                    {"q9blob", ColumnTypeCode.Blob},
#if NET_40_OR_GREATER
                         {"q10varint", ColumnTypeCode.Varint},
                         {"q11decimal", ColumnTypeCode.Decimal},
#endif
                    {"q12list", ColumnTypeCode.List},
                    {"q13set", ColumnTypeCode.Set},
                    {"q14map", ColumnTypeCode.Map}
                    //{"q12counter", Metadata.ColumnTypeCode.Counter}, A table that contains a counter can only contain counters
                };
            
            string tablename = TableName ?? "table" + Guid.NewGuid().ToString("N");
            StringBuilder sb = new StringBuilder(@"CREATE TABLE " + tablename + " (");
            Randomm urndm = new Randomm(DateTimeOffset.Now.Millisecond);

            foreach (var col in columns)
                sb.Append(col.Key + " " + col.Value.ToString() +
                          (((col.Value == ColumnTypeCode.List) ||
                            (col.Value == ColumnTypeCode.Set) ||
                            (col.Value == ColumnTypeCode.Map))
                               ? "<int" + (col.Value == ColumnTypeCode.Map ? ",varchar>" : ">")
                               : "") + ", ");

            sb.Append("PRIMARY KEY(");
            int rowKeys = urndm.Next(1, columns.Count - 3);

            for (int i = 0; i < rowKeys; i++)
                sb.Append(columns.Keys.Where(key => key.StartsWith("q" + i.ToString())).First() +
                          ((i == rowKeys - 1) ? "" : ", "));
            var opt = tableOptions != null ? " WITH " + tableOptions.ToString() : "";
            sb.Append("))" + opt + ";");

            QueryTools.ExecuteSyncNonQuery(Session, sb.ToString());

            var table = this.Cluster.Metadata.GetTable(KeyspaceName ?? Keyspace, tablename);
            foreach (var metaCol in table.TableColumns)
            {
                Assert.True(columns.Keys.Contains(metaCol.Name));
                Assert.True(metaCol.TypeCode ==
                            columns.Where(tpc => tpc.Key == metaCol.Name).First().Value);
                Assert.True(metaCol.Table == tablename);
                Assert.True(metaCol.Keyspace == (KeyspaceName ?? Keyspace));
            }
            if (tableOptions != null)                                   
                Assert.True(tableOptions.Equals(table.Options));
            
        }


        public void checkKSMetadata()
        {
            string keyspacename = "keyspace" + Guid.NewGuid().ToString("N").ToLower();
            bool durableWrites = false;
            string strgyClass = "SimpleStrategy";
            short rplctnFactor = 1;
            Session.Execute(
string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : '{1}', 'replication_factor' : {2} }}
         AND durable_writes={3};"
, keyspacename, strgyClass, rplctnFactor.ToString(), durableWrites.ToString()));

            Session.ChangeKeyspace(keyspacename);


            for (int i = 0; i < 10; i++)
                checkMetadata("table" + Guid.NewGuid().ToString("N"), keyspacename);

            KeyspaceMetadata ksmd = Cluster.Metadata.GetKeyspace(keyspacename);
            Assert.True(ksmd.DurableWrites == durableWrites);
            Assert.True(ksmd.Replication.Where(opt => opt.Key == "replication_factor").First().Value == rplctnFactor);
            Assert.True(ksmd.StrategyClass == strgyClass);
        }

        public void CreateKeyspaceWithPropertiesTest(string strategy_class)
        {
            Randomm rndm = new Randomm(DateTime.Now.Millisecond);
            Session.DeleteKeyspaceIfExists(Keyspace);
            bool durable_writes = rndm.NextBoolean();

            int? replication_factor = null;
            int? data_centers_count = null;
            Dictionary<string, int> datacenters_replication_factors = null;

            if (strategy_class == ReplicationStrategies.SimpleStrategy)
            {
                replication_factor = rndm.Next(1, 21);
                Session = Cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(ReplicationStrategies.CreateSimpleStrategyReplicationProperty((int)replication_factor), durable_writes);
            }
            else
                if (strategy_class == ReplicationStrategies.NetworkTopologyStrategy)
                {
                    data_centers_count = rndm.Next(1, 11);
                    datacenters_replication_factors = new Dictionary<string, int>((int)data_centers_count);
                    for (int i = 0; i < data_centers_count; i++)
                        datacenters_replication_factors.Add("dc" + i.ToString(), rndm.Next(1, 21));
                    Session = Cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(ReplicationStrategies.CreateNetworkTopologyStrategyReplicationProperty(datacenters_replication_factors), durable_writes);
                }

            KeyspaceMetadata ksmd = Cluster.Metadata.GetKeyspace(Keyspace);
            Assert.Equal(strategy_class, ksmd.StrategyClass);
            Assert.Equal(durable_writes, ksmd.DurableWrites);
            if (replication_factor != null)
                Assert.Equal(replication_factor, ksmd.Replication["replication_factor"]);
            if (datacenters_replication_factors != null)
                Assert.True(datacenters_replication_factors.SequenceEqual(ksmd.Replication));
        }

        public void BigInsertTest(int RowsNo = 5000)
        {
            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();
            Randomm rndm = new Randomm(DateTime.Now.Millisecond);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};"
                , keyspaceName));

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
		 fval float,
		 dval double,
         PRIMARY KEY(tweet_id))", tableName));
            }
            catch (AlreadyExistsException)
            {
            }

            StringBuilder longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            for (int i = 0; i < RowsNo; i++)
            {
                longQ.AppendFormat(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body,
		 fval,
		 dval)
VALUES ({1},'test{2}',{3},'body{2}',{4},{5});", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true", rndm.NextSingle(), rndm.NextDouble());
            }
            longQ.AppendLine("APPLY BATCH;");
            QueryTools.ExecuteSyncNonQuery(Session, longQ.ToString(), "Inserting...");
            QueryTools.ExecuteSyncQuery(Session, string.Format(@"SELECT * from {0};", tableName));
            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"DROP TABLE {0};", tableName));

            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"DROP KEYSPACE {0};", keyspaceName));
        }

    }
}
