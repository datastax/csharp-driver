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

#if NET_40_OR_GREATER
using System.Numerics;
#endif

namespace Cassandra.MSTest
{
    [TestClass]
    public class MetadataTests
    {
        string Keyspace = "tester";

        Cluster Cluster;
        Session Session;
        CCMBridge.CCMCluster CCMCluster;


        public MetadataTests()
        {
        }

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }

        [TestCleanup]
        public void Dispose()
        {
        }

        public void checkPureMetadata(string TableName = null, string KeyspaceName = null, TableOptions tableOptions = null)
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
        
        public void checkMetadata(string TableName = null, string KeyspaceName = null, TableOptions tableOptions = null)
        {
            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            try
            {
                Session = CCMCluster.Session;
                Cluster = CCMCluster.Cluster;
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Thread.Sleep(1000);
                Session.ChangeKeyspace(Keyspace);

                checkPureMetadata(TableName, KeyspaceName, tableOptions);
            }
            finally
            {
                CCMCluster.Discard();
            }
        }

        public void checkKSMetadata()
        {
            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            try
            {
                Session = CCMCluster.Session;
                Cluster = CCMCluster.Cluster;
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Thread.Sleep(1000);
                Session.ChangeKeyspace(Keyspace);

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
                    checkPureMetadata("table" + Guid.NewGuid().ToString("N"), keyspacename);

                KeyspaceMetadata ksmd = Cluster.Metadata.GetKeyspace(keyspacename);
                Assert.True(ksmd.DurableWrites == durableWrites);
                Assert.True(ksmd.Replication.Where(opt => opt.Key == "replication_factor").First().Value == rplctnFactor);
                Assert.True(ksmd.StrategyClass == strgyClass);

            }
            finally
            {
                CCMCluster.Discard();
            }
        }

        public void CreateKeyspaceWithPropertiesTest(string strategy_class)
        {
            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            try
            {
                Session = CCMCluster.Session;
                Cluster = CCMCluster.Cluster;

                Randomm rndm = new Randomm(DateTime.Now.Millisecond);
                bool durable_writes = rndm.NextBoolean();

                int? replication_factor = null;
                int? data_centers_count = null;
                Dictionary<string, int> datacenters_replication_factors = null;

                if (strategy_class == ReplicationStrategies.SimpleStrategy)
                {
                    replication_factor = rndm.Next(1, 21);
                    Session.CreateKeyspaceIfNotExists(Keyspace,ReplicationStrategies.CreateSimpleStrategyReplicationProperty((int)replication_factor), durable_writes);
                    Thread.Sleep(1000);
                    Session.ChangeKeyspace(Keyspace);
                }
                else
                    if (strategy_class == ReplicationStrategies.NetworkTopologyStrategy)
                    {
                        data_centers_count = rndm.Next(1, 11);
                        datacenters_replication_factors = new Dictionary<string, int>((int)data_centers_count);
                        for (int i = 0; i < data_centers_count; i++)
                            datacenters_replication_factors.Add("dc" + i.ToString(), rndm.Next(1, 21));
                        Session.CreateKeyspaceIfNotExists(Keyspace, ReplicationStrategies.CreateNetworkTopologyStrategyReplicationProperty(datacenters_replication_factors), durable_writes);
                    }

                KeyspaceMetadata ksmd = Cluster.Metadata.GetKeyspace(Keyspace);
                Assert.Equal(strategy_class, ksmd.StrategyClass);
                Assert.Equal(durable_writes, ksmd.DurableWrites);
                if (replication_factor != null)
                    Assert.Equal(replication_factor, ksmd.Replication["replication_factor"]);
                if (datacenters_replication_factors != null)
                    Assert.True(datacenters_replication_factors.SequenceEqual(ksmd.Replication));
            }
            finally
            {
                CCMCluster.Discard();
            }
        }


        [TestMethod]
        [Ignore]//OK
        public void checkSimpleStrategyKeyspace()
        {
            CreateKeyspaceWithPropertiesTest(ReplicationStrategies.SimpleStrategy);
        }

        [TestMethod]
        [Ignore]//OK
        public void checkNetworkTopologyStrategyKeyspace()
        {
            CreateKeyspaceWithPropertiesTest(ReplicationStrategies.NetworkTopologyStrategy);
        }

        [TestMethod]
        [Ignore]//OK
        public void checkTableMetadata()
        {
            checkMetadata();
        }

        [TestMethod]
        [Ignore]//OK
        public void checkTableMetadataWithOptions()
        {
            checkMetadata(tableOptions: new TableOptions("Comment", 0.5, 0.6, true, 42, 0.01, "ALL",
                new SortedDictionary<string, string> { { "class", "org.apache.cassandra.db.compaction.LeveledCompactionStrategy" }, { "sstable_size_in_mb", "15" } },
                new SortedDictionary<string, string> { { "sstable_compression", "org.apache.cassandra.io.compress.SnappyCompressor" }, { "chunk_length_kb", "128" } }));
        }

        [TestMethod]
        [Priority]
        public void checkKeyspaceMetadata()
        {
            checkKSMetadata();
        }
    }
}
