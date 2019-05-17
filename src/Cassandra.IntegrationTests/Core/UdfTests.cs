using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class UdfTests : TestGlobals
    {
        private ITestCluster _testCluster;
        private readonly List<ICluster> _clusters = new List<ICluster>();

        private ICluster GetCluster(bool metadataSync)
        {
            var cluster = Cluster.Builder()
                                 .AddContactPoint(_testCluster.InitialContactPoint)
                                 .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync).SetRefreshSchemaDelayIncrement(1).SetMaxTotalRefreshSchemaDelay(5))
                                 .Build();
            _clusters.Add(cluster);
            return cluster;
        }

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            if (CassandraVersion < Version.Parse("2.2"))
            {
                return;
            }
            _testCluster = TestClusterManager.GetTestCluster(1, 0, false, DefaultMaxClusterCreateRetries, false, false);
            _testCluster.UpdateConfig("enable_user_defined_functions: true");
            _testCluster.Start(1);
            using (var cluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var queries = new[]
                {
                    "CREATE KEYSPACE  ks_udf WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
                    "CREATE FUNCTION  ks_udf.return_one() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return 1;'",
                    "CREATE FUNCTION  ks_udf.plus(s int, v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;'",
                    "CREATE FUNCTION  ks_udf.plus(s bigint, v bigint) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return s+v;'",
                    "CREATE AGGREGATE ks_udf.sum(int) SFUNC plus STYPE int INITCOND 1",
                    "CREATE AGGREGATE ks_udf.sum(bigint) SFUNC plus STYPE bigint INITCOND 2"
                };
                foreach (var q in queries)
                {
                    session.Execute(q);   
                }
            }
        }

        [TearDown]
        public void TearDown()
        {
            foreach (var cluster in _clusters)
            {
                try
                {
                    cluster.Shutdown(500);
                }
                catch (Exception ex)
                {
                    Trace.TraceError(ex.Message);
                }
            }
            _clusters.Clear();
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Retrieve_Metadata_Of_Cql_Function(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = ks.GetFunction("plus", new [] {"int", "int"});
            if (metadataSync)
            {
                //it is the same as retrieving from Metadata, it gets cached
                Assert.AreEqual(func, cluster.Metadata.GetFunction("ks_udf", "plus", new [] {"int", "int"}));
            }
            else
            {
                Assert.AreNotEqual(func, cluster.Metadata.GetFunction("ks_udf", "plus", new [] {"int", "int"}));
            }
            Assert.NotNull(func);
            Assert.AreEqual("plus", func.Name);
            Assert.AreEqual("ks_udf", func.KeyspaceName);
            CollectionAssert.AreEqual(new [] {"s", "v"}, func.ArgumentNames);
            Assert.AreEqual(2, func.ArgumentTypes.Length);
            Assert.AreEqual(ColumnTypeCode.Int, func.ArgumentTypes[0].TypeCode);
            Assert.AreEqual(ColumnTypeCode.Int, func.ArgumentTypes[1].TypeCode);
            Assert.AreEqual("return s+v;", func.Body);
            Assert.AreEqual("java", func.Language);
            Assert.AreEqual(ColumnTypeCode.Int, func.ReturnType.TypeCode);
            Assert.AreEqual(false, func.CalledOnNullInput);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Retrieve_Metadata_Of_Cql_Function_Without_Parameters(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = ks.GetFunction("return_one", new string[0]);
            Assert.NotNull(func);
            Assert.AreEqual("return_one", func.Name);
            Assert.AreEqual("ks_udf", func.KeyspaceName);
            Assert.AreEqual(0, func.ArgumentNames.Length);
            Assert.AreEqual(0, func.ArgumentTypes.Length);
            Assert.AreEqual(0, func.Signature.Length);
            Assert.AreEqual("return 1;", func.Body);
            Assert.AreEqual("java", func.Language);
            Assert.AreEqual(ColumnTypeCode.Int, func.ReturnType.TypeCode);
            Assert.AreEqual(false, func.CalledOnNullInput);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Be_Case_Sensitive(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.NotNull(ks.GetFunction("plus", new[] { "bigint", "bigint" }));
            Assert.Null(ks.GetFunction("PLUS", new[] { "bigint", "bigint" }));
            Assert.Null(ks.GetFunction("plus", new[] { "BIGINT", "bigint" }));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Null_When_Not_Found(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = ks.GetFunction("func_does_not_exists", new string[0]);
            Assert.Null(func);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Null_When_Not_Found_By_Signature(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = ks.GetFunction("plus", new[] { "text", "text" });
            Assert.Null(func);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Cache_The_Metadata(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.AreEqual(ks.GetFunction("plus", new[] { "text", "text" }), ks.GetFunction("plus", new[] { "text", "text" }));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Most_Up_To_Date_Metadata_Via_Events(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var session = cluster.Connect("ks_udf");
            var cluster2 = GetCluster(metadataSync);
            var session2 = cluster.Connect("ks_udf");
            session.Execute("CREATE OR REPLACE FUNCTION stringify(i int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(i);'");
            cluster2.RefreshSchema("ks_udf");
            Task.Delay(500).GetAwaiter().GetResult(); // wait for events to be processed
            var _ = cluster2.Metadata.KeyspacesSnapshot // cache 
                                .Single(kvp => kvp.Key == "ks_udf")
                                .Value
                                .GetFunction("stringify", new[] { "int" });
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = cluster.Metadata.GetFunction("ks_udf", "stringify", new[] { "int" });
            Assert.NotNull(func);
            Assert.AreEqual("return Integer.toString(i);", func.Body);
            session.Execute("CREATE OR REPLACE FUNCTION stringify(i int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(i) + \"hello\";'");
            if (metadataSync)
            {
                TestHelper.RetryAssert(() =>
                {
                    func = cluster2.Metadata.GetFunction("ks_udf", "stringify", new[] { "int" });
                    Assert.NotNull(func);
                    Assert.AreEqual("return Integer.toString(i) + \"hello\";", func.Body);
                }, 100, 100);
            }
            else
            {
                Task.Delay(2000).GetAwaiter().GetResult();
                func = cluster2.Metadata.KeyspacesSnapshot
                               .Single(kvp => kvp.Key == "ks_udf")
                               .Value
                               .GetFunction("stringify", new[] { "int" });
                Assert.IsNotNull(func);
                Assert.AreEqual("return Integer.toString(i);", func.Body); // event wasnt processed
            }
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Retrieve_Metadata_Of_Aggregate(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var aggregate = ks.GetAggregate("sum", new[] { "bigint" });
            Assert.NotNull(aggregate);
            Assert.AreEqual("sum", aggregate.Name);
            Assert.AreEqual("ks_udf", aggregate.KeyspaceName);
            Assert.AreEqual(1, aggregate.ArgumentTypes.Length);
            CollectionAssert.AreEqual(new[] { "bigint" }, aggregate.Signature);
            Assert.AreEqual(ColumnTypeCode.Bigint, aggregate.ArgumentTypes[0].TypeCode);
            Assert.AreEqual(ColumnTypeCode.Bigint, aggregate.ReturnType.TypeCode);
            Assert.AreEqual(ColumnTypeCode.Bigint, aggregate.StateType.TypeCode);
            Assert.AreEqual("2", aggregate.InitialCondition);
            Assert.AreEqual("plus", aggregate.StateFunction);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Return_Null_When_Not_Found(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.Null(ks.GetAggregate("aggr_does_not_exists", new[] { "bigint" }));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Be_Case_Sensitive(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.NotNull(ks.GetAggregate("sum", new[] { "bigint" }));
            Assert.Null(ks.GetAggregate("SUM", new[] { "bigint" }));
            Assert.Null(ks.GetAggregate("sum", new[] { "BIGINT" }));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Cache_The_Metadata(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.AreEqual(ks.GetAggregate("sum", new[] { "int" }), ks.GetAggregate("sum", new[] { "int" }));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Return_Most_Up_To_Date_Metadata_Via_Events(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var session = cluster.Connect("ks_udf");
            var cluster2 = GetCluster(metadataSync);
            var session2 = cluster2.Connect("ks_udf");
            session.Execute("CREATE OR REPLACE AGGREGATE ks_udf.sum2(int) SFUNC plus STYPE int INITCOND 0");
            cluster2.RefreshSchema("ks_udf");
            Task.Delay(500).GetAwaiter().GetResult(); // wait for events to be processed
            var _ = cluster2.Metadata.KeyspacesSnapshot // cache
                            .Single(kvp => kvp.Key == "ks_udf")
                            .Value
                            .GetAggregate("sum2", new[] { "int" });
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var aggregate = cluster.Metadata.GetAggregate("ks_udf", "sum2", new[] {"int"});
            Assert.AreEqual("0", aggregate.InitialCondition);
            session.Execute("CREATE OR REPLACE AGGREGATE ks_udf.sum2(int) SFUNC plus STYPE int INITCOND 200");
            TestUtils.WaitForSchemaAgreement(cluster);
            if (metadataSync)
            {
                TestHelper.RetryAssert(() =>
                {
                    aggregate = cluster.Metadata.GetAggregate("ks_udf", "sum2", new[] { "int" });
                    Assert.NotNull(aggregate);
                    Assert.AreEqual("200", aggregate.InitialCondition);
                }, 100, 100);
            }
            else
            {
                Task.Delay(2000).GetAwaiter().GetResult();
                aggregate = cluster2.Metadata.KeyspacesSnapshot
                                    .Single(kvp => kvp.Key == "ks_udf")
                                    .Value
                                    .GetAggregate("sum2", new[] { "int" });
                Assert.IsNotNull(aggregate);
                Assert.AreEqual("0", aggregate.InitialCondition); // event wasnt processed
            }
        }
    }
}
