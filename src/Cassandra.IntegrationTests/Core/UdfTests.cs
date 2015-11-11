using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class UdfTests : TestGlobals
    {
        private ITestCluster _testCluster;
        private readonly List<ICluster> _clusters = new List<ICluster>();

        private ICluster GetCluster()
        {
            var cluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build();
            _clusters.Add(cluster);
            return cluster;
        }

        [TestFixtureSetUp]
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

        [Test, TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Retrieve_Metadata_Of_Cql_Function()
        {
            var cluster = GetCluster();
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = ks.GetFunction("plus", new [] {"int", "int"});
            //it is the same as retrieving from Metadata, it gets cached
            Assert.AreEqual(func, cluster.Metadata.GetFunction("ks_udf", "plus", new [] {"int", "int"}));
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

        [Test, TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Retrieve_Metadata_Of_Cql_Function_Without_Parameters()
        {
            var ks = GetCluster().Metadata.GetKeyspace("ks_udf");
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

        [Test, TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Be_Case_Sensitive()
        {
            var cluster = GetCluster();
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.NotNull(ks.GetFunction("plus", new[] { "bigint", "bigint" }));
            Assert.Null(ks.GetFunction("PLUS", new[] { "bigint", "bigint" }));
            Assert.Null(ks.GetFunction("plus", new[] { "BIGINT", "bigint" }));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Null_When_Not_Found()
        {
            var ks = GetCluster().Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = ks.GetFunction("func_does_not_exists", new string[0]);
            Assert.Null(func);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Null_When_Not_Found_By_Signature()
        {
            var ks = GetCluster().Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = ks.GetFunction("plus", new[] { "text", "text" });
            Assert.Null(func);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Cache_The_Metadata()
        {
            var ks = GetCluster().Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.AreEqual(ks.GetFunction("plus", new[] { "text", "text" }), ks.GetFunction("plus", new[] { "text", "text" }));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Most_Up_To_Date_Metadata_Via_Events()
        {
            var cluster = GetCluster();
            var session = cluster.Connect("ks_udf");
            session.Execute("CREATE FUNCTION stringify(i int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(i);'");
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var func = cluster.Metadata.GetFunction("ks_udf", "stringify", new[] { "int" });
            Assert.NotNull(func);
            Assert.AreEqual("return Integer.toString(i);", func.Body);
            session.Execute("CREATE OR REPLACE FUNCTION stringify(i int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(i) + \"hello\";'");
            Thread.Sleep(10000);
            func = cluster.Metadata.GetFunction("ks_udf", "stringify", new[] { "int" });
            Assert.NotNull(func);
            Assert.AreEqual("return Integer.toString(i) + \"hello\";", func.Body);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Retrieve_Metadata_Of_Aggregate()
        {
            var cluster = GetCluster();
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

        [Test, TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Return_Null_When_Not_Found()
        {
            var cluster = GetCluster();
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.Null(ks.GetAggregate("aggr_does_not_exists", new[] { "bigint" }));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Be_Case_Sensitive()
        {
            var cluster = GetCluster();
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.NotNull(ks.GetAggregate("sum", new[] { "bigint" }));
            Assert.Null(ks.GetAggregate("SUM", new[] { "bigint" }));
            Assert.Null(ks.GetAggregate("sum", new[] { "BIGINT" }));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Cache_The_Metadata()
        {
            var ks = GetCluster().Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            Assert.AreEqual(ks.GetAggregate("sum", new[] { "int" }), ks.GetAggregate("sum", new[] { "int" }));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Return_Most_Up_To_Date_Metadata_Via_Events()
        {
            var cluster = GetCluster();
            var session = cluster.Connect("ks_udf");
            session.Execute("CREATE AGGREGATE ks_udf.sum2(int) SFUNC plus STYPE int INITCOND 0");
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.NotNull(ks);
            var aggregate = cluster.Metadata.GetAggregate("ks_udf", "sum2", new[] {"int"});
            Assert.AreEqual("0", aggregate.InitialCondition);
            session.Execute("CREATE OR REPLACE AGGREGATE ks_udf.sum2(int) SFUNC plus STYPE int INITCOND 200");
            Thread.Sleep(5000);
            aggregate = cluster.Metadata.GetAggregate("ks_udf", "sum2", new[] { "int" });
            Assert.AreEqual("200", aggregate.InitialCondition);
        }
    }
}
