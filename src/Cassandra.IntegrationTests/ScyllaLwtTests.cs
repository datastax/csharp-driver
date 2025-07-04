using System;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [TestFixture]
    public class ScyllaLwtTest : TestGlobals
    {
        private ITestCluster _realCluster;

        [TearDown]
        public void TestTearDown()
        {
            TestClusterManager.TryRemove();
            _realCluster = null;
        }

        [Test]
        public void Scylla_Should_Recognize_Bound_LWT_Query()
        {
            _realCluster = TestClusterManager.CreateNew();
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var _session = cluster.Connect();

            _session.Execute("DROP KEYSPACE IF EXISTS lwt_test");
            _session.Execute($"CREATE KEYSPACE lwt_test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}}");
            _session.Execute("CREATE TABLE IF NOT EXISTS lwt_test.bound_statement_test (a int PRIMARY KEY, b int)");

            // Prepare a non-LWT statement
            var statementNonLWT = _session.Prepare("UPDATE lwt_test.bound_statement_test SET b = ? WHERE a = ?");
            // Prepare an LWT statement
            var statementLWT = _session.Prepare("UPDATE lwt_test.bound_statement_test SET b = ? WHERE a = ? IF b = ?");

            var boundNonLWT = statementNonLWT.Bind(3, 1);
            var boundLWT = statementLWT.Bind(3, 1, 5);

            // Check LWT detection
            Assert.False(boundNonLWT.IsLwt(), "Non-LWT statement should not be detected as LWT");
            Assert.True(boundLWT.IsLwt(), "LWT statement should be detected as LWT");
        }

        [Test]
        public void Scylla_Should_Recognize_Prepared_LWT_Query()
        {
            _realCluster = TestClusterManager.CreateNew();
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var _session = cluster.Connect();

            _session.Execute("DROP KEYSPACE IF EXISTS lwt_test");
            _session.Execute($"CREATE KEYSPACE lwt_test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}}");
            _session.Execute("CREATE TABLE IF NOT EXISTS lwt_test.prepared_statement_test (a int PRIMARY KEY, b int)");

            // Prepare a non-LWT statement
            var statementNonLWT = _session.Prepare("UPDATE lwt_test.prepared_statement_test SET b = 3 WHERE a = 1");
            // Prepare an LWT statement
            var statementLWT = _session.Prepare("UPDATE lwt_test.prepared_statement_test SET b = 3 WHERE a = 1 IF b = 5");

            // Check LWT detection
            Assert.False(statementNonLWT.IsLwt, "Non-LWT statement should not be detected as LWT");
            Assert.True(statementLWT.IsLwt, "LWT statement should be detected as LWT");
        }

        [Test]
        public void Should_Use_Only_One_Node_When_LWT_Detected()
        {
            _realCluster = TestClusterManager.CreateNew(3);
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var session = cluster.Connect();

            // Create keyspace and table
            session.Execute("DROP KEYSPACE IF EXISTS lwt_test");
            session.Execute($"CREATE KEYSPACE lwt_test WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}");
            session.Execute($"USE lwt_test");
            session.Execute("CREATE TABLE foo (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            int pk = 1234;
            var routingKey = new RoutingKey();
            routingKey.RawRoutingKey = new byte[] { 0, 0, 0x04, 0xd2 }; // pk=1234 as bytes

            // Get the replicas for this routing key
            var replicas = cluster.GetReplicas("lwt_test", routingKey.RawRoutingKey);
            var owner = replicas.First();

            var statement = session.Prepare("INSERT INTO foo (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS");
            Assert.True(statement.IsLwt, "Statement should be detected as LWT");

            var coordinatorEndpoints = new HashSet<System.Net.IPEndPoint>();
            for (int i = 0; i < 30; i++)
            {
                var result = session.Execute(statement.Bind(pk, i, 123));
                var coordinatorEndpoint = result.Info.QueriedHost;
                coordinatorEndpoints.Add(coordinatorEndpoint);
            }

            // For LWT queries, all should go to the same coordinator (the owner)
            Assert.AreEqual(1, coordinatorEndpoints.Count, "LWT queries should use only one coordinator");
            Assert.That(coordinatorEndpoints.Contains(owner.Host.Address), "LWT queries should use the replica owner as coordinator");
        }

        [Test]
        public void Should_Not_Use_Only_One_Node_When_Non_LWT()
        {
            // Sanity check for the previous test - non-LWT queries should not always be sent to same node
            _realCluster = TestClusterManager.CreateNew(3);
            var cluster = ClusterBuilder()
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                          .AddContactPoint(_realCluster.InitialContactPoint)
                          .Build();
            var session = cluster.Connect();

            // Create keyspace and table
            session.Execute("DROP KEYSPACE IF EXISTS lwt_test");
            session.Execute($"CREATE KEYSPACE lwt_test WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}");
            session.Execute($"USE lwt_test");
            session.Execute("CREATE TABLE foo (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            int pk = 1234;
            var statement = session.Prepare("INSERT INTO foo (pk, ck, v) VALUES (?, ?, ?)");
            Assert.False(statement.IsLwt, "Statement should not be detected as LWT");

            var coordinatorEndpoints = new HashSet<System.Net.IPEndPoint>();
            for (int i = 0; i < 30; i++)
            {
                var result = session.Execute(statement.Bind(pk, i, 123));
                var coordinatorEndpoint = result.Info.QueriedHost;
                coordinatorEndpoints.Add(coordinatorEndpoint);
            }

            // Because keyspace RF == 3, non-LWT queries should distribute across all nodes
            Assert.AreEqual(3, coordinatorEndpoints.Count, "Non-LWT queries should use all available coordinators");
        }
    }

}
