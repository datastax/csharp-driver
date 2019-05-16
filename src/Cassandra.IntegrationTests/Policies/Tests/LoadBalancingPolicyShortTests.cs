using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Serialization;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category("short")]
    public class LoadBalancingPolicyShortTests : TestGlobals
    {
        /// <summary>
        /// Validate that two sessions connected to the same DC use separate Policy instances
        /// </summary>
        [Test]
        public void TwoSessionsConnectedToSameDcUseSeparatePolicyInstances()
        {
            var builder = Cluster.Builder();
            var testCluster = TestClusterManager.CreateNew();

            using (var cluster1 = builder.WithConnectionString(String.Format("Contact Points={0}1", testCluster.ClusterIpPrefix)).Build())
            using (var cluster2 = builder.WithConnectionString(String.Format("Contact Points={0}2", testCluster.ClusterIpPrefix)).Build())
            {
                var session1 = (Session) cluster1.Connect();
                var session2 = (Session) cluster2.Connect();
                Assert.AreNotSame(session1.Policies.LoadBalancingPolicy, session2.Policies.LoadBalancingPolicy, "Load balancing policy instances should be different");
                Assert.AreNotSame(session1.Policies.ReconnectionPolicy, session2.Policies.ReconnectionPolicy, "Reconnection policy instances should be different");
                Assert.AreNotSame(session1.Policies.RetryPolicy, session2.Policies.RetryPolicy, "Retry policy instances should be different");
            }
        }
        /// <summary>
        /// Validate that no hops occur when inserting into a single partition 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void TokenAware_TargetPartition_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.CreateNew(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(testCluster.Session, 1);
            var traces = new List<QueryTrace>();
            for (var i = -10; i < 10; i++)
            {
                var partitionKey = BitConverter.GetBytes(i).Reverse().ToArray();
                var statement = new SimpleStatement(String.Format("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                    .SetRoutingKey(new RoutingKey() { RawRoutingKey = partitionKey })
                    .EnableTracing();
                var rs = testCluster.Session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        /// <summary>
        /// Validate that no hops occur when inserting GUID values into the key 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void TokenAware_Guid_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.CreateNew(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            string uniqueTableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(session);
            session.Execute(String.Format("CREATE TABLE {0} (k uuid PRIMARY KEY, i int)", uniqueTableName));
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var key = Guid.NewGuid();
                var statement = new SimpleStatement(String.Format("INSERT INTO " + uniqueTableName + " (k, i) VALUES ({0}, {1})", key, i))
                    .SetRoutingKey(
                        new RoutingKey() { RawRoutingKey = TypeSerializer.GuidShuffle(key.ToByteArray()) })
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        /// <summary>
        /// Validate that no hops occur when inserting into a composite key 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void TokenAware_Composite_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.CreateNew(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            policyTestTools.CreateSchema(session);
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            session.Execute(String.Format("CREATE TABLE {0} (k1 text, k2 int, i int, PRIMARY KEY ((k1, k2)))", policyTestTools.TableName));
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var statement = new SimpleStatement(String.Format("INSERT INTO " + policyTestTools.TableName + " (k1, k2, i) VALUES ('{0}', {0}, {0})", i))
                    .SetRoutingKey(
                        new RoutingKey() { RawRoutingKey = Encoding.UTF8.GetBytes(i.ToString()) },
                        new RoutingKey() { RawRoutingKey = BitConverter.GetBytes(i).Reverse().ToArray() })
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        /// <summary>
        /// Validate that no hops occur when inserting into a composite key with a prepared statement
        /// @test_category load_balancing:token_aware
        /// </summary>
        [Test]
        public void TokenAware_Prepared_Composite_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.CreateNew(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            policyTestTools.CreateSchema(session);
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            session.Execute(String.Format("CREATE TABLE {0} (k1 text, k2 int, i int, PRIMARY KEY ((k1, k2)))", policyTestTools.TableName));
            Thread.Sleep(1000);
            var ps = session.Prepare(String.Format("INSERT INTO {0} (k1, k2, i) VALUES (?, ?, ?)", policyTestTools.TableName));
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var statement = ps.Bind(i.ToString(), i, i).EnableTracing();
                //Routing key is calculated by the driver
                Assert.NotNull(statement.RoutingKey);
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        /// <summary>
        /// Validate that no hops occur when inserting string values via a prepared statement 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// @test_category prepared_statements
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TokenAware_BindString_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.CreateNew(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            policyTestTools.CreateSchema(session);
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            session.Execute(String.Format("CREATE TABLE {0} (k text PRIMARY KEY, i int)", policyTestTools.TableName));
            var traces = new List<QueryTrace>();
            string key = "value";
            for (var i = 100; i < 140; i++)
            {
                key += (char)i;
                var partitionKey = Encoding.UTF8.GetBytes(key);
                var statement = new SimpleStatement("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES (?, ?)", key, i)
                    .SetRoutingKey(new RoutingKey() { RawRoutingKey = partitionKey })
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        /// <summary>
        /// Validate that no hops occur when inserting int values via a prepared statement 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// @test_category prepared_statements
        /// </summary>
        [Test]
        public void TokenAware_BindInt_NoHops()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.CreateNew(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            // Test
            var session = testCluster.Session;
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(session, 1);
            var traces = new List<QueryTrace>();
            var pstmt = session.Prepare("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES (?, ?)");
            for (var i = (int)short.MinValue; i < short.MinValue + 40; i++)
            {
                var statement = pstmt
                    .Bind(i, i)
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        /// <summary>
        /// Validate that hops occur when the wrong partition is targeted 
        /// 
        /// @test_category load_balancing:dc_aware,round_robin
        /// @test_category replication_strategy
        /// </summary>
        [Test]
        public void TokenAware_TargetWrongPartition_HopsOccur()
        {
            // Setup
            PolicyTestTools policyTestTools = new PolicyTestTools();
            ITestCluster testCluster = TestClusterManager.CreateNew(3);
            testCluster.Builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            testCluster.InitClient();

            var session = testCluster.Session;
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            policyTestTools.CreateSchema(session, 1);
            var traces = new List<QueryTrace>();
            for (var i = 1; i < 10; i++)
            {
                //The partition key is wrongly calculated
                var statement = new SimpleStatement(String.Format("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                    .SetRoutingKey(new RoutingKey() { RawRoutingKey = new byte[] { 0, 0, 0, 0 } })
                    .EnableTracing();
                var rs = session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there were hops
            var hopsPerQuery = traces.Select(t => t.Events.Any(e => e.Source.ToString() == t.Coordinator.ToString()));
            Assert.True(hopsPerQuery.Any(v => v));
        }

        /// <summary>
        /// Token Aware with vnodes test
        /// </summary>
        [Test, TestCase(true), TestCase(false)]
        public void TokenAware_VNodes_Test(bool metadataSync)
        {
            var testCluster = TestClusterManager.CreateNew(3, new TestClusterOptions { UseVNodes = true });
            var cluster = Cluster.Builder()
                                 .AddContactPoint(testCluster.InitialContactPoint)
                                 .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync))
                                 .Build();
            try
            {
                var session = cluster.Connect();
                Assert.AreEqual(256, cluster.AllHosts().First().Tokens.Count());
                session.Execute("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");
                session.ChangeKeyspace("ks1");
                session.Execute("CREATE TABLE tbl1 (id uuid primary key)");
                var ps = session.Prepare("INSERT INTO tbl1 (id) VALUES (?)");
                var traces = new List<QueryTrace>();
                for (var i = 0; i < 10; i++)
                {
                    var id = Guid.NewGuid();
                    var bound = ps
                        .Bind(id)
                        .EnableTracing();
                    var rs = session.Execute(bound);
                    traces.Add(rs.Info.QueryTrace);
                }
                
                //Check that there weren't any hops
                foreach (var t in traces)
                {
                    //The coordinator must be the only one executing the query
                    Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
                }
            }
            finally
            {
                cluster.Dispose();
                testCluster.Remove();
            }
        }

        [Test, TestCase(true), TestCase(false)]
        public void Token_Aware_Uses_Keyspace_From_Statement_To_Determine_Replication(bool metadataSync)
        {
            var testCluster = TestClusterManager.CreateNew(3, new TestClusterOptions { UseVNodes = true });
            var cluster = Cluster.Builder()
                                 .AddContactPoint(testCluster.InitialContactPoint)
                                 .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync))
                                 .Build();
            try
            {
                // Connect without a keyspace
                var session = cluster.Connect();
                session.Execute("CREATE KEYSPACE ks_tap_stmt_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2}");
                session.Execute("CREATE TABLE ks_tap_stmt_ks.tbl1 (id uuid primary key)");
                var ps = session.Prepare("INSERT INTO ks_tap_stmt_ks.tbl1 (id) VALUES (?)");
                var id = Guid.NewGuid();
                var coordinators = new HashSet<IPEndPoint>();
                for (var i = 0; i < 20; i++)
                {
                    var rs = session.Execute(ps.Bind(id));
                    coordinators.Add(rs.Info.QueriedHost);
                }
                // There should be exactly 2 different coordinators for a given token
                Assert.AreEqual(metadataSync ? 2 : 1, coordinators.Count);

                // Manually calculate the routing key
                var routingKey = Serializer.Default.Serialize(id);
                // Get the replicas
                var replicas = cluster.GetReplicas("ks_tap_stmt_ks", routingKey);
                Assert.AreEqual(metadataSync ? 2 : 1, replicas.Count);
                CollectionAssert.AreEquivalent(replicas.Select(h => h.Address), coordinators);
            }
            finally
            {
                cluster.Dispose();
                testCluster.Remove();
            }
        }
    }
}
