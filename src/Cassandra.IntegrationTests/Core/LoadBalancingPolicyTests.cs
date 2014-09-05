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
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;
using System.Collections.Generic;
using System.Net;
using System.Diagnostics;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class LoadBalancingPolicyTests : PolicyTestTools
    {
        protected TraceLevel _originalTraceLevel;

        protected virtual string IpPrefix
        {
            get
            {
                return "127.0.0.";
            }
        }

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            _originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Diagnostics.CassandraTraceSwitch.Level = _originalTraceLevel;
        }

        [Test]
        public void RoundRobinTestCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            try
            {
                createSchema(clusterInfo.Session);
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 6);
                assertQueried(IpPrefix + "2", 6);

                resetCoordinators();
                TestUtils.CcmBootstrapNode(clusterInfo, 3);
                TestUtils.waitFor(IpPrefix + "3", clusterInfo.Cluster, 60);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 4);
                assertQueried(IpPrefix + "2", 4);
                assertQueried(IpPrefix + "3", 4);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 1);
                TestUtils.waitForDecommission(IpPrefix + "1", clusterInfo.Cluster, 60);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "2", 6);
                assertQueried(IpPrefix + "3", 6);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void PoliciesAreDifferentInstancesWhenDefault()
        {

            var builder = Cluster.Builder();
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);
            try
            {
                using (var cluster1 = builder.WithConnectionString(String.Format("Contact Points={0}1", IpPrefix)).Build())
                using (var cluster2 = builder.WithConnectionString(String.Format("Contact Points={0}2", IpPrefix)).Build())
                {
                    using (var session1 = (Session)cluster1.Connect())
                    using (var session2 = (Session)cluster2.Connect())
                    {
                        Assert.True(!Object.ReferenceEquals(session1.Policies.LoadBalancingPolicy, session2.Policies.LoadBalancingPolicy), "Load balancing policy instances should be different");
                        Assert.True(!Object.ReferenceEquals(session1.Policies.ReconnectionPolicy, session2.Policies.ReconnectionPolicy), "Reconnection policy instances should be different");
                        Assert.True(!Object.ReferenceEquals(session1.Policies.RetryPolicy, session2.Policies.RetryPolicy), "Retry policy instances should be different");
                    }
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void roundRobinWith2DCsTestCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);
            try
            {
                createSchema(clusterInfo.Session);
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 3);
                assertQueried(IpPrefix + "2", 3);
                assertQueried(IpPrefix + "3", 3);
                assertQueried(IpPrefix + "4", 3);

                resetCoordinators();
                TestUtils.CcmBootstrapNode(clusterInfo, 5, "dc2");
                TestUtils.CcmDecommissionNode(clusterInfo, 1);
                TestUtils.waitFor(IpPrefix + "5", clusterInfo.Cluster, 20);
                TestUtils.waitForDecommission(IpPrefix + "1", clusterInfo.Cluster, 20);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 3);
                assertQueried(IpPrefix + "3", 3);
                assertQueried(IpPrefix + "4", 3);
                assertQueried(IpPrefix + "5", 3);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void DCAwareRoundRobinTestCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);
            try
            {
            createMultiDCSchema(clusterInfo.Session);
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 6);
                assertQueried(IpPrefix + "4", 6);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void forceStopCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            builder.WithQueryTimeout(10000);
            var clusterInfo = TestUtils.CcmSetup(4, builder, null);
            try
            {
                createSchema(clusterInfo.Session);
                init(clusterInfo, 12);
                query(clusterInfo, 12);
                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 1);
                TestUtils.CcmStopForce(clusterInfo, 2);
                TestUtils.waitForDown(IpPrefix + "1", clusterInfo.Cluster, 40);
                TestUtils.waitForDown(IpPrefix + "2", clusterInfo.Cluster, 40);

                query(clusterInfo, 12);

                TestUtils.CcmStopForce(clusterInfo, 3);
                TestUtils.CcmStopForce(clusterInfo, 4);
                TestUtils.waitForDown(IpPrefix + "3", clusterInfo.Cluster, 40);
                TestUtils.waitForDown(IpPrefix + "4", clusterInfo.Cluster, 40);

                try
                {
                    query(clusterInfo, 12);
                    Assert.Fail("It should throw an exception");
                }
                catch (NoHostAvailableException)
                {
                    // No more nodes so ...
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void dcAwareRoundRobinTestWithOneRemoteHostCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2", 1));
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);
            try
            {
                createMultiDCSchema(clusterInfo.Session);
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 6);
                assertQueried(IpPrefix + "4", 6);
                assertQueried(IpPrefix + "5", 0);

                resetCoordinators();
                TestUtils.CcmBootstrapNode(clusterInfo, 5, "dc3");
                TestUtils.waitFor(IpPrefix + "5", clusterInfo.Cluster, 60);


                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 6);
                assertQueried(IpPrefix + "4", 6);
                assertQueried(IpPrefix + "5", 0);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 3);
                TestUtils.CcmDecommissionNode(clusterInfo, 4);
                TestUtils.waitForDecommission(IpPrefix + "3", clusterInfo.Cluster, 20);
                TestUtils.waitForDecommission(IpPrefix + "4", clusterInfo.Cluster, 20);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 6);
                assertQueried(IpPrefix + "3", 0);
                assertQueried(IpPrefix + "4", 0);
                assertQueried(IpPrefix + "5", 6);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 5);
                TestUtils.waitForDecommission(IpPrefix + "5", clusterInfo.Cluster, 20);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);
                assertQueried(IpPrefix + "3", 0);
                assertQueried(IpPrefix + "4", 0);
                assertQueried(IpPrefix + "5", 0);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 2);
                TestUtils.waitForDecommission(IpPrefix + "2", clusterInfo.Cluster, 20);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 12);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 0);
                assertQueried(IpPrefix + "4", 0);
                assertQueried(IpPrefix + "5", 0);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 1);
                TestUtils.waitForDown(IpPrefix + "2", clusterInfo.Cluster, 20);

                try
                {
                    query(clusterInfo, 12);
                    Assert.Fail();
                }
                catch (NoHostAvailableException)
                {
                    // No more nodes so ...
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TokenAwareTargetsPartitionNoHopsQueryTest()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            try
            {
                var session = clusterInfo.Session;
                createSchema(session, 1);
                var traces = new List<QueryTrace>();
                for (var i = -10; i < 10; i++)
                {
                    var partitionKey = BitConverter.GetBytes(i).Reverse().ToArray();
                    var statement = new SimpleStatement(String.Format("INSERT INTO test (k, i) VALUES ({0}, {0})", i))
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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TokenAwareTargetsPartitionBindStringNoHopsQueryTest()
        {
            if (Options.Default.CassandraVersion < new Version(2, 0))
            {
                Assert.Ignore("Test suitable to be run against Cassandra 2.0 or above");
            }
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(4, builder);
            try
            {
                var session = clusterInfo.Session;
                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, TestUtils.SIMPLE_KEYSPACE, 1)));
                session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
                session.WaitForSchemaAgreement(session.Execute(String.Format("CREATE TABLE {0} (k text PRIMARY KEY, i int)", TABLE)));
                var traces = new List<QueryTrace>();
                string key = "value";
                for (var i = 100; i < 140; i++)
                {
                    key += (char) i;
                    var partitionKey = Encoding.UTF8.GetBytes(key);
                    var statement = new SimpleStatement("INSERT INTO test (k, i) VALUES (?, ?)")
                        .Bind(key, i)
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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TokenAwareTargetsPartitionGuidNoHopsQueryTest()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(4, builder);
            try
            {
                var session = clusterInfo.Session;
                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, TestUtils.SIMPLE_KEYSPACE, 1));
                Thread.Sleep(3000);
                session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
                Thread.Sleep(3000);
                session.Execute(String.Format("CREATE TABLE {0} (k uuid PRIMARY KEY, i int)", TABLE));
                var traces = new List<QueryTrace>();
                for (var i = 0; i < 10; i++)
                {
                    var key = Guid.NewGuid();
                    var statement = new SimpleStatement(String.Format("INSERT INTO test (k, i) VALUES ({0}, {1})", key, i))
                        .SetRoutingKey(
                            new RoutingKey() { RawRoutingKey = TypeCodec.GuidShuffle(key.ToByteArray()) })
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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TokenAwareTargetsPartitionCompositeNoHopsQueryTest()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(4, builder);
            try
            {
                var session = clusterInfo.Session;
                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, TestUtils.SIMPLE_KEYSPACE, 1)));
                session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
                session.WaitForSchemaAgreement(session.Execute(String.Format("CREATE TABLE {0} (k1 text, k2 int, i int, PRIMARY KEY ((k1, k2)))", TABLE)));
                var traces = new List<QueryTrace>();
                for (var i = 0; i < 10; i++)
                {
                    var statement = new SimpleStatement(String.Format("INSERT INTO test (k1, k2, i) VALUES ('{0}', {0}, {0})", i))
                        .SetRoutingKey(
                            new RoutingKey() { RawRoutingKey = Encoding.UTF8.GetBytes(i.ToString())},
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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TokenAwareTargetsPartitionNoHopsPrepareTest()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            try
            {
                var session = clusterInfo.Session;
                createSchema(session, 1);
                var traces = new List<QueryTrace>();
                var pstmt = session.Prepare("INSERT INTO test (k, i) VALUES (?, ?)");
                for (var i = (int)short.MinValue; i < short.MinValue + 40; i++)
                {
                    var partitionKey = BitConverter.GetBytes(i).Reverse().ToArray();
                    var statement = pstmt
                        .Bind(i, i)
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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void TokenAwareTargetsWrongPartitionHopsTest()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            try
            {
                var session = clusterInfo.Session;
                createSchema(session, 1);
                var traces = new List<QueryTrace>();
                for (var i = 1; i < 10; i++)
                {
                    var partitionKey = BitConverter.GetBytes(i).Reverse().ToArray();
                    //The partition key is wrongly calculated
                    var statement = new SimpleStatement(String.Format("INSERT INTO test (k, i) VALUES ({0}, {0})", i))
                        .SetRoutingKey(new RoutingKey() { RawRoutingKey = new byte[] {0, 0, 0, 0} })
                        .EnableTracing();
                    var rs = session.Execute(statement);
                    traces.Add(rs.Info.QueryTrace);
                }
                //Check that there were hops
                var hopsPerQuery = traces.Select(t => t.Events.Any(e => e.Source.ToString() == t.Coordinator.ToString()));
                Assert.True(hopsPerQuery.Any(v => v));
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void DcAwareNeverHitsRemoteDc()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc1"));
            //create a cluster with 2 nodes on dc1 and another 2 nodes on dc2
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);
            try
            {
                var hosts = clusterInfo.Cluster.AllHosts();
                //2 hosts in each datacenter
                Assert.AreEqual(2, hosts.Count(h => h.Datacenter == "dc1"));
                Assert.AreEqual(2, hosts.Count(h => h.Datacenter == "dc2"));
                var session = clusterInfo.Session;
                var queriedHosts = new List<IPEndPoint>();
                for (var i = 0; i < 50; i++)
                {
                    var rs = session.Execute("SELECT * FROM system.schema_columnfamilies");
                    queriedHosts.Add(rs.Info.QueriedHost);
                }
                //Only the ones in the local
                Assert.True(queriedHosts.Contains(new IPEndPoint(IPAddress.Parse(IpPrefix + "1"), ProtocolOptions.DefaultPort)), "Only hosts from the local DC should be queried 1");
                Assert.True(queriedHosts.Contains(new IPEndPoint(IPAddress.Parse(IpPrefix + "2"), ProtocolOptions.DefaultPort)), "Only hosts from the local DC should be queried 2");
                Assert.False(queriedHosts.Contains(new IPEndPoint(IPAddress.Parse(IpPrefix + "3"), ProtocolOptions.DefaultPort)), "Only hosts from the local DC should be queried 3");
                Assert.False(queriedHosts.Contains(new IPEndPoint(IPAddress.Parse(IpPrefix + "4"), ProtocolOptions.DefaultPort)), "Only hosts from the local DC should be queried 4");
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void tokenAwareTestCCM()
        {
            tokenAwareTest(false);
        }

        [Test]
        public void tokenAwarePreparedTestCCM()
        {
            tokenAwareTest(true);
        }

        public void tokenAwareTest(bool usePrepared)
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            try
            {
                createSchema(clusterInfo.Session);
                //clusterInfo.Cluster.RefreshSchema();
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                // Not the best test ever, we should use OPP and check we do it the
                // right nodes. But since M3P is hard-coded for now, let just check
                // we just hit only one node.
                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);

                resetCoordinators();
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 2);
                TestUtils.waitForDown(IpPrefix + "2", clusterInfo.Cluster, 60);

                try
                {
                    query(clusterInfo, 12, usePrepared);
                    Assert.Fail();
                }
                catch (UnavailableException)
                {
                }
                catch (ReadTimeoutException)
                {
                }

                resetCoordinators();
                TestUtils.CcmStart(clusterInfo, 2);
                TestUtils.waitFor(IpPrefix + "2", clusterInfo.Cluster, 60);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 2);
                TestUtils.waitForDecommission(IpPrefix + "2", clusterInfo.Cluster, 60);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 12);
                assertQueried(IpPrefix + "2", 0);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void tokenAwareWithRF2TestCCM()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            try
            {
                createSchema(clusterInfo.Session, 2);

                init(clusterInfo, 12);
                query(clusterInfo, 12);

                // Not the best test ever, we should use OPP and check we do it the
                // right nodes. But since M3P is hard-coded for now, let just check
                // we just hit only one node.
                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);
                assertQueried(IpPrefix + "3", 0);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}
