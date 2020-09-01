//
//      Copyright (C) DataStax Inc.
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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
#if NETCOREAPP
using System.Net.Http;
#endif
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.DataStax.Cloud;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestAttributes;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using Cassandra.Tests;
using Cassandra.Tests.TestAttributes;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.DataStax.Cloud
{
    [SniEnabledOnly]
    [CloudSupported(Supported = true)]
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.Cloud), Category(TestCategory.RealClusterLong)]
    public class CloudIntegrationTests : SharedCloudClusterTest
    {
        [Test]
        public void Should_ThrowNoHostAvailable_When_MetadataServiceIsUnreachable()
        {
            var ex = Assert.ThrowsAsync<NoHostAvailableException>(
                () => CreateSessionAsync(
                    "creds-v1-unreachable.zip", 
                    retries: 1, 
                    act: 
                    b => b
                         .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(5000).SetConnectTimeoutMillis(10000))
                         .WithQueryTimeout(5000)));
            Assert.IsTrue(ex.Message.Contains("https://192.0.2.255:30443/metadata"), ex.Message);
            Assert.IsTrue(ex.Message.Contains("There was an error fetching the metadata information"), ex.Message);
            Assert.IsTrue(ex.Message.Contains("Please make sure your cluster is not parked or terminated."), ex.Message);
        }

        [Test]
        public async Task Should_ContinueQuerying_When_ANodeGoesDown()
        {
            var session = await CreateSessionAsync(act: builder =>
                builder.WithPoolingOptions(
                           new PoolingOptions().SetHeartBeatInterval(50))
                       .WithReconnectionPolicy(new ConstantReconnectionPolicy(40))
                       .WithQueryOptions(new QueryOptions().SetDefaultIdempotence(true))).ConfigureAwait(false);

            Assert.IsTrue(session.Cluster.AllHosts().All(h => h.IsUp));
            var restarted = true;
            var t = Task.Run(async () =>
            {
                TestCluster.Stop(1);
                await Task.Delay(2000).ConfigureAwait(false);
                TestCluster.Start(1, "--jvm_arg \"-Ddse.product_type=DATASTAX_APOLLO\"");
                await Task.Delay(500).ConfigureAwait(false);
                try
                {
                    TestHelper.RetryAssert(
                        () =>
                        {
                            var dict = Session.Cluster.Metadata.TokenToReplicasMap.GetByKeyspace("system_distributed");
                            Assert.AreEqual(3, dict.First().Value.Count);
                            Assert.AreEqual(3, Session.Cluster.AllHosts().Count);
                            Assert.IsTrue(Session.Cluster.AllHosts().All(h => h.IsUp));
                        },
                        20,
                        500);
                }
                finally
                {
                    Volatile.Write(ref restarted, true);
                }
            });

            var t2 = Task.Run(async () =>
            {
                while (true)
                {
                    if (Volatile.Read(ref restarted))
                    {
                        return;
                    }

                    var tasks = new List<Task>();
                    long counter = 0;
                    foreach (var _ in Enumerable.Range(0, 32))
                    {
                        tasks.Add(Task.Run(async () =>
                        {
                            while (true)
                            {
                                var c = Interlocked.Increment(ref counter);
                                if (c > 1000)
                                {
                                    return;
                                }

                                try
                                {
                                    await session.ExecuteAsync(new SimpleStatement("SELECT key FROM system.local")).ConfigureAwait(false);
                                }
                                catch (QueryTimeoutException){}
                            }
                        }));
                    }

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
            });
            await Task.WhenAll(t, t2).ConfigureAwait(false);
        }

        [Test]
        public void Should_ThrowException_When_BundleDoesNotExist()
        {
            var ex = Assert.Throws<InvalidOperationException>(
                () => ClusterBuilder()
                      .WithCloudSecureConnectionBundle("does-not-exist.zip")
                      .WithCredentials("user1", "user1").Build());
        }
        
        [Test]
        public void Should_FailFast_When_ConfigJsonDoesNotHaveCredentialsAndUserDoesNotProvideCredentials()
        {
            var ex = Assert.Throws<ArgumentException>(() => CreateTemporaryCluster("creds-v1-wo-creds.zip", withCredentials: false));
            Assert.AreEqual(
                ex.Message, 
                "No credentials were provided. When using the secure connection bundle, " +
                "your cluster's credentials must be provided via the Builder.WithCredentials() method.");
        }

        [Test]
        public void Should_SetAuthProvider()
        {
            Assert.IsNotNull(Session.Cluster.Configuration.AuthProvider.GetType());
            Assert.AreEqual(typeof(PlainTextAuthProvider), Session.Cluster.Configuration.AuthProvider.GetType());
            var provider = (PlainTextAuthProvider)Session.Cluster.Configuration.AuthProvider;
            Assert.AreEqual("user1", provider.Username);
        }

        [Test]
        public async Task Should_MatchSystemLocalInformationOfEachNode()
        {
            const int port = 9042;
            var session = await CreateSessionAsync(act: b => b.WithLoadBalancingPolicy(new RoundRobinPolicy())).ConfigureAwait(false);
            var queriedHosts = new HashSet<IPAddress>();
            foreach (var i in Enumerable.Range(0, 3))
            {
                var rs = await session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local")).ConfigureAwait(false);
                var row = rs.First();
                var host = session.Cluster.GetHost(new IPEndPoint(rs.Info.QueriedHost.Address, rs.Info.QueriedHost.Port));
                Assert.IsNotNull(host);
                queriedHosts.Add(rs.Info.QueriedHost.Address);
                Assert.AreEqual(host.HostId, row.GetValue<Guid>("host_id"));
                Assert.AreEqual(host.Address, new IPEndPoint(row.GetValue<IPAddress>("rpc_address"), port));
            }

            Assert.AreEqual(3, queriedHosts.Count);
        }

        [Test]
        public async Task Should_HaveTwoRows_When_QueryingSystemPeers()
        {
            var rs = await Session.ExecuteAsync(new SimpleStatement("select * from system.peers")).ConfigureAwait(false);
            var allRs = rs.ToList();
            Assert.AreEqual(2, allRs.Count);
        }

        [Test]
        public void TokenAware_Prepared_Composite_NoHops()
        {
            // Setup
            var policyTestTools = new PolicyTestTools();

            // Test
            policyTestTools.CreateSchema(Session, 1, forceSchemaAgreement: true);
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            Session.Execute($"CREATE TABLE {policyTestTools.TableName} (k1 text, k2 int, i int, PRIMARY KEY ((k1, k2)))");
            Thread.Sleep(1000);
            TestHelper.RetryAssert(() =>
            {
                Assert.True(Session.Cluster.Metadata.CheckSchemaAgreementAsync().Result);
            }, 500, 150);
            var ps = Session.Prepare($"INSERT INTO {policyTestTools.TableName} (k1, k2, i) VALUES (?, ?, ?)");
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var statement = ps.Bind(i.ToString(), i, i).EnableTracing();
                //Routing key is calculated by the driver
                Assert.NotNull(statement.RoutingKey);
                var rs = Session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        [Test]
        public void TokenAware_TargetWrongPartition_HopsOccur()
        {
            // Setup
            var policyTestTools = new PolicyTestTools { TableName = TestUtils.GetUniqueTableName() };

            policyTestTools.CreateSchema(Session, 1, forceSchemaAgreement: true);
            var traces = new List<QueryTrace>();
            for (var i = 1; i < 10; i++)
            {
                //The partition key is wrongly calculated
                var statement = new SimpleStatement(string.Format("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                                .SetRoutingKey(new RoutingKey() { RawRoutingKey = new byte[] { 0, 0, 0, 0 } })
                                .EnableTracing();
                var rs = Session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there were hops
            var hopsPerQuery = traces.Select(t => t.Events.Any(e => e.Source.ToString() == t.Coordinator.ToString()));
            Assert.True(hopsPerQuery.Any(v => v));
        }

        [Test]
        public void TokenAware_NoKey_HopsOccurAndAllNodesAreChosenAsCoordinators()
        {
            // Setup
            var policyTestTools = new PolicyTestTools { TableName = TestUtils.GetUniqueTableName() };

            policyTestTools.CreateSchema(Session, 1, forceSchemaAgreement: true);
            var traces = new List<QueryTrace>();
            for (var i = 1; i < 10; i++)
            {
                //The partition key is wrongly calculated
                var statement = new SimpleStatement(string.Format("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                                .EnableTracing();
                var rs = Session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there were hops
            var hopsPerQuery = traces.Select(t => t.Events.Any(e => e.Source.ToString() == t.Coordinator.ToString()));
            Assert.True(hopsPerQuery.Any(v => v));
            var tracesPerCoordinator = traces.GroupBy(t => t.Coordinator).ToDictionary(t => t.Key, t => t.Count());
            Assert.AreEqual(3, tracesPerCoordinator.Count);
            Assert.IsTrue(tracesPerCoordinator.All(kvp => kvp.Value == 3));
        }

        [Test]
        public void Should_ThrowSslException_When_ClientCertIsNotProvided()
        {
            var ex = Assert.ThrowsAsync<NoHostAvailableException>(() => CreateSessionAsync("creds-v1-wo-cert.zip", retries: 1));
            AssertIsSslError(ex);
        }

        [Test]
        public void Should_ThrowSslException_When_CaMismatch()
        {
            var ex = Assert.ThrowsAsync<NoHostAvailableException>(() => CreateSessionAsync("creds-v1-invalid-ca.zip", retries: 1));
            AssertCaMismatchSslError(ex);
        }

        [Test]
        public void Should_ParseBundleCorrectly_When_BundlePathIsProvided()
        {
            var scb = new SecureConnectionBundleParser()
                .ParseBundle(
                    Path.Combine(
                        ((CloudCluster)TestCluster).SniHomeDirectory,
                        "certs",
                        "bundles",
                        "creds-v1.zip"));

            Assert.IsNotNull(scb.CaCert);
            Assert.IsNotNull(scb.ClientCert);
            Assert.IsFalse(string.IsNullOrWhiteSpace(scb.Config.CertificatePassword));
            Assert.IsTrue(scb.ClientCert.HasPrivateKey);
            Assert.AreEqual(30443, scb.Config.Port);
            Assert.AreEqual("localhost", scb.Config.Host);
        }

        [Test]
        public async Task Should_UseCorrectConsistencyLevelDefaults_When_Dbaas()
        {
            var session = await CreateSessionAsync(act: b => b
                .WithExecutionProfiles(opt => opt
                    .WithProfile("default", profile =>
                        profile.WithConsistencyLevel(ConsistencyLevel.Any))
                    .WithProfile("profile", profile =>
                        profile.WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
                    .WithDerivedProfile("derived", "profile", profile =>
                        profile.WithConsistencyLevel(ConsistencyLevel.LocalQuorum)))).ConfigureAwait(false);
            Assert.AreEqual(ConsistencyLevel.LocalQuorum, Cluster.Configuration.DefaultRequestOptions.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalQuorum, Cluster.Configuration.QueryOptions.GetConsistencyLevel());
            Assert.AreEqual(ConsistencyLevel.Serial, Cluster.Configuration.DefaultRequestOptions.SerialConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.Serial, Cluster.Configuration.QueryOptions.GetSerialConsistencyLevel());

            var ks = TestUtils.GetUniqueKeyspaceName().ToLower();
            const string createKeyspaceQuery = "CREATE KEYSPACE {0} WITH replication = {{ 'class' : '{1}', {2} }}";
            session.Execute(string.Format(createKeyspaceQuery, ks, "SimpleStrategy", "'replication_factor' : 3"));
            TestHelper.RetryAssert(
                () => Assert.IsTrue(session.Cluster.Metadata.CheckSchemaAgreementAsync().Result),
                1000, 60);
            var table = new Table<Author>(session, MappingConfiguration.Global, "author", ks);
            TestHelper.RetryAssert(() =>
            {
                try
                {
                    table.CreateIfNotExists();
                    RowSet rs = null;
                    Assert.Throws<InvalidQueryException>(() => { rs = session.Execute($"INSERT INTO {ks}.author(authorid) VALUES ('auth')"); });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        rs = session.Execute($"INSERT INTO {ks}.author(authorid) VALUES ('auth')", "profile");
                    });

                    rs = session.Execute($"INSERT INTO {ks}.author(authorid) VALUES ('auth')", "derived");
                    Assert.AreEqual(ConsistencyLevel.LocalQuorum, rs.Info.AchievedConsistency);

                    rs = session.Execute($"INSERT INTO {ks}.author(authorid) VALUES ('auth') IF NOT EXISTS", "derived");
                    Assert.IsTrue(string.Compare(rs.First()["[applied]"].ToString(), "false", StringComparison.OrdinalIgnoreCase) == 0);

                    rs = session.Execute($"SELECT authorid FROM {ks}.author WHERE authorid = 'auth'", "derived");
                    var row = rs.First();
                    Assert.AreEqual(ConsistencyLevel.LocalQuorum, rs.Info.AchievedConsistency);
                    Assert.AreEqual("auth", row["authorid"].ToString());
                }
                catch (QueryTimeoutException) {}
            }, 1000, 10);
        }

        private void AssertCaMismatchSslError(NoHostAvailableException ex)
        {
#if NETCOREAPP
            var ex2 = ex.InnerException;
            Assert.IsTrue(ex2 is HttpRequestException, ex2.ToString());
            var ex3 = ex2.InnerException;
            Assert.IsTrue(ex2 is HttpRequestException, ex2.ToString());
            Assert.IsTrue(ex2.Message.Contains("The SSL connection could not be established"), ex2.Message);
#elif NETFRAMEWORK
            if (TestHelper.IsMono)
            {
                var ex2 = ex.InnerException;
                Assert.IsTrue(ex2 is WebException, ex2.ToString());
                Assert.IsTrue(ex2.Message.Contains("Authentication failed"), ex2.Message);
                Assert.IsTrue(ex2.Message.Contains("TrustFailure"), ex2.Message);
            }
            else
            {
                var ex2 = ex.InnerException;
                Assert.IsTrue(ex2 is WebException, ex2.ToString());
                Assert.IsTrue(ex2.Message.Contains("Could not establish trust relationship for the SSL/TLS secure channel"), ex2.Message);
            }
#endif
        }

        private void AssertIsSslError(NoHostAvailableException ex)
        {
#if NETFRAMEWORK
            if (TestHelper.IsMono)
            {
                var ex2 = ex.InnerException;
                Assert.IsTrue(ex2 is WebException, ex2.ToString());
                Assert.IsTrue(ex2.Message.Contains("Authentication failed"), ex2.Message);
                Assert.IsTrue(ex2.Message.Contains("SecureChannelFailure"), ex2.Message);
            }
            else
            {
                var ex2 = ex.InnerException;
                Assert.IsTrue(ex2 is WebException, ex2.ToString());
                Assert.IsTrue(ex2.Message.Contains("Could not create SSL/TLS secure channel."), ex2.Message);
            }
#else
            var ex2 = ex.InnerException;
            Assert.IsTrue(ex2 is HttpRequestException, ex2.ToString());

            // SocketsHttpHandler
            Assert.IsTrue(ex2.Message.Contains("The SSL connection could not be established"), ex2.Message);
#endif
        }
    }
}