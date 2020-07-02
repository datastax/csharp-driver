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
using System.Diagnostics;
using System.Text;

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.SessionManagement;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.MetadataTests
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealClusterLong)]
    public class TokenMapTopologyChangeTests : TestGlobals
    {
        private ITestCluster TestCluster { get; set; }
        private IInternalCluster ClusterObjSync { get; set; }
        private IInternalCluster ClusterObjNotSync { get; set; }

        [Test]
        public void TokenMap_Should_RebuildTokenMap_When_NodeIsDecommissioned()
        {
            var listener = new TestTraceListener();
            var level = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            Trace.Listeners.Add(listener);
            try
            {
                TestCluster = TestClusterManager.CreateNew(3, new TestClusterOptions { UseVNodes = true });
                var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                ClusterObjSync = ClusterBuilder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(true))
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(5000))
                                        .Build();

                ClusterObjNotSync = ClusterBuilder()
                                           .AddContactPoint(TestCluster.InitialContactPoint)
                                           .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                                           .WithReconnectionPolicy(new ConstantReconnectionPolicy(5000))
                                           .Build();

                var sessionNotSync = ClusterObjNotSync.Connect();
                var sessionSync = ClusterObjSync.Connect();

                var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
                sessionNotSync.Execute(createKeyspaceCql);

                TestUtils.WaitForSchemaAgreement(ClusterObjNotSync);
                TestUtils.WaitForSchemaAgreement(ClusterObjSync);

                sessionNotSync.ChangeKeyspace(keyspaceName);
                sessionSync.ChangeKeyspace(keyspaceName);

                ICollection<Host> replicasSync = null;
                ICollection<Host> replicasNotSync = null;

                TestHelper.RetryAssert(() =>
                {
                    Assert.AreEqual(3, ClusterObjSync.InternalMetadata.Hosts.Count);
                    Assert.AreEqual(3, ClusterObjNotSync.InternalMetadata.Hosts.Count);

                    replicasSync = ClusterObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                    replicasNotSync = ClusterObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));

                    Assert.AreEqual(3, replicasSync.Count);
                    Assert.AreEqual(1, replicasNotSync.Count);
                }, 100, 150);

                var oldTokenMapNotSync = ClusterObjNotSync.InternalMetadata.TokenToReplicasMap;
                var oldTokenMapSync = ClusterObjSync.InternalMetadata.TokenToReplicasMap;
                
                if (TestClusterManager.SupportsDecommissionForcefully())
                {
                    this.TestCluster.DecommissionNodeForcefully(1);
                }
                else
                {
                    this.TestCluster.DecommissionNode(1);
                }

                this.TestCluster.Stop(1);

                TestHelper.RetryAssert(() =>
                {
                    Assert.AreEqual(2, ClusterObjSync.InternalMetadata.Hosts.Count, "ClusterObjSync.Metadata.Hosts.Count");
                    Assert.AreEqual(2, ClusterObjNotSync.InternalMetadata.Hosts.Count, "ClusterObjNotSync.Metadata.Hosts.Count");

                    replicasSync = ClusterObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                    replicasNotSync = ClusterObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));

                    Assert.AreEqual(2, replicasSync.Count, "replicasSync.Count");
                    Assert.AreEqual(1, replicasNotSync.Count, "replicasNotSync.Count");

                    Assert.IsFalse(object.ReferenceEquals(ClusterObjNotSync.InternalMetadata.TokenToReplicasMap, oldTokenMapNotSync));
                    Assert.IsFalse(object.ReferenceEquals(ClusterObjSync.InternalMetadata.TokenToReplicasMap, oldTokenMapSync));
                }, 1000, 360);

                oldTokenMapNotSync = ClusterObjNotSync.InternalMetadata.TokenToReplicasMap;
                oldTokenMapSync = ClusterObjSync.InternalMetadata.TokenToReplicasMap;

                this.TestCluster.BootstrapNode(4);
                TestHelper.RetryAssert(() =>
                {
                    Assert.AreEqual(3, ClusterObjSync.InternalMetadata.Hosts.Count);
                    Assert.AreEqual(3, ClusterObjNotSync.InternalMetadata.Hosts.Count);

                    replicasSync = ClusterObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                    replicasNotSync = ClusterObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));

                    Assert.AreEqual(3, replicasSync.Count);
                    Assert.AreEqual(1, replicasNotSync.Count);

                    Assert.IsFalse(object.ReferenceEquals(ClusterObjNotSync.InternalMetadata.TokenToReplicasMap, oldTokenMapNotSync));
                    Assert.IsFalse(object.ReferenceEquals(ClusterObjSync.InternalMetadata.TokenToReplicasMap, oldTokenMapSync));
                }, 1000, 360);

            }
            catch (Exception ex)
            {
                Trace.Flush();
                Assert.Fail("Exception: " + ex.ToString() + Environment.NewLine + string.Join(Environment.NewLine, listener.Queue.ToArray()));
            }
            finally
            {
                Trace.Listeners.Remove(listener);
                Diagnostics.CassandraTraceSwitch.Level = level;
            }
        }

        [TearDown]
        public void TearDown()
        {
            ClusterObjSync?.Shutdown();
            ClusterObjNotSync?.Shutdown();
            TestCluster?.Remove();
        }
    }
}