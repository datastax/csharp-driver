// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, TestCassandraVersion(4, 0, isOssRequired: true)]
    public class TokenAwareTransientReplicationTests : SharedClusterTest
    {
        public TokenAwareTransientReplicationTests() : base(3, false, new TestClusterOptions
        {
            UseVNodes = false,
            CassandraYaml = new[] { "enable_transient_replication: true" }
        })
        {
        }

        [Test]
        public void TokenAware_TransientReplication_NoHopsAndOnlyFullReplicas()
        {
            var cluster = Cluster.Builder()
                                 .AddContactPoint(TestCluster.InitialContactPoint)
                                 .Build();
            try
            {
                var session = cluster.Connect();
                var ks = TestUtils.GetUniqueKeyspaceName();
                session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'NetworkTopologyStrategy', '{cluster.AllHosts().First().Datacenter}' : '3/1'}}");
                session.ChangeKeyspace(ks);
                session.Execute("CREATE TABLE tbl1 (id uuid primary key) WITH read_repair='NONE' AND additional_write_policy='NEVER'");
                var ps = session.Prepare("INSERT INTO tbl1 (id) VALUES (?)");
                var traces = new List<QueryTrace>();
                var id = Guid.NewGuid();
                for (var i = 0; i < 50; i++)
                {
                    var bound = ps
                        .Bind(id)
                        .EnableTracing();
                    var rs = session.Execute(bound);
                    traces.Add(rs.Info.QueryTrace);
                }

                var fullReplicas = new HashSet<string>(traces.Select(t => t.Coordinator.ToString()));
                Assert.AreEqual(2, fullReplicas.Count);
                
                //Check that there weren't any hops
                foreach (var t in traces)
                {
                    //The full replicas (2) must be the only ones present in the trace.
                    Assert.True(t.Events.All(e => fullReplicas.Contains(e.Source.ToString())), 
                        "There were trace events from another host for coordinator " + t.Coordinator);
                    Assert.AreEqual(fullReplicas.Count, t.Events.Select(e => e.Source.ToString()).Distinct().Count(), 
                        "Only both full replicas should have trace events which wasn't the case for coordinator " + t.Coordinator);
                }
            }
            finally
            {
                cluster.Dispose();
            }
        }
    }
}