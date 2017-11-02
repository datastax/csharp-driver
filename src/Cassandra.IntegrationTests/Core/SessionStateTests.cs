//
//      Copyright (C) 2017 DataStax Inc.
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
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture]
    public class SessionStateTests
    {
        private SimulacronCluster _testCluster;

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            _testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3"});
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _testCluster.Remove().Wait();
        }
        
        [Test]
        public async Task Session_GetState_Should_Return_A_Snapshot_Of_The_Pools_State()
        {
            var poolingOptions = PoolingOptions.Create().SetCoreConnectionsPerHost(HostDistance.Local, 2);
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(_testCluster.InitialContactPoint)
                                        .WithPoolingOptions(poolingOptions)
                                        .Build())
            {
                const string query = "SELECT * FROM system.local";
                var session = cluster.Connect();
                var counter = 0;
                ISessionState state = null;
                // Warmup
                await TestHelper.TimesLimit(() => session.ExecuteAsync(new SimpleStatement(query)), 200, 10);
                const int limit = 100;
                // Perform several queries and get a snapshot somewhere
                await TestHelper.TimesLimit(async () =>
                {
                    var count = Interlocked.Increment(ref counter);
                    if (count == 180)
                    {
                        // after some requests
                        state = session.GetState();
                    }
                    return await session.ExecuteAsync(new SimpleStatement(query)).ConfigureAwait(false);
                }, 300, 100).ConfigureAwait(false);
                Assert.NotNull(state);
                var stringState = state.ToString();
                CollectionAssert.AreEquivalent(cluster.AllHosts(), state.GetConnectedHosts());
                foreach (var host in cluster.AllHosts())
                {
                    Assert.AreEqual(2, state.GetOpenConnections(host));
                    StringAssert.Contains($"\"{host.Address}\": {{", stringState);
                }
                var totalInFlight = cluster.AllHosts().Sum(h => state.GetInFlightQueries(h));
                Assert.Greater(totalInFlight, 0);
                Assert.LessOrEqual(totalInFlight, limit);
            }
        }
    }
}