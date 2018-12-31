// 
//       Copyright DataStax Inc.
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
// 

using System.Linq;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class HeartbeatTests
    {
        private SimulacronCluster _testCluster;
        private const string OptionsQueryType = "OPTIONS";
        private const string Query = "SELECT id FROM dummy_table";

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            _testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "1"});
            _testCluster.Prime(new
            {
                when = new { query = Query },
                then = new { result = "success" }
            });
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _testCluster.Remove().Wait();
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task Connection_Should_Send_Options_Requests_For_Heartbeats(bool executeQuery)
        {
            var builder = Cluster.Builder()
                                 .WithPoolingOptions(PoolingOptions.Create().SetHeartBeatInterval(1000))
                                 .AddContactPoint(_testCluster.InitialContactPoint);

            using (var cluster = builder.Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var logs = await _testCluster.GetNodes().First()
                                             .GetQueriesAsync(null, OptionsQueryType).ConfigureAwait(false);

                // Test idle connection after connect and after a successful query
                if (executeQuery)
                {
                    await session.ExecuteAsync(new SimpleStatement(Query)).ConfigureAwait(false);
                }
                var initialCount = logs.Count;

                await Task.Delay(1400).ConfigureAwait(false);

                logs = await _testCluster.GetNodes().First()
                                         .GetQueriesAsync(null, OptionsQueryType).ConfigureAwait(false);
                Assert.That(logs.Count, Is.GreaterThan(initialCount));
            }
        }
    }
}