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
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class PrepareLongTests : TestGlobals
    {
        [Test]
        public void PreparedStatement_Is_Usable_After_Node_Restart_When_Connecting_Providing_A_Keyspace()
        {
            var testCluster = TestClusterManager.CreateNew();

            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint)
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(500)).Build())
            {
                // Connect using a keyspace
                var session = cluster.Connect("system");
                var ps = session.Prepare("SELECT key FROM local");
                var host = cluster.AllHosts().First();
                var row = session.Execute(ps.Bind()).First();
                Assert.NotNull(row.GetValue<string>("key"));

                // Stop the node
                testCluster.Stop(1);
                TestHelper.WaitUntil(() => !host.IsUp, 500, 40);
                Assert.False(host.IsUp);

                // Restart the node
                testCluster.Start(1);
                TestHelper.WaitUntil(() => host.IsUp, 500, 40);
                Assert.True(host.IsUp);

                // The same prepared statement should be valid
                row = session.Execute(ps.Bind()).First();
                Assert.NotNull(row.GetValue<string>("key"));
            }
        }
    }
}