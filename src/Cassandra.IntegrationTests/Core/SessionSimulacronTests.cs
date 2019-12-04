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

using System.Linq;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.SessionManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class SessionSimulacronTests
    {
        private SimulacronCluster _simulacronCluster;
        private Cluster _cluster;

        [TearDown]
        public void Setup()
        {
            _cluster?.Dispose();
            _simulacronCluster?.Dispose();
        }

        [Test]
        public void Should_UseTheSameSerializerInAllConnections()
        {
            _simulacronCluster = SimulacronCluster.CreateNew(3);
            _cluster = Cluster.Builder().AddContactPoint(_simulacronCluster.InitialContactPoint).Build();
            var session = (IInternalSession) _cluster.Connect();
            var serializer = _cluster.InternalRef.GetControlConnection().Serializer;
            var connections = session.GetPools().SelectMany(kvp => kvp.Value.ConnectionsSnapshot);
            
            foreach (var c in connections)
            {
                Assert.AreSame(serializer, c.Serializer);
            }
        }
    }
}