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

using Cassandra.MetadataHelpers;

using NUnit.Framework;

namespace Cassandra.Tests.MetadataHelpers
{
    [TestFixture]
    public class SimpleStrategyTests
    {
        [Test]
        public void Should_ReturnThreeReplicasPerToken()
        {
            var target = new SimpleStrategy(ReplicationFactor.Parse("2"));
            var testData = ReplicationStrategyTestData.Create();

            var result = target.ComputeTokenToReplicaMap(
                testData.Ring, testData.PrimaryReplicas, testData.NumberOfHostsWithTokens, testData.Datacenters);
            
            // 3 dcs, 3 hosts per rack, 3 racks per dc, 10 tokens per host
            Assert.AreEqual(10 * 3 * 3 * 3, result.Count);

            foreach (var token in result)
            {
                Assert.AreEqual(2, token.Value.Count);
            }
        }
    }
}