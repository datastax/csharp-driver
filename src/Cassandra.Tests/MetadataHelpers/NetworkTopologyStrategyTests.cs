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

using System.Collections.Generic;
using Cassandra.MetadataHelpers;
using NUnit.Framework;

namespace Cassandra.Tests.MetadataHelpers
{
    [TestFixture]
    public class NetworkTopologyStrategyTests
    {
        [Test]
        public void AreReplicationFactorsSatisfied_Should_ReturnTrue_When_NoHostInDc()
        {
            var ksReplicationFactor = new Dictionary<string, ReplicationFactor>
            {
                {"dc1", ReplicationFactor.Parse("1")},
                {"dc2", ReplicationFactor.Parse("3")},
                {"dc3", ReplicationFactor.Parse("1")}
            };
            var replicasByDc = new Dictionary<string, int>
            {
                {"dc1", 1},
                {"dc2", 3}
            };
            //no host in DC 3
            var datacenters = new Dictionary<string, DatacenterInfo>
            {
                {"dc1", new DatacenterInfo { HostLength = 10 } },
                {"dc2", new DatacenterInfo { HostLength = 10 } }
            };
            Assert.True(NetworkTopologyStrategy.AreReplicationFactorsSatisfied(ksReplicationFactor, replicasByDc, datacenters));
        }

        [Test]
        public void AreReplicationFactorsSatisfied_Should_ReturnFalse_When_LessReplicasThanReplicationFactorInOneDc()
        {
            var ksReplicationFactor = new Dictionary<string, ReplicationFactor>
            {
                {"dc1", ReplicationFactor.Parse("1")},
                {"dc2", ReplicationFactor.Parse("3")},
                {"dc3", ReplicationFactor.Parse("1")}
            };
            var replicasByDc = new Dictionary<string, int>
            {
                {"dc1", 1},
                {"dc2", 1}
            };
            //no host in DC 3
            var datacenters = new Dictionary<string, DatacenterInfo>
            {
                {"dc1", new DatacenterInfo { HostLength = 10 } },
                {"dc2", new DatacenterInfo { HostLength = 10 } }
            };
            Assert.False(NetworkTopologyStrategy.AreReplicationFactorsSatisfied(ksReplicationFactor, replicasByDc, datacenters));
        }

        [Test]
        public void AreReplicationFactorsSatisfied_Should_ReturnTrue_When_OnlyFullReplicas()
        {
            var ksReplicationFactor = new Dictionary<string, ReplicationFactor>
            {
                {"dc1", ReplicationFactor.Parse("1")},
                {"dc2", ReplicationFactor.Parse("3/1")},
                {"dc3", ReplicationFactor.Parse("1")}
            };
            var replicasByDc = new Dictionary<string, int>
            {
                {"dc1", 1},
                {"dc2", 2},
                {"dc3", 1}
            };
            //no host in DC 3
            var datacenters = new Dictionary<string, DatacenterInfo>
            {
                {"dc1", new DatacenterInfo { HostLength = 10 } },
                {"dc2", new DatacenterInfo { HostLength = 10 } },
                {"dc3", new DatacenterInfo { HostLength = 10 } }
            };
            Assert.True(NetworkTopologyStrategy.AreReplicationFactorsSatisfied(ksReplicationFactor, replicasByDc, datacenters));
        }

        [Test]
        public void AreReplicationFactorsSatisfied_Should_ReturnFalse_When_LessReplicasThanRf()
        {
            var ksReplicationFactor = new Dictionary<string, ReplicationFactor>
            {
                {"dc1", ReplicationFactor.Parse("1")},
                {"dc2", ReplicationFactor.Parse("3/1")},
                {"dc3", ReplicationFactor.Parse("1")}
            };
            var replicasByDc = new Dictionary<string, int>
            {
                {"dc1", 1},
                {"dc2", 1},
                {"dc3", 1}
            };
            //no host in DC 3
            var datacenters = new Dictionary<string, DatacenterInfo>
            {
                {"dc1", new DatacenterInfo { HostLength = 10 } },
                {"dc2", new DatacenterInfo { HostLength = 10 } },
                {"dc3", new DatacenterInfo { HostLength = 10 } }
            };
            Assert.False(NetworkTopologyStrategy.AreReplicationFactorsSatisfied(ksReplicationFactor, replicasByDc, datacenters));
        }

        [Test]
        public void Should_ReturnAppropriateReplicasPerDcPerToken()
        {
            var target = new NetworkTopologyStrategy(
                new Dictionary<string, ReplicationFactor>
                {
                    { "dc1", ReplicationFactor.Parse("2") },
                    { "dc2", ReplicationFactor.Parse("3/1") },
                    { "dc3", ReplicationFactor.Parse("3/2") }
                });
            var testData = ReplicationStrategyTestData.Create();

            var result = target.ComputeTokenToReplicaMap(
                testData.Ring, testData.PrimaryReplicas, testData.NumberOfHostsWithTokens, testData.Datacenters);

            // 3 dcs, 3 hosts per rack, 3 racks per dc, 10 tokens per host
            Assert.AreEqual(10 * 3 * 3 * 3, result.Count);

            foreach (var token in result)
            {
                // 2 for dc1, 2 for dc2, 1 for dc3
                Assert.AreEqual(2 + 2 + 1, token.Value.Count);
            }
        }

        [Test]
        public void Should_ReturnEqualsTrueAndSameHashCode_When_BothStrategiesHaveSameReplicationSettings()
        {
            var target1 = new NetworkTopologyStrategy(
                new Dictionary<string, ReplicationFactor>
                {
                    { "dc1", ReplicationFactor.Parse("2") },
                    { "dc2", ReplicationFactor.Parse("3/1") },
                    { "dc3", ReplicationFactor.Parse("3/2") }
                });
            var target2 = new NetworkTopologyStrategy(
                new Dictionary<string, ReplicationFactor>
                {
                    { "dc3", ReplicationFactor.Parse("3/2") },
                    { "dc1", ReplicationFactor.Parse("2") },
                    { "dc2", ReplicationFactor.Parse("3/1") }
                });

            Assert.AreEqual(target1.GetHashCode(), target2.GetHashCode());
            Assert.IsTrue(target1.Equals(target2));
            Assert.IsTrue(target2.Equals(target1));
            Assert.AreEqual(target1, target2);
        }

        [Test]
        public void Should_NotReturnEqualsTrue_When_StrategiesHaveDifferentReplicationFactors()
        {
            var target1 = new NetworkTopologyStrategy(
                new Dictionary<string, ReplicationFactor>
                {
                    { "dc1", ReplicationFactor.Parse("2") },
                    { "dc2", ReplicationFactor.Parse("3/1") },
                    { "dc3", ReplicationFactor.Parse("3/2") }
                });
            var target2 = new NetworkTopologyStrategy(
                new Dictionary<string, ReplicationFactor>
                {
                    { "dc3", ReplicationFactor.Parse("3/2") },
                    { "dc1", ReplicationFactor.Parse("2") },
                    { "dc2", ReplicationFactor.Parse("3/2") }
                });

            Assert.AreNotEqual(target1.GetHashCode(), target2.GetHashCode());
            Assert.IsFalse(target1.Equals(target2));
            Assert.IsFalse(target2.Equals(target1));
            Assert.AreNotEqual(target1, target2);
        }

        [Test]
        public void Should_NotReturnEqualsTrue_When_StrategiesHaveDifferentDatacenters()
        {
            var target1 = new NetworkTopologyStrategy(
                new Dictionary<string, ReplicationFactor>
                {
                    { "dc1", ReplicationFactor.Parse("2") },
                    { "dc2", ReplicationFactor.Parse("3/1") },
                });
            var target2 = new NetworkTopologyStrategy(
                new Dictionary<string, ReplicationFactor>
                {
                    { "dc1", ReplicationFactor.Parse("2") },
                });

            Assert.AreNotEqual(target1.GetHashCode(), target2.GetHashCode());
            Assert.IsFalse(target1.Equals(target2));
            Assert.IsFalse(target2.Equals(target1));
            Assert.AreNotEqual(target1, target2);
        }
    }
}