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
using Dse.MetadataHelpers;
using NUnit.Framework;

namespace Dse.Test.Unit.MetadataHelpers
{
    [TestFixture]
    public class NetworkTopologyStrategyTests
    {
        [Test]
        public void AreReplicationFactorsSatisfied_Should_ReturnTrue_When_NoHostInDc()
        {
            var ksReplicationFactor = new Dictionary<string, int>
            {
                {"dc1", 1},
                {"dc2", 3},
                {"dc3", 1}
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
            var ksReplicationFactor = new Dictionary<string, int>
            {
                {"dc1", 1},
                {"dc2", 3},
                {"dc3", 1}
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
        public void Should_NotReturnEqualsTrue_When_StrategiesHaveDifferentReplicationFactors()
        {
            var target1 = new NetworkTopologyStrategy(
                new Dictionary<string, int>
                {
                    { "dc1", 2 },
                    { "dc2", 1 },
                    { "dc3", 3 }
                });
            var target2 = new NetworkTopologyStrategy(
                new Dictionary<string, int>
                {
                    { "dc3", 3 },
                    { "dc1", 2 },
                    { "dc2", 3 }
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
                new Dictionary<string, int>
                {
                    { "dc1", 2 },
                    { "dc2", 3 },
                });
            var target2 = new NetworkTopologyStrategy(
                new Dictionary<string, int>
                {
                    { "dc1", 2 },
                });
            
            Assert.AreNotEqual(target1.GetHashCode(), target2.GetHashCode());
            Assert.IsFalse(target1.Equals(target2));
            Assert.IsFalse(target2.Equals(target1));
            Assert.AreNotEqual(target1, target2);
        }
    }
}