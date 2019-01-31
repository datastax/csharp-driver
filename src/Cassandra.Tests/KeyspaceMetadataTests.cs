// 
//       Copyright DataStax, Inc.
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

using System.Collections.Concurrent;
using System.Collections.Generic;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class KeyspaceMetadataTests
    {
        [Test]
        public void Ctor_Should_ParseNetworkTopologyStrategyClass_When_LongClassName()
        {
            var ks = new KeyspaceMetadata(
                null, "ks2", true, "org.apache.cassandra.locator.NetworkTopologyStrategy", new Dictionary<string, int> { { "dc1", 2 }, { "dc2", 1 } });

            Assert.IsNotNull(ks.Strategy);
            Assert.AreEqual(ReplicationStrategies.NetworkTopologyStrategy, ks.StrategyClass);
        }
        
        [Test]
        public void Ctor_Should_ParseSimpleStrategyClass_When_LongClassName()
        {
            var ks = new KeyspaceMetadata(
                null, "ks2", true, "org.apache.cassandra.locator.SimpleStrategy", new Dictionary<string, int> { { "replication_factor", 2 } });

            Assert.IsNotNull(ks.Strategy);
            Assert.AreEqual(ReplicationStrategies.SimpleStrategy, ks.StrategyClass);
        }
        
        [Test]
        public void Ctor_Should_ParseNetworkTopologyStrategyClass_When_ShortClassName()
        {
            var ks = new KeyspaceMetadata(
                null, "ks2", true, "NetworkTopologyStrategy", new Dictionary<string, int> { { "dc1", 2 }, { "dc2", 1 } });

            Assert.IsNotNull(ks.Strategy);
            Assert.AreEqual(ReplicationStrategies.NetworkTopologyStrategy, ks.StrategyClass);
        }
        
        [Test]
        public void Ctor_Should_ParseSimpleStrategyClass_When_ShortClassName()
        {
            var ks = new KeyspaceMetadata(
                null, "ks2", true, "SimpleStrategy", new Dictionary<string, int> { { "replication_factor", 2 } });

            Assert.IsNotNull(ks.Strategy);
            Assert.AreEqual(ReplicationStrategies.SimpleStrategy, ks.StrategyClass);
        }
        
        [Test]
        public void Ctor_Should_StrategyBeNull_When_UnrecognizedStrategyClass()
        {
            var ks = new KeyspaceMetadata(
                null, "ks2", true, "random", new Dictionary<string, int> { { "replication_factor", 2 } });

            Assert.IsNull(ks.Strategy);
            Assert.AreEqual("random", ks.StrategyClass);
        }
        /// <summary>
        /// This scenario happens when it's a virtual keyspace.
        /// </summary>
        [Test]
        public void Ctor_Should_InitializeStrategyWithNull_When_NullReplicationOptionsAndStrategyClassArePassed()
        {
            var sut = new KeyspaceMetadata(null, "name", false, null, null, true);
            Assert.IsNull(sut.Strategy);
        }

        /// <summary>
        /// This scenario happens when it's a virtual keyspace.
        /// </summary>
        [Test]
        public void Ctor_Should_InitializeStrategyClassWithNull_When_NullReplicationOptionsAndStrategyClassArePassed()
        {
            var sut = new KeyspaceMetadata(null, "name", false, null, null, true);
            Assert.IsNull(sut.StrategyClass);
        }

        /// <summary>
        /// This scenario happens when it's a virtual keyspace.
        /// </summary>
        [Test]
        public void Ctor_Should_InitializeReplicationsWithNull_When_NullReplicationOptionsAndStrategyClassArePassed()
        {
            var sut = new KeyspaceMetadata(null, "name", false, null, null, true);
            Assert.IsNull(sut.Replication);
        }

        [Test]
        public void Ctor_Should_InitializeStrategyWithNull_When_NullReplicationOptionsArePassedButNonNullStrategyClassIsPassed()
        {
            var sut = new KeyspaceMetadata(null, "name", true, "SimpleStrategy", null, true);
            Assert.IsNull(sut.Strategy);
        }

        [Test]
        public void Ctor_Should_InitializeReplicationWithNull_When_NullReplicationOptionsArePassedButNonNullStrategyClassIsPassed()
        {
            var sut = new KeyspaceMetadata(null, "name", true, "SimpleStrategy", null, true);
            Assert.IsNull(sut.Replication);
        }

        [Test]
        public void Ctor_Should_InitializeStrategyClassWithNonNull_When_NullReplicationOptionsArePassedButNonNullStrategyClassIsPassed()
        {
            var sut = new KeyspaceMetadata(null, "name", true, "SimpleStrategy", null, true);
            Assert.IsNotNull(sut.StrategyClass);
        }
        
        [Test]
        public void Ctor_Should_InitializeStrategyWithNull_When_NonNullReplicationOptionsArePassedButNullStrategyClassIsPassed()
        {
            var sut = new KeyspaceMetadata(null, "name", true, null, new ConcurrentDictionary<string, int>(), true);
            Assert.IsNull(sut.Strategy);
        }

        [Test]
        public void Ctor_Should_InitializeReplicationWithNonNull_When_NonNullReplicationOptionsArePassedButNullStrategyClassIsPassed()
        {
            var sut = new KeyspaceMetadata(null, "name", true, null, new ConcurrentDictionary<string, int>(), true);
            Assert.IsNotNull(sut.Replication);
        }

        [Test]
        public void Ctor_Should_InitializeStrategyClassWithNull_When_NonNullReplicationOptionsArePassedButNullStrategyClassIsPassed()
        {
            var sut = new KeyspaceMetadata(null, "name", true, null, new ConcurrentDictionary<string, int>(), true);
            Assert.IsNull(sut.StrategyClass);
        }
    }
}