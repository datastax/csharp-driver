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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.MetadataHelpers;
using Cassandra.ProtocolEvents;
using Cassandra.Tests.Connections;
using Cassandra.Tests.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TokenTests
    {
        [Test]
        public void Murmur_Hash_Test()
        {
            //inputs and result values from Cassandra
            var values = new Dictionary<byte[], M3PToken>()
            {
                {new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},                                    new M3PToken(-5563837382979743776L)},
                {new byte[] {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},                                   new M3PToken(-1513403162740402161L)},
                {new byte[] {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18},                                  new M3PToken(-495360443712684655L)},
                {new byte[] {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},                                 new M3PToken(1734091135765407943L)},
                {new byte[] {5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},                                new M3PToken(-3199412112042527988L)},
                {new byte[] {6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21},                               new M3PToken(-6316563938475080831L)},
                {new byte[] {7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},                              new M3PToken(8228893370679682632L)},
                {new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},                                           new M3PToken(5457549051747178710L)},
                {new byte[] {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},           new M3PToken(-2824192546314762522L)},
                {new byte[] {254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254},           new M3PToken(-833317529301936754)},
                {new byte[] {000, 001, 002, 003, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},           new M3PToken(6463632673159404390L)},
                {new byte[] {254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254},      new M3PToken(-1672437813826982685L)},
                {new byte[] {254, 254, 254, 254},                                                                       new M3PToken(4566408979886474012L)},
                {new byte[] {0, 0, 0, 0},                                                                               new M3PToken(-3485513579396041028L)},
                {new byte[] {0, 1, 127, 127},                                                                           new M3PToken(6573459401642635627)},
                {new byte[] {0, 255, 255, 255},                                                                         new M3PToken(123573637386978882)},
                {new byte[] {255, 1, 2, 3},                                                                             new M3PToken(-2839127690952877842)},
                {new byte[] {000, 001, 002, 003, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},           new M3PToken(6463632673159404390L)},
                {new byte[] {226, 231},                                                                                 new M3PToken(-8582699461035929883L)},
                {new byte[] {226, 231, 226, 231, 226, 231, 1},                                                          new M3PToken(2222373981930033306)},
            };
            var factory = new M3PToken.M3PTokenFactory();
            foreach (var kv in values)
            {
                Assert.AreEqual(kv.Value, factory.Hash(kv.Key));
            }
        }

        [Test]
        public void RandomPartitioner_Hash_Test()
        {
            //inputs and result values from Cassandra
            Func<string, IToken> getToken = RPToken.Factory.Parse;
            var values = new Dictionary<byte[], IToken>()
            {
                {new byte[] {0},                                                      getToken("143927757573010354572009627285182898319")},
                {new byte[] {1},                                                      getToken("113842407384990359002707962975597223745")},
                {new byte[] {2},                                                      getToken("129721498153058668219395762571499089729")},
                {new byte[] {3},                                                      getToken("161634087634434392855851743730996420760")},
                {new byte[] {1, 1, 1, 1, 1},                                          getToken("62826831507722661030027787191787718361")},
                {new byte[] {1, 1, 1, 1, 3},                                          getToken("3280052967642184217852195524766331890")},
                {new byte[] {1, 1, 1, 1, 3},                                          getToken("3280052967642184217852195524766331890")},
                {TestHelper.HexToByteArray("00112233445566778899aabbccddeeff"),       getToken("146895617013011042239963905141456044092")},
                {TestHelper.HexToByteArray("00112233445566778899aabbccddeef0"),       getToken("152768415488763703226794584233555130431")}
            };
            foreach (var kv in values)
            {
                Assert.AreEqual(kv.Value, RPToken.Factory.Hash(kv.Key));
                Assert.AreEqual(kv.Value.ToString(), RPToken.Factory.Hash(kv.Key).ToString());
            }
        }

        [Test]
        public void TokenMap_SimpleStrategy_With_Keyspace_Test()
        {
            var hosts = new List<Host>
            {
                { TestHelper.CreateHost("192.168.0.0", "dc1", "rack", new HashSet<string>{"0"})},
                { TestHelper.CreateHost("192.168.0.1", "dc1", "rack", new HashSet<string>{"10"})},
                { TestHelper.CreateHost("192.168.0.2", "dc1", "rack", new HashSet<string>{"20"})}
            };
            var keyspaces = new List<KeyspaceMetadata>
            {
                TokenTests.CreateSimpleKeyspace("ks1", 2),
                TokenTests.CreateSimpleKeyspace("ks2", 10)
            };
            var tokenMap = TokenMap.Build("Murmur3Partitioner", hosts, keyspaces);

            //the primary replica and the next
            var replicas = tokenMap.GetReplicas("ks1", new M3PToken(0));
            Assert.AreEqual("0,1", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(-100));
            Assert.AreEqual("0,1", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
            //Greater than the greatest token
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(500000));
            Assert.AreEqual("0,1", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));

            //The next replica should be the first
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(20));
            Assert.AreEqual("2,0", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));

            //The closest replica and the next
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(19));
            Assert.AreEqual("2,0", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));

            //Even if the replication factor is greater than the ring, it should return only ring size
            replicas = tokenMap.GetReplicas("ks2", new M3PToken(5));
            Assert.AreEqual("1,2,0", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));

            //The primary replica only as the keyspace was not found
            replicas = tokenMap.GetReplicas(null, new M3PToken(0));
            Assert.AreEqual("0", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
            replicas = tokenMap.GetReplicas(null, new M3PToken(10));
            Assert.AreEqual("1", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
            replicas = tokenMap.GetReplicas("ks_does_not_exist", new M3PToken(20));
            Assert.AreEqual("2", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
            replicas = tokenMap.GetReplicas(null, new M3PToken(19));
            Assert.AreEqual("2", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
        }

        [Test]
        public void TokenMap_SimpleStrategy_With_Hosts_Without_Tokens()
        {
            var hosts = new List<Host>
            {
                { TestHelper.CreateHost("192.168.0.0", "dc1", "rack", new HashSet<string>{"0"})},
                { TestHelper.CreateHost("192.168.0.1", "dc1", "rack", new string[0])},
                { TestHelper.CreateHost("192.168.0.2", "dc1", "rack", new HashSet<string>{"20"})}
            };
            var keyspaces = new List<KeyspaceMetadata>
            {
                TokenTests.CreateSimpleKeyspace("ks1", 10),
                TokenTests.CreateSimpleKeyspace("ks2", 2)
            };
            var tokenMap = TokenMap.Build("Murmur3Partitioner", hosts, keyspaces);

            //the primary replica and the next
            var replicas = tokenMap.GetReplicas("ks1", new M3PToken(0));
            //The node without tokens should not be considered
            CollectionAssert.AreEqual(new byte[] { 0, 2 }, replicas.Select(TestHelper.GetLastAddressByte));
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(-100));
            CollectionAssert.AreEqual(new byte[] { 0, 2 }, replicas.Select(TestHelper.GetLastAddressByte));
            //Greater than the greatest token
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(500000));
            CollectionAssert.AreEqual(new byte[] { 0, 2 }, replicas.Select(TestHelper.GetLastAddressByte));

            //The next replica should be the first
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(20));
            CollectionAssert.AreEqual(new byte[] { 2, 0 }, replicas.Select(TestHelper.GetLastAddressByte));
        }

        [Test]
        public void TokenMap_NetworkTopologyStrategy_With_Keyspace_Test()
        {
            var hosts = new List<Host>
            {
                { TestHelper.CreateHost("192.168.0.0", "dc1", "rack1", new HashSet<string>{"0"})},
                { TestHelper.CreateHost("192.168.0.1", "dc1", "rack1", new HashSet<string>{"100"})},
                { TestHelper.CreateHost("192.168.0.2", "dc1", "rack1", new HashSet<string>{"200"})},
                { TestHelper.CreateHost("192.168.0.100", "dc2", "rack1", new HashSet<string>{"1"})},
                { TestHelper.CreateHost("192.168.0.101", "dc2", "rack1", new HashSet<string>{"101"})},
                { TestHelper.CreateHost("192.168.0.102", "dc2", "rack1", new HashSet<string>{"201"})}
            };
            const string strategy = ReplicationStrategies.NetworkTopologyStrategy;
            var keyspaces = new List<KeyspaceMetadata>
            {
                //network strategy with rf 2 per dc
                new KeyspaceMetadata(null, "ks1", true, strategy, new Dictionary<string, int> {{"dc1", 2}, {"dc2", 2}}),
                //Testing simple (even it is not supposed to be)
                new KeyspaceMetadata(null, "ks2", true, ReplicationStrategies.SimpleStrategy, new Dictionary<string, int> {{"replication_factor", 3}}),
                //network strategy with rf 3 dc1 and 1 dc2
                new KeyspaceMetadata(null, "ks3", true, strategy, new Dictionary<string, int> {{"dc1", 3}, {"dc2", 1}, {"dc3", 5}}),
                //network strategy with rf 4 dc1
                new KeyspaceMetadata(null, "ks4", true, strategy, new Dictionary<string, int> {{"dc1", 5}})
            };
            var tokenMap = TokenMap.Build("Murmur3Partitioner", hosts, keyspaces);
            //KS1
            //the primary replica and the next
            var replicas = tokenMap.GetReplicas("ks1", new M3PToken(0));
            Assert.AreEqual("0,100,1,101", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
            //The next replica should be the first
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(200));
            Assert.AreEqual("2,102,0,100", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
            //The closest replica and the next
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(190));
            Assert.AreEqual("2,102,0,100", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));

            //KS2
            //Simple strategy: 3 tokens no matter which dc
            replicas = tokenMap.GetReplicas("ks2", new M3PToken(5000));
            Assert.AreEqual("0,100,1", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));

            //KS3
            replicas = tokenMap.GetReplicas("ks3", new M3PToken(0));
            Assert.AreEqual("0,100,1,2", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
            replicas = tokenMap.GetReplicas("ks3", new M3PToken(201));
            Assert.AreEqual("102,0,1,2", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));

            //KS4
            replicas = tokenMap.GetReplicas("ks4", new M3PToken(0));
            Assert.AreEqual("0,1,2", String.Join(",", replicas.Select(TestHelper.GetLastAddressByte)));
        }

        [Test]
        public void TokenMap_Build_NetworkTopology_Adjacent_Ranges_Test()
        {
            const string strategy = ReplicationStrategies.NetworkTopologyStrategy;
            var hosts = new[]
            {
                //0 and 100 are adjacent
                TestHelper.CreateHost("192.168.0.1", "dc1", "rack1", new HashSet<string> {"0", "100", "1000"}),
                TestHelper.CreateHost("192.168.0.2", "dc1", "rack1", new HashSet<string> {"200",      "2000", "20000"}),
                TestHelper.CreateHost("192.168.0.3", "dc1", "rack1", new HashSet<string> {"300",      "3000", "30000"})
            };
            var ks = new KeyspaceMetadata(null, "ks1", true, strategy, new Dictionary<string, int> { { "dc1", 2 } });
            var map = TokenMap.Build("Murmur3Partitioner", hosts, new[] { ks });
            var replicas = map.GetReplicas("ks1", new M3PToken(0));
            Assert.AreEqual(2, replicas.Count);
            //It should contain the first host and the second, even though the first host contains adjacent
            CollectionAssert.AreEqual(new byte[] { 1, 2 }, replicas.Select(TestHelper.GetLastAddressByte));
        }

        [Test]
        public void TokenMap_Build_Should_Memorize_Tokens_Per_Replication_Test()
        {
            const string strategy = ReplicationStrategies.NetworkTopologyStrategy;
            var hosts = new[]
            {
                //0 and 100 are adjacent
                TestHelper.CreateHost("192.168.0.1", "dc1", "dc1_rack1", new HashSet<string> {"0", "100", "1000"}),
                TestHelper.CreateHost("192.168.0.2", "dc1", "dc1_rack2", new HashSet<string> {"200", "2000", "20000"}),
                TestHelper.CreateHost("192.168.0.3", "dc1", "dc1_rack1", new HashSet<string> {"300", "3000", "30000"}),
                TestHelper.CreateHost("192.168.0.4", "dc2", "dc2_rack1", new HashSet<string> {"400", "4000", "40000"}),
                TestHelper.CreateHost("192.168.0.5", "dc2", "dc2_rack2", new HashSet<string> {"500", "5000", "50000"})
            };
            var ks1 = new KeyspaceMetadata(null, "ks1", true, strategy, new Dictionary<string, int> { { "dc1", 2 }, { "dc2", 1 } });
            var ks2 = new KeyspaceMetadata(null, "ks2", true, strategy, new Dictionary<string, int> { { "dc1", 2 }, { "dc2", 1 } });
            var ks3 = new KeyspaceMetadata(null, "ks3", true, strategy, new Dictionary<string, int> { { "dc1", 2 } });
            var map = TokenMap.Build("Murmur3Partitioner", hosts, new[] { ks1, ks2, ks3 });
            var tokens1 = map.GetByKeyspace("ks1");
            var tokens2 = map.GetByKeyspace("ks2");
            var tokens3 = map.GetByKeyspace("ks3");
            Assert.AreSame(tokens1, tokens2);
            Assert.AreNotSame(tokens1, tokens3);
        }

        [Test]
        public void TokenMap_Build_NetworkTopology_Multiple_Racks_Test()
        {
            const string strategy = ReplicationStrategies.NetworkTopologyStrategy;
            var hosts = new[]
            {
                // DC1 racks has contiguous tokens
                // DC2 racks are properly organized
                TestHelper.CreateHost("192.168.0.0", "dc1", "dc1_rack1", new HashSet<string> {"0"}),
                TestHelper.CreateHost("192.168.0.1", "dc2", "dc2_rack1", new HashSet<string> {"1"}),
                TestHelper.CreateHost("192.168.0.2", "dc1", "dc1_rack2", new HashSet<string> {"2"}),
                TestHelper.CreateHost("192.168.0.3", "dc2", "dc2_rack2", new HashSet<string> {"3"}),
                TestHelper.CreateHost("192.168.0.4", "dc1", "dc1_rack1", new HashSet<string> {"4"}),
                TestHelper.CreateHost("192.168.0.5", "dc2", "dc2_rack1", new HashSet<string> {"5"}),
                TestHelper.CreateHost("192.168.0.6", "dc1", "dc1_rack2", new HashSet<string> {"6"}),
                TestHelper.CreateHost("192.168.0.7", "dc2", "dc2_rack2", new HashSet<string> {"7"})
            };
            var ks = new KeyspaceMetadata(null, "ks1", true, strategy, new Dictionary<string, int>
            {
                { "dc1", 3 },
                { "dc2", 2 }
            });
            var map = TokenMap.Build("Murmur3Partitioner", hosts, new[] { ks });
            var replicas = map.GetReplicas("ks1", new M3PToken(0));
            CollectionAssert.AreEqual(new byte[] { 0, 1, 2, 3, 4 }, replicas.Select(TestHelper.GetLastAddressByte));
        }

        [Test]
        public void TokenMap_Build_NetworkTopology_Multiple_Racks_Skipping_Hosts_Test()
        {
            const string strategy = ReplicationStrategies.NetworkTopologyStrategy;
            var hosts = new[]
            {
                // DC1 racks has contiguous tokens
                // DC2 racks are properly organized
                TestHelper.CreateHost("192.168.0.0", "dc1", "dc1_rack1", new HashSet<string> {"0"}),
                TestHelper.CreateHost("192.168.0.1", "dc2", "dc2_rack1", new HashSet<string> {"1"}),
                TestHelper.CreateHost("192.168.0.2", "dc1", "dc1_rack1", new HashSet<string> {"2"}),
                TestHelper.CreateHost("192.168.0.3", "dc2", "dc2_rack2", new HashSet<string> {"3"}),
                TestHelper.CreateHost("192.168.0.4", "dc1", "dc1_rack2", new HashSet<string> {"4"}),
                TestHelper.CreateHost("192.168.0.5", "dc2", "dc2_rack1", new HashSet<string> {"5"}),
                TestHelper.CreateHost("192.168.0.6", "dc1", "dc1_rack2", new HashSet<string> {"6"}),
                TestHelper.CreateHost("192.168.0.7", "dc2", "dc2_rack2", new HashSet<string> {"7"})
            };
            var ks = new KeyspaceMetadata(null, "ks1", true, strategy, new Dictionary<string, int>
            {
                { "dc1", 3 },
                { "dc2", 2 }
            });
            var map = TokenMap.Build("Murmur3Partitioner", hosts, new[] { ks });
            var values = new[]
            {
                Tuple.Create(0, new byte[] { 0, 1, 3, 4, 2 }),
                Tuple.Create(1, new byte[] { 1, 2, 3, 4, 6 }),
                Tuple.Create(4, new byte[] { 4, 5, 7, 0, 6 })
            };
            foreach (var v in values)
            {
                var replicas = map.GetReplicas("ks1", new M3PToken(v.Item1));
                CollectionAssert.AreEqual(v.Item2, replicas.Select(TestHelper.GetLastAddressByte));
            }
        }

        [Test, TestTimeout(2000)]
        public void TokenMap_Build_NetworkTopology_Quickly_Leave_When_Dc_Not_Found()
        {
            const string strategy = ReplicationStrategies.NetworkTopologyStrategy;
            var hosts = new Host[100];
            for (var i = 0; i < hosts.Length; i++)
            {
                hosts[i] = TestHelper.CreateHost("192.168.0." + i, "dc" + (i % 2), "rack1", new HashSet<string>());
            }
            for (var i = 0; i < 256 * hosts.Length; i++)
            {
                var tokens = (HashSet<string>)hosts[i % hosts.Length].Tokens;
                tokens.Add(i.ToString());
            }
            var ks = new KeyspaceMetadata(null, "ks1", true, strategy, new Dictionary<string, int>
            {
                { "dc1", 3 },
                { "dc2", 2 },
                { "dc3", 1 }
            });
            TokenMap.Build("Murmur3Partitioner", hosts, new[] { ks });
        }

        [Test]
        public void TokenMap_Build_SimpleStrategy_Adjacent_Ranges_Test()
        {
            var hosts = new[]
            {
                //0 and 100 are adjacent
                TestHelper.CreateHost("192.168.0.1", "dc1", "rack1", new HashSet<string> {"0", "100", "1000"}),
                TestHelper.CreateHost("192.168.0.2", "dc1", "rack1", new HashSet<string> {"200", "2000", "20000"}),
                TestHelper.CreateHost("192.168.0.3", "dc1", "rack1", new HashSet<string> {"300", "3000", "30000"})
            };
            var ks = TokenTests.CreateSimpleKeyspace("ks1", 2);
            var map = TokenMap.Build("Murmur3Partitioner", hosts, new[] { ks });
            var replicas = map.GetReplicas("ks1", new M3PToken(0));
            Assert.AreEqual(2, replicas.Count);
            //It should contain the first host and the second, even though the first host contains adjacent
            CollectionAssert.AreEqual(new byte[] { 1, 2 }, replicas.Select(TestHelper.GetLastAddressByte));
        }
        
        [Test]
        public void Build_Should_OnlyCallOncePerReplicationConfiguration_When_MultipleKeyspacesWithSameReplicationOptions()
        {
            var hosts = new List<Host>
            {
                { TestHelper.CreateHost("192.168.0.0", "dc1", "rack", new HashSet<string>{"0"})},
                { TestHelper.CreateHost("192.168.0.1", "dc1", "rack", new HashSet<string>{"10"})},
                { TestHelper.CreateHost("192.168.0.2", "dc1", "rack", new HashSet<string>{"20"})},
                { TestHelper.CreateHost("192.168.0.3", "dc2", "rack", new HashSet<string>{"30"})},
                { TestHelper.CreateHost("192.168.0.4", "dc2", "rack", new HashSet<string>{"40"})}
            };

            var factory = new ProxyReplicationStrategyFactory();
            var keyspaces = new List<KeyspaceMetadata>
            {
                // unique configurations
                TokenTests.CreateSimpleKeyspace("ks1", 2, factory),
                TokenTests.CreateSimpleKeyspace("ks2", 10, factory),
                TokenTests.CreateSimpleKeyspace("ks3", 5, factory),
                TokenTests.CreateNetworkTopologyKeyspace("ks4", new Dictionary<string, int> {{"dc1", 2}, {"dc2", 2}}, factory),
                TokenTests.CreateNetworkTopologyKeyspace("ks5", new Dictionary<string, int> {{"dc1", 1}, {"dc2", 2}}, factory),
                TokenTests.CreateNetworkTopologyKeyspace("ks6", new Dictionary<string, int> {{"dc1", 1}}, factory),

                // duplicate configurations
                TokenTests.CreateNetworkTopologyKeyspace("ks7", new Dictionary<string, int> {{"dc1", 2}, {"dc2", 2}}, factory),
                TokenTests.CreateNetworkTopologyKeyspace("ks8", new Dictionary<string, int> {{"dc1", 1}}, factory),
                TokenTests.CreateNetworkTopologyKeyspace("ks9", new Dictionary<string, int> {{"dc1", 1}, {"dc2", 2}}, factory),
                TokenTests.CreateSimpleKeyspace("ks10", 10, factory),
                TokenTests.CreateSimpleKeyspace("ks11", 2, factory)
            };
            var tokenMap = TokenMap.Build("Murmur3Partitioner", hosts, keyspaces);

            var proxyStrategies = keyspaces.Select(k => (ProxyReplicationStrategy)k.Strategy).ToList();

            Assert.AreEqual(6, proxyStrategies.Count(strategy => strategy.Calls > 0));

            AssertOnlyOneStrategyIsCalled(proxyStrategies, 0, 10);
            AssertOnlyOneStrategyIsCalled(proxyStrategies, 1, 9);
            AssertOnlyOneStrategyIsCalled(proxyStrategies, 2);
            AssertOnlyOneStrategyIsCalled(proxyStrategies, 3, 6);
            AssertOnlyOneStrategyIsCalled(proxyStrategies, 4, 8);
            AssertOnlyOneStrategyIsCalled(proxyStrategies, 5, 7);
        }

        [Test]
        [Repeat(1)]
        public void Should_UpdateKeyspacesAndTokenMapCorrectly_When_MultipleThreadsCallingRefreshKeyspace()
        {
            var keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>();

            // unique configurations
            keyspaces.AddOrUpdate("ks1", TokenTests.CreateSimpleKeyspace("ks1", 2), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks2", TokenTests.CreateSimpleKeyspace("ks2", 10), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks3", TokenTests.CreateSimpleKeyspace("ks3", 5), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks4", TokenTests.CreateNetworkTopologyKeyspace("ks4", new Dictionary<string, int> { { "dc1", 2 }, { "dc2", 2 } }), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks5", TokenTests.CreateNetworkTopologyKeyspace("ks5", new Dictionary<string, int> { { "dc1", 1 }, { "dc2", 2 } }), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks6", TokenTests.CreateNetworkTopologyKeyspace("ks6", new Dictionary<string, int> { { "dc1", 1 } }), (s, keyspaceMetadata) => keyspaceMetadata);

            // duplicate configurations
            keyspaces.AddOrUpdate("ks7", TokenTests.CreateNetworkTopologyKeyspace("ks7", new Dictionary<string, int> { { "dc1", 2 }, { "dc2", 2 } }), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks8", TokenTests.CreateNetworkTopologyKeyspace("ks8", new Dictionary<string, int> { { "dc1", 1 } }), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks9", TokenTests.CreateNetworkTopologyKeyspace("ks9", new Dictionary<string, int> { { "dc1", 1 }, { "dc2", 2 } }), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks10", TokenTests.CreateSimpleKeyspace("ks10", 10), (s, keyspaceMetadata) => keyspaceMetadata);
            keyspaces.AddOrUpdate("ks11", TokenTests.CreateSimpleKeyspace("ks11", 2), (s, keyspaceMetadata) => keyspaceMetadata);

            var schemaParser = new FakeSchemaParser(keyspaces);
            var config = new TestConfigurationBuilder
            {
                ConnectionFactory = new FakeConnectionFactory()
            }.Build();
            var metadata = new Metadata(config, schemaParser) {Partitioner = "Murmur3Partitioner"};
            metadata.ControlConnection = new ControlConnection(
                new ProtocolEventDebouncer(new TaskBasedTimerFactory(), TimeSpan.FromMilliseconds(20), TimeSpan.FromSeconds(100)), 
                ProtocolVersion.V3, 
                config, 
                metadata);
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.1"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.2"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.3"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.4"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.5"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.6"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.7"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.8"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.9"), 9042));
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.10"), 9042));
            var initialToken = 1;
            foreach (var h in metadata.Hosts)
            {
                h.SetInfo(new TestHelper.DictionaryBasedRow(new Dictionary<string, object>
                {
                    { "data_center", initialToken % 2 == 0 ? "dc1" : "dc2"},
                    { "rack", "rack1" },
                    { "tokens", GenerateTokens(initialToken, 256) },
                    { "release_version", "3.11.1" }
                }));
                initialToken++;
            }
            metadata.RebuildTokenMapAsync(false, true).GetAwaiter().GetResult();
            var expectedTokenMap = metadata.TokenToReplicasMap;
            Assert.NotNull(expectedTokenMap);
            var bag = new ConcurrentBag<string>();
            var tasks = new List<Task>();
            for (var i = 0; i < 100; i++)
            {
                var index = i;
                tasks.Add(Task.Factory.StartNew(
                    () =>
                    {
                        for (var j = 0; j < 35; j++)
                        {
                            if (j % 10 == 0 && index % 2 == 0)
                            {
                                metadata.RefreshSchemaAsync().GetAwaiter().GetResult();
                            }
                            else if (j % 16 == 0)
                            {
                                if (bag.TryTake(out var ksName))
                                {
                                    if (keyspaces.TryRemove(ksName, out var ks))
                                    {
                                        metadata.RefreshSchemaAsync(ksName).GetAwaiter().GetResult();
                                        ks = metadata.GetKeyspace(ksName);
                                        if (ks != null)
                                        {
                                            throw new Exception($"refresh for {ks.Name} returned non null after refresh single.");
                                        }
                                    }
                                }
                            }
                            else 
                            if (j % 2 == 0)
                            {
                                if (bag.TryTake(out var ksName))
                                {
                                    if (keyspaces.TryRemove(ksName, out var ks))
                                    {
                                        metadata.ControlConnection.HandleKeyspaceRefreshLaterAsync(ks.Name).GetAwaiter().GetResult();
                                        ks = metadata.GetKeyspace(ksName);
                                        if (ks != null)
                                        {
                                            throw new Exception($"refresh for {ks.Name} returned non null after remove.");
                                        }
                                    }
                                }
                            }
                            else
                            {
                                var keyspaceName = $"ks_____{index}_____{j}";
                                var ks = TokenTests.CreateSimpleKeyspace(keyspaceName, (index * j) % 10);
                                keyspaces.AddOrUpdate(
                                    keyspaceName, 
                                    ks, 
                                    (s, keyspaceMetadata) => ks);
                                metadata.ControlConnection.HandleKeyspaceRefreshLaterAsync(ks.Name).GetAwaiter().GetResult();
                                ks = metadata.GetKeyspace(ks.Name);
                                if (ks == null)
                                {
                                    throw new Exception($"refresh for {keyspaceName} returned null after add.");
                                }
                                bag.Add(keyspaceName);
                            }
                        }
                    },
                    TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach));
            }
            Task.WaitAll(tasks.ToArray());
            AssertSameReplicas(keyspaces.Values, expectedTokenMap, metadata.TokenToReplicasMap);
        }

        [Test]
        public void RefreshSingleKeyspace_Should_BuildTokenMap_When_TokenMapIsNull()
        {
            var keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>();
            keyspaces.GetOrAdd("ks1", TokenTests.CreateSimpleKeyspace("ks1", 1));
            var schemaParser = new FakeSchemaParser(keyspaces);
            var metadata = new Metadata(new Configuration(), schemaParser) {Partitioner = "Murmur3Partitioner"};
            metadata.Hosts.Add(new IPEndPoint(IPAddress.Parse("192.168.0.1"), 9042));;
            metadata.Hosts.First().SetInfo(new TestHelper.DictionaryBasedRow(new Dictionary<string, object>
            {
                { "data_center", "dc1"},
                { "rack", "rack1" },
                { "tokens", GenerateTokens(1, 256) },
                { "release_version", "3.11.1" }
            }));

            Assert.IsNull(metadata.TokenToReplicasMap);
            metadata.RefreshSingleKeyspace("ks1").GetAwaiter().GetResult();
            Assert.NotNull(metadata.TokenToReplicasMap);
        }

        private void AssertSameReplicas(IEnumerable<KeyspaceMetadata> keyspaces, IReadOnlyTokenMap expectedTokenMap, IReadOnlyTokenMap actualTokenMap)
        {
            foreach (var k in keyspaces)
            {
                var actual = actualTokenMap.GetByKeyspace(k.Name);
                var expected = expectedTokenMap.GetByKeyspace(k.Name);
                if (expected != null)
                {
                    CollectionAssert.AreEqual(expected.Keys, actual.Keys);
                    foreach (var kvp in expected)
                    {
                        Assert.IsTrue(
                            expected[kvp.Key].SetEquals(actual[kvp.Key]),
                            $"mismatch in keyspace '{k}' and token '{kvp.Key}': " +
                            $"'{string.Join(",", expected[kvp.Key].Select(h => h.Address.ToString()))}' vs " +
                            $"'{string.Join(",", actual[kvp.Key].Select(h => h.Address.ToString()))}'");
                    }
                }
                else
                {
                    // keyspace is one of the keyspaces that were inserted by the tasks and wasn't removed
                    var rf = k.Replication["replication_factor"];
                    Assert.AreEqual(10 * 256, actual.Count);
                    foreach (var kvp in actual)
                    {
                        Assert.AreEqual(rf, kvp.Value.Count);
                    }
                }
            }
        }

        private void AssertOnlyOneStrategyIsCalled(IList<ProxyReplicationStrategy> strategies, params int[] equalStrategiesIndexes)
        {
            var sameStrategies = equalStrategiesIndexes.Select(t => strategies[t]).ToList();
            Assert.AreEqual(1, sameStrategies.Count(strategy => strategy.Calls == 1));
            Assert.AreEqual(sameStrategies.Count - 1, sameStrategies.Count(strategy => strategy.Calls == 0));
        }

        private IEnumerable<string> GenerateTokens(int initialToken, int numTokens)
        {
            var output = new List<string>();
            for (var i = 0; i < numTokens; i++)
            {
                output.Add(initialToken.ToString());
                initialToken += 1000;
            }
            return output;
        }

        private static KeyspaceMetadata CreateSimpleKeyspace(string name, int replicationFactor, IReplicationStrategyFactory factory = null)
        {
            return new KeyspaceMetadata(
                null,
                name,
                true,
                ReplicationStrategies.SimpleStrategy,
                new Dictionary<string, int> { { "replication_factor", replicationFactor } },
                factory ?? new ReplicationStrategyFactory());
        }

        private static KeyspaceMetadata CreateNetworkTopologyKeyspace(string name, IDictionary<string, int> replicationFactors, IReplicationStrategyFactory factory = null)
        {
            return new KeyspaceMetadata(
                null,
                name,
                true,
                ReplicationStrategies.NetworkTopologyStrategy,
                replicationFactors,
                factory ?? new ReplicationStrategyFactory());
        }
    }
}