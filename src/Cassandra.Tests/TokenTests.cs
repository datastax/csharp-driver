//
//      Copyright (C) 2012-2014 DataStax Inc.
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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
﻿using Moq;
﻿using NUnit.Framework;

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
            const string strategy = ReplicationStrategies.SimpleStrategy;
            var keyspaces = new List<KeyspaceMetadata>
            {
                CreateKeyspace("ks1", strategy, 2),
                CreateKeyspace("ks2", strategy, 10)
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
            const string strategy = ReplicationStrategies.SimpleStrategy;
            var keyspaces = new List<KeyspaceMetadata>
            {
                CreateKeyspace("ks1", strategy, 10),
                CreateKeyspace("ks2", strategy, 2)
            };
            var tokenMap = TokenMap.Build("Murmur3Partitioner", hosts, keyspaces);

            //the primary replica and the next
            var replicas = tokenMap.GetReplicas("ks1", new M3PToken(0));
            //The node without tokens should not be considered
            CollectionAssert.AreEqual(new byte[] { 0, 2}, replicas.Select(TestHelper.GetLastAddressByte));
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(-100));
            CollectionAssert.AreEqual(new byte[] { 0, 2}, replicas.Select(TestHelper.GetLastAddressByte));
            //Greater than the greatest token
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(500000));
            CollectionAssert.AreEqual(new byte[] { 0, 2 }, replicas.Select(TestHelper.GetLastAddressByte));

            //The next replica should be the first
            replicas = tokenMap.GetReplicas("ks1", new M3PToken(20));
            CollectionAssert.AreEqual(new byte[] { 2, 0}, replicas.Select(TestHelper.GetLastAddressByte));
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
        public void TokenMap_Build_SimpleStrategy_Adjacent_Ranges_Test()
        {
            const string strategy = ReplicationStrategies.SimpleStrategy;
            var hosts = new[]
            {
                //0 and 100 are adjacent
                TestHelper.CreateHost("192.168.0.1", "dc1", "rack1", new HashSet<string> {"0", "100", "1000"}),
                TestHelper.CreateHost("192.168.0.2", "dc1", "rack1", new HashSet<string> {"200",      "2000", "20000"}),
                TestHelper.CreateHost("192.168.0.3", "dc1", "rack1", new HashSet<string> {"300",      "3000", "30000"})
            };
            var ks = CreateKeyspace("ks1", strategy, 2);
            var map = TokenMap.Build("Murmur3Partitioner", hosts, new[] { ks });
            var replicas = map.GetReplicas("ks1", new M3PToken(0));
            Assert.AreEqual(2, replicas.Count);
            //It should contain the first host and the second, even though the first host contains adjacent 
            CollectionAssert.AreEqual(new byte[] { 1, 2 }, replicas.Select(TestHelper.GetLastAddressByte));
        }

        private static KeyspaceMetadata CreateKeyspace(string name, string strategy, int replicationFactor)
        {
            return new KeyspaceMetadata(null, name, true, strategy, new Dictionary<string, int> { { "replication_factor", replicationFactor } });
        }
    }
}
