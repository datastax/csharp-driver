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

using System.Linq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class ProtocolTests
    {
        [TestCase(ProtocolVersion.V4, ProtocolVersion.V5)]
        [TestCase(ProtocolVersion.V2, ProtocolVersion.V3)]
        [TestCase(ProtocolVersion.V3, ProtocolVersion.V4)]
        [TestCase(ProtocolVersion.V5, ProtocolVersion.DseV2)]
        [TestCase(ProtocolVersion.V5, ProtocolVersion.MaxSupported)]
        [TestCase(ProtocolVersion.MinSupported, ProtocolVersion.V2)]
        [TestCase(ProtocolVersion.V1, ProtocolVersion.V2)]
        [TestCase((byte)0, ProtocolVersion.MinSupported)]
        public void GetLowerSupported_Should_NotSkipBetaVersions_When_AllowBetaProtocolVersionsTrue(
            ProtocolVersion version, ProtocolVersion initialVersion)
        {
            Assert.AreEqual(version, initialVersion.GetLowerSupported(true));
        }
        
        [TestCase(ProtocolVersion.V4, ProtocolVersion.V5)]
        [TestCase(ProtocolVersion.V2, ProtocolVersion.V3)]
        [TestCase(ProtocolVersion.V3, ProtocolVersion.V4)]
        [TestCase(ProtocolVersion.V4, ProtocolVersion.DseV2)]
        [TestCase(ProtocolVersion.V4, ProtocolVersion.MaxSupported)]
        [TestCase(ProtocolVersion.MinSupported, ProtocolVersion.V2)]
        [TestCase(ProtocolVersion.V1, ProtocolVersion.V2)]
        [TestCase((byte)0, ProtocolVersion.MinSupported)]
        public void GetLowerSupported_Should_SkipBetaVersions_When_AllowBetaProtocolVersionsFalse(
            ProtocolVersion version, ProtocolVersion initialVersion)
        {
            Assert.AreEqual(version, initialVersion.GetLowerSupported(false));
        }

        [TestCase(ProtocolVersion.V4, "4.0.0", "1.2.19")]
        [TestCase(ProtocolVersion.V3, "4.0.0", "2.1.17")]
        [TestCase(ProtocolVersion.V3, "3.0.13", "2.1.17")]
        [TestCase(ProtocolVersion.V3, "2.2.11", "2.1.17")]
        [TestCase(ProtocolVersion.V2, "2.2.11", "2.0.17")]
        [TestCase(ProtocolVersion.V2, "2.0.17", "2.1.11")]
        [TestCase(ProtocolVersion.V1, "1.2.19", "2.2.11")]
        [TestCase(ProtocolVersion.V1, "1.2.19", "2.1.11")]
        [TestCase(ProtocolVersion.V1, "2.0.17", "1.2.19", null)]
        public void GetHighestCommon_Should_Downgrade_To_Protocol_VX_With_Hosts(ProtocolVersion version,
                                                                                params string[] cassandraVersions)
        {
            Assert.AreEqual(version, ProtocolVersion.MaxSupported.GetHighestCommon(false, cassandraVersions.Select(GetHost)));
        }
        

        [TestCase(ProtocolVersion.V5, "4.0.0", "1.2.19")]
        [TestCase(ProtocolVersion.V3, "4.0.0", "2.1.17")]
        [TestCase(ProtocolVersion.V3, "3.0.13", "2.1.17")]
        [TestCase(ProtocolVersion.V3, "2.2.11", "2.1.17")]
        [TestCase(ProtocolVersion.V2, "2.2.11", "2.0.17")]
        [TestCase(ProtocolVersion.V2, "2.0.17", "2.1.11")]
        [TestCase(ProtocolVersion.V1, "1.2.19", "2.2.11")]
        [TestCase(ProtocolVersion.V1, "1.2.19", "2.1.11")]
        [TestCase(ProtocolVersion.V1, "2.0.17", "1.2.19", null)]
        public void GetHighestCommon_Should_NotSkipBeta_When_AllowBetaVersionIsTrue(ProtocolVersion version,
                                                                                params string[] cassandraVersions)
        {
            Assert.AreEqual(version, ProtocolVersion.MaxSupported.GetHighestCommon(true, cassandraVersions.Select(GetHost)));
        }

        [TestCase(ProtocolVersion.V3, "6.0/3.10.2", "4.8.1/2.1.17", "5.1/3.0.13")]
        [TestCase(ProtocolVersion.V4, "6.0/3.10.2", "5.1/3.0.13")]
        public void GetHighestCommon_Should_Downgrade_To_Protocol_VX_With_Dse_Hosts(ProtocolVersion version,
                                                                                    params string[] cassandraVersions)
        {
            Assert.AreEqual(version, ProtocolVersion.MaxSupported.GetHighestCommon(false, cassandraVersions.Select(GetHost)));
        }
        
        [TestCase(ProtocolVersion.V4, "4.0.0")]
        [TestCase(ProtocolVersion.V4, "4.0.0", "1.2.19")]
        [TestCase(ProtocolVersion.V4, "3.0.13", "3.0.11", "2.2.9")]
        // can't downgrade because C* 3.0 does not support protocol lower versions than v3.
        [TestCase(ProtocolVersion.V4, "3.0.13", "2.0.17")]
        [TestCase(ProtocolVersion.V4, "3.0.13", "1.2.19")]
        [TestCase(ProtocolVersion.V3, "3.0.13", "2.2.11")]
        public void GetHighestCommon_Should_Not_Downgrade_Protocol_With_Hosts(ProtocolVersion version,
                                                                              params string[] cassandraVersions)
        {
            Assert.AreEqual(version, version.GetHighestCommon(false, cassandraVersions.Select(GetHost)));
        }
        
        [TestCase(ProtocolVersion.V5, "4.0.0")]
        [TestCase(ProtocolVersion.V5, "4.0.0", "1.2.19")]
        [TestCase(ProtocolVersion.V4, "3.0.13", "3.0.11", "2.2.9")]
        // can't downgrade because C* 3.0 does not support protocol lower versions than v3.
        [TestCase(ProtocolVersion.V4, "3.0.13", "2.0.17")]
        [TestCase(ProtocolVersion.V4, "3.0.13", "1.2.19")]
        [TestCase(ProtocolVersion.V3, "3.0.13", "2.2.11")]
        public void GetHighestCommon_Should_Not_Downgrade_Protocol_With_Hosts_When_AllowBetaVersionIsTrue(ProtocolVersion version,
                                                                              params string[] cassandraVersions)
        {
            Assert.AreEqual(version, version.GetHighestCommon(true, cassandraVersions.Select(GetHost)));
        }

        [TestCase(ProtocolVersion.V4, "5.1.7/3.0.13", "5.0.13/3.0.11", "2.2.9")]
        [TestCase(ProtocolVersion.DseV2, "6.0/3.0.0", "6.0/3.0.0")]
        // can't downgrade because C* 3.0 does not support protocol lower versions than v3.
        [TestCase(ProtocolVersion.DseV2, "6.0/3.0.13", "4.5/2.0.17")]
        public void GetHighestCommon_Should_Not_Downgrade_Protocol_With_Dse_Hosts(ProtocolVersion version,
                                                                                  params string[] cassandraVersions)
        {
            Assert.AreEqual(version, version.GetHighestCommon(false, cassandraVersions.Select(GetHost)));
        }

        private static Host GetHost(string cassandraVersion, int index)
        {
            string dseVersion = null;
            var separatorIndex = cassandraVersion?.IndexOf('/');
            if (separatorIndex >= 0)
            {
                dseVersion = cassandraVersion.Substring(0, separatorIndex.Value);
                cassandraVersion = cassandraVersion.Substring(separatorIndex.Value + 1);
            }
            return TestHelper.CreateHost($"127.0.0.{index + 1}", "dc1", "rack1", null, cassandraVersion, dseVersion);
        }
    }
}