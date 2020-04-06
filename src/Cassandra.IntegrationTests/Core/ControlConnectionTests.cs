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
using System.Collections.Generic;
using System.Net;
using Cassandra.Connections;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.ProtocolEvents;
using NUnit.Framework;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.SessionManagement;
using Cassandra.Tests;
using Moq;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class ControlConnectionTests : TestGlobals
    {
        private const int InitTimeout = 2000;
        private ITestCluster _testCluster;

        [OneTimeSetUp]
        public void SetupFixture()
        {
            _testCluster = TestClusterManager.CreateNew();
        }

        [Test]
        public void Should_Use_Maximum_Protocol_Version_Supported()
        {
            var cc = NewInstance();
            cc.InitAsync().Wait(InitTimeout);
            Assert.AreEqual(GetProtocolVersion(), cc.ProtocolVersion);
            cc.Dispose();
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Should_Use_Maximum_Protocol_Version_Provided()
        {
            var version = ProtocolVersion.V2;
            if (TestClusterManager.CheckCassandraVersion(false, Version.Parse("3.0"), Comparison.GreaterThanOrEqualsTo))
            {
                //protocol 2 is not supported in Cassandra 3.0+
                version = ProtocolVersion.V3;
            }
            var cc = NewInstance(version);
            cc.InitAsync().Wait(InitTimeout);
            Assert.AreEqual(version, cc.ProtocolVersion);
            cc.Dispose();
        }

        [Test, TestCassandraVersion(2, 2, Comparison.LessThan)]
        public void Should_Downgrade_The_Protocol_Version()
        {
            //Use a higher protocol version
            var version = (ProtocolVersion)(GetProtocolVersion() + 1);
            var cc = NewInstance(version);
            cc.InitAsync().Wait(InitTimeout);
            Assert.AreEqual(version - 1, cc.ProtocolVersion);
        }

        [Test, TestCassandraVersion(3, 0)]
        public void Should_Downgrade_The_Protocol_Version_With_Higher_Version_Than_Supported()
        {
            // Use a non-existent higher cassandra protocol version
            var version = (ProtocolVersion)0x0f;
            var cc = NewInstance(version);
            cc.InitAsync().Wait(InitTimeout);
            Assert.AreEqual(ProtocolVersion.V4, cc.ProtocolVersion);
        }

        private ControlConnection NewInstance(
            ProtocolVersion version = ProtocolVersion.MaxSupported,
            Configuration config = null,
            Metadata metadata = null)
        {
            config = config ?? new Configuration();
            if (metadata == null)
            {
                metadata = new Metadata(config);
                metadata.AddHost(new IPEndPoint(IPAddress.Parse(_testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            }
            var cc = new ControlConnection(
                Mock.Of<IInternalCluster>(), 
                GetEventDebouncer(config), 
                version, 
                config,
                metadata,
                config.TopologyRefresherFactory.Create(metadata, config),
                new List<IContactPoint>
                {
                    new IpLiteralContactPoint(IPAddress.Parse(_testCluster.InitialContactPoint), config.ProtocolOptions, config.ServerNameResolver )
                });
            metadata.ControlConnection = cc;
            return cc;
        }

        private IProtocolEventDebouncer GetEventDebouncer(Configuration config)
        {
            return new ProtocolEventDebouncer(
                new TaskBasedTimerFactory(), 
                TimeSpan.FromMilliseconds(config.MetadataSyncOptions.RefreshSchemaDelayIncrement), 
                TimeSpan.FromMilliseconds(config.MetadataSyncOptions.MaxTotalRefreshSchemaDelay));
        }
    }
}