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
using Cassandra.Connections.Control;
using Cassandra.Helpers;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.ProtocolEvents;
using NUnit.Framework;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.SessionManagement;
using Cassandra.Tasks;
using Cassandra.Tests;
using Cassandra.Tests.Connections.TestHelpers;
using Moq;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class ControlConnectionTests : TestGlobals
    {
        private const int InitTimeout = 2000;
        private ITestCluster _testCluster;
        private IClusterInitializer _clusterInitializer;

        [OneTimeSetUp]
        public void SetupFixture()
        {
            _testCluster = TestClusterManager.CreateNew();
            _clusterInitializer = Mock.Of<IClusterInitializer>();
            Mock.Get(_clusterInitializer).Setup(c => c.PostInitializeAsync()).Returns(TaskHelper.Completed);
        }

        [Test]
        public void Should_Use_Maximum_Protocol_Version_Supported()
        {
            var cc = NewInstance(ProtocolVersion.MaxSupported, out var config);
            cc.InitAsync(_clusterInitializer).Wait(InitTimeout);
            Assert.AreEqual(GetProtocolVersion(), config.SerializerManager.CurrentProtocolVersion);
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
            var cc = NewInstance(version, out var config);
            cc.InitAsync(_clusterInitializer).Wait(InitTimeout);
            Assert.AreEqual(version, config.SerializerManager.CurrentProtocolVersion);
            cc.Dispose();
        }

        [Test, TestCassandraVersion(2, 2, Comparison.LessThan)]
        public void Should_Downgrade_The_Protocol_Version()
        {
            //Use a higher protocol version
            var version = (ProtocolVersion)(GetProtocolVersion() + 1);
            var cc = NewInstance(version, out var config);
            cc.InitAsync(_clusterInitializer).Wait(InitTimeout);
            Assert.AreEqual(version - 1, config.SerializerManager.CurrentProtocolVersion);
        }

        [Test, TestCassandraVersion(3, 0)]
        public void Should_Downgrade_The_Protocol_Version_With_Higher_Version_Than_Supported()
        {
            // Use a non-existent higher cassandra protocol version
            var version = (ProtocolVersion)0x0f;
            var cc = NewInstance(version, out var config);
            cc.InitAsync(_clusterInitializer).Wait(InitTimeout);
            Assert.AreEqual(ProtocolVersion.V4, config.SerializerManager.CurrentProtocolVersion);
        }

        private ControlConnection NewInstance(ProtocolVersion version, out Configuration config)
        {
            config = new Configuration();
            config.SerializerManager.ChangeProtocolVersion(version);
            var internalMetadata = new FakeInternalMetadata(config);
            internalMetadata.AddHost(new IPEndPoint(IPAddress.Parse(_testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            var cc = new ControlConnection(
                Mock.Of<IInternalCluster>(), 
                config,
                internalMetadata,
                new List<IContactPoint>
                {
                    new IpLiteralContactPoint(IPAddress.Parse(_testCluster.InitialContactPoint), config.ProtocolOptions, config.ServerNameResolver )
                });
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