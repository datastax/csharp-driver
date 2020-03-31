//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Cassandra.Tests;
using Dse.Connections;
using Dse.ProtocolEvents;
using NUnit.Framework;
using Dse.Test.Integration.TestClusterManagement;

namespace Dse.Test.Integration.Core
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
            if (CassandraVersion >= Version.Parse("3.0"))
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
            var cc = new ControlConnection(GetEventDebouncer(config), version, config, metadata, new List<object> { _testCluster.InitialContactPoint });
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