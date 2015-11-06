using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using NUnit.Framework;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class ControlConnectionTests : TestGlobals
    {
        private ITestCluster _testCluster;

        [TestFixtureSetUp]
        public void SetupFixture()
        {
            _testCluster = TestClusterManager.CreateNew();
        }

        [Test]
        public void Should_Use_Maximum_Protocol_Version_Supported()
        {
            var cc = NewInstance();
            cc.Init();
            Assert.AreEqual(GetExpectedProtocolVersion(), cc.ProtocolVersion);
            cc.Dispose();
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Should_Use_Maximum_Protocol_Version_Provided()
        {
            var version = (byte) (GetExpectedProtocolVersion() - 1);
            var cc = NewInstance(version);
            cc.Init();
            Assert.AreEqual(version, cc.ProtocolVersion);
            cc.Dispose();
        }

        [Test, TestCassandraVersion(2, 2, TestBase.Comparison.LessThan)]
        public void Should_Downgrade_The_Protocol_Version()
        {
            //Use a higher protocol version
            var version = (byte) (GetExpectedProtocolVersion() + 1);
            var cc = NewInstance(version);
            cc.Init();
            Assert.AreEqual(version - 1, cc.ProtocolVersion);
        }

        private ControlConnection NewInstance(byte version = 0, Configuration config = null, Metadata metadata = null)
        {
            if (version == 0)
            {
                version = (byte) Cluster.MaxProtocolVersion;
            }
            if (config == null)
            {
                config = new Configuration();
                config.BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager();
            }
            if (metadata == null)
            {
                metadata = new Metadata(config);
                metadata.AddHost(new IPEndPoint(IPAddress.Parse(_testCluster.InitialContactPoint), ProtocolOptions.DefaultPort));
            }
            var cc = new ControlConnection(version, config, metadata);
            metadata.ControlConnection = cc;
            return cc;
        }

        private byte GetExpectedProtocolVersion()
        {
            var expectedVersion = (byte)Cluster.MaxProtocolVersion;
            if (TestClusterManager.CassandraVersion < Version.Parse("2.2"))
            {
                expectedVersion = 3;
            }
            if (TestClusterManager.CassandraVersion < Version.Parse("2.1"))
            {
                expectedVersion = 2;
            }
            if (TestClusterManager.CassandraVersion < Version.Parse("2.0"))
            {
                expectedVersion = 1;
            }
            return expectedVersion;
        }
    }
}
