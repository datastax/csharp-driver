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
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;

using Cassandra.Connections;
using Cassandra.Graph;
using Cassandra.Insights.Schema;
using Cassandra.Insights.Schema.StartupMessage;
using Cassandra.SessionManagement;

using Moq;

using NUnit.Framework;

namespace Cassandra.Tests.Insights.MessageFactories
{
    [TestFixture]
    public class InsightsMessageFactoryTests
    {
        [Test]
        public void Should_ReturnCorrectMetadata_When_CreateStartupMessageIsCalled()
        {
            var cluster = GetCluster();
            var target = Configuration.DefaultInsightsStartupMessageFactory;
            var timestamp = (long)(DateTimeOffset.UtcNow - new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)).TotalMilliseconds;

            var act = target.CreateMessage(cluster, Mock.Of<IInternalSession>());

            Assert.AreEqual(InsightType.Event, act.Metadata.InsightType);
            Assert.AreEqual("v1", act.Metadata.InsightMappingId);
            Assert.AreEqual("driver.startup", act.Metadata.Name);
            Assert.GreaterOrEqual(act.Metadata.Timestamp, timestamp);
            Assert.AreEqual(1, act.Metadata.Tags.Count);
            Assert.AreEqual("csharp", act.Metadata.Tags["language"]);
        }

        [Test]
        public void Should_ReturnCorrectMetadata_When_CreateStatusMessageIsCalled()
        {
            var cluster = GetCluster();
            var target = Configuration.DefaultInsightsStatusMessageFactory;
            var timestamp = (long)(DateTimeOffset.UtcNow - new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)).TotalMilliseconds;

            var act = target.CreateMessage(cluster, Mock.Of<IInternalSession>());

            Assert.AreEqual(InsightType.Event, act.Metadata.InsightType);
            Assert.AreEqual("v1", act.Metadata.InsightMappingId);
            Assert.AreEqual("driver.status", act.Metadata.Name);
            Assert.GreaterOrEqual(act.Metadata.Timestamp, timestamp);
            Assert.AreEqual(1, act.Metadata.Tags.Count);
            Assert.AreEqual("csharp", act.Metadata.Tags["language"]);
        }

        [Test]
        public void Should_ReturnCorrectData_When_CreateStatusMessageIsCalled()
        {
            var cluster = GetCluster();
            var session = Mock.Of<IInternalSession>();
            var mockPool1 = Mock.Of<IHostConnectionPool>();
            var mockPool2 = Mock.Of<IHostConnectionPool>();
            Mock.Get(mockPool1).SetupGet(m => m.InFlight).Returns(1);
            Mock.Get(mockPool1).SetupGet(m => m.OpenConnections).Returns(3);
            Mock.Get(mockPool2).SetupGet(m => m.InFlight).Returns(2);
            Mock.Get(mockPool2).SetupGet(m => m.OpenConnections).Returns(4);
            var host1 = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
            var host2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042);
            var pools = new Dictionary<IPEndPoint, IHostConnectionPool>
            {
                { host1, mockPool1 },
                { host2, mockPool2 }
            };
            Mock.Get(cluster).Setup(m => m.GetHost(host1)).Returns(new Host(host1));
            Mock.Get(cluster).Setup(m => m.GetHost(host2)).Returns(new Host(host2));
            Mock.Get(session).Setup(s => s.GetPools()).Returns(pools.ToArray());
            Mock.Get(session).Setup(m => m.Cluster).Returns(cluster);
            Mock.Get(session).SetupGet(m => m.InternalSessionId).Returns(Guid.Parse("E21EAB96-D91E-4790-80BD-1D5FB5472258"));
            var target = Configuration.DefaultInsightsStatusMessageFactory;

            var act = target.CreateMessage(cluster, session);

            Assert.AreEqual("10.10.10.10:9011", act.Data.ControlConnection);
            Assert.AreEqual("BECFE098-E462-47E7-B6A7-A21CD316D4C0", act.Data.ClientId.ToUpper());
            Assert.AreEqual("E21EAB96-D91E-4790-80BD-1D5FB5472258", act.Data.SessionId.ToUpper());
            Assert.AreEqual(2, act.Data.ConnectedNodes.Count);
            Assert.AreEqual(3, act.Data.ConnectedNodes["127.0.0.1:9042"].Connections);
            Assert.AreEqual(1, act.Data.ConnectedNodes["127.0.0.1:9042"].InFlightQueries);
            Assert.AreEqual(4, act.Data.ConnectedNodes["127.0.0.2:9042"].Connections);
            Assert.AreEqual(2, act.Data.ConnectedNodes["127.0.0.2:9042"].InFlightQueries);
        }

        [Test]
        public void Should_ReturnCorrectData_When_CreateStartupMessageIsCalled()
        {
            var cluster = GetCluster();
            var target = Configuration.DefaultInsightsStartupMessageFactory;

            var session = Mock.Of<IInternalSession>();
            Mock.Get(session).SetupGet(m => m.InternalSessionId).Returns(Guid.Parse("E21EAB96-D91E-4790-80BD-1D5FB5472258"));
            var act = target.CreateMessage(cluster, session);

            InsightsMessageFactoryTests.AssertStartupOptions(act);

            Assert.AreEqual(1, act.Data.ConfigAntiPatterns.Count);
            Assert.IsTrue(act.Data.ConfigAntiPatterns.ContainsKey("downgradingConsistency"));

            InsightsMessageFactoryTests.AssertContactPoints(act);

            InsightsMessageFactoryTests.AssertExecutionProfile(act);

            Assert.IsFalse(string.IsNullOrWhiteSpace(act.Data.HostName));
            Assert.AreEqual(4, act.Data.ProtocolVersion);
            Assert.AreEqual(CompressionType.Snappy, act.Data.Compression);
            Assert.AreEqual("10.10.10.10:9011", act.Data.InitialControlConnection);
            Assert.AreEqual("10.10.10.2:9015", act.Data.LocalAddress);
            Assert.AreEqual(10000, act.Data.HeartbeatInterval);
            Assert.AreEqual("E21EAB96-D91E-4790-80BD-1D5FB5472258", act.Data.SessionId.ToUpper());

            Assert.AreEqual(false, act.Data.Ssl.Enabled);

            Assert.AreEqual(1, act.Data.PoolSizeByHostDistance.Local);
            Assert.AreEqual(5, act.Data.PoolSizeByHostDistance.Remote);

            Assert.AreEqual(2, act.Data.PeriodicStatusInterval);

            Assert.AreEqual(typeof(NoneAuthProvider).Namespace, act.Data.AuthProvider.Namespace);
            Assert.AreEqual(nameof(NoneAuthProvider), act.Data.AuthProvider.Type);

            Assert.AreEqual(typeof(ConstantReconnectionPolicy).Namespace, act.Data.ReconnectionPolicy.Namespace);
            Assert.AreEqual(nameof(ConstantReconnectionPolicy), act.Data.ReconnectionPolicy.Type);
            Assert.AreEqual(1, act.Data.ReconnectionPolicy.Options.Count);
            Assert.AreEqual(150, act.Data.ReconnectionPolicy.Options["constantDelayMs"]);

            InsightsMessageFactoryTests.AssertPlatformInfo(act);
        }

        private static void AssertStartupOptions(Insight<InsightsStartupData> act)
        {
            Assert.AreEqual("appname", act.Data.ApplicationName);
            Assert.AreEqual(false, act.Data.ApplicationNameWasGenerated);
            Assert.AreEqual("appv1", act.Data.ApplicationVersion);
            Assert.AreEqual("DataStax C# Driver for Apache Cassandra", act.Data.DriverName);
            Assert.AreEqual("BECFE098-E462-47E7-B6A7-A21CD316D4C0", act.Data.ClientId.ToUpper());
            Assert.IsFalse(string.IsNullOrWhiteSpace(act.Data.DriverVersion));
        }

        private static void AssertContactPoints(Insight<InsightsStartupData> act)
        {
            Assert.AreEqual(1, act.Data.ContactPoints.Count);
            Assert.AreEqual(1, act.Data.ContactPoints["localhost"].Count);
            Assert.AreEqual("127.0.0.1:9042", act.Data.ContactPoints["localhost"][0]);
            Assert.AreEqual(1, act.Data.DataCenters.Count);
            Assert.AreEqual("dc123", act.Data.DataCenters.Single());
        }

        private static void AssertPlatformInfo(Insight<InsightsStartupData> act)
        {
            Assert.Greater(act.Data.PlatformInfo.CentralProcessingUnits.Length, 0);
#if NETCOREAPP2_0
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Assert.IsFalse(string.IsNullOrWhiteSpace(act.Data.PlatformInfo.CentralProcessingUnits.Model));
            }
            else
            {
                Assert.IsNull(act.Data.PlatformInfo.CentralProcessingUnits.Model);
            }
#endif
            Assert.IsFalse(string.IsNullOrWhiteSpace(act.Data.PlatformInfo.OperatingSystem.Version));
            Assert.IsFalse(string.IsNullOrWhiteSpace(act.Data.PlatformInfo.OperatingSystem.Name));
            Assert.IsFalse(string.IsNullOrWhiteSpace(act.Data.PlatformInfo.OperatingSystem.Arch));
            Assert.IsFalse(string.IsNullOrWhiteSpace(act.Data.PlatformInfo.Runtime.RuntimeFramework));
#if NETCOREAPP2_0
            Assert.AreEqual(".NET Standard 1.5", act.Data.PlatformInfo.Runtime.TargetFramework);
#elif NETCOREAPP2_1
            Assert.AreEqual(".NET Standard 2.0", act.Data.PlatformInfo.Runtime.TargetFramework);
#elif NET452
            Assert.AreEqual(".NET Framework 4.5", act.Data.PlatformInfo.Runtime.TargetFramework);
#endif
            Assert.Greater(
                act.Data.PlatformInfo.Runtime.Dependencies
                   .Count(c =>
                       !string.IsNullOrWhiteSpace(c.Value.Version)
                       && !string.IsNullOrWhiteSpace(c.Value.FullName)
                       && !string.IsNullOrWhiteSpace(c.Value.Name)),
                0);
        }

        private static void AssertExecutionProfile(Insight<InsightsStartupData> act)
        {
            Assert.AreEqual(1, act.Data.ExecutionProfiles.Count);
            var defaultProfile = act.Data.ExecutionProfiles["default"];
            Assert.AreEqual(ConsistencyLevel.All, defaultProfile.Consistency);
            Assert.AreEqual("g", defaultProfile.GraphOptions["source"]);
            Assert.AreEqual("gremlin-groovy", defaultProfile.GraphOptions["language"]);
            Assert.AreEqual(typeof(RoundRobinPolicy).Namespace, defaultProfile.LoadBalancing.Namespace);
            Assert.AreEqual(nameof(RoundRobinPolicy), defaultProfile.LoadBalancing.Type);
            Assert.IsNull(defaultProfile.LoadBalancing.Options);
            Assert.AreEqual(1505, defaultProfile.ReadTimeout);
#pragma warning disable 618
            Assert.AreEqual(typeof(DowngradingConsistencyRetryPolicy).Namespace, defaultProfile.Retry.Namespace);
            Assert.AreEqual(nameof(DowngradingConsistencyRetryPolicy), defaultProfile.Retry.Type);
#pragma warning restore 618
            Assert.IsNull(defaultProfile.Retry.Options);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, defaultProfile.SerialConsistency);
            Assert.AreEqual(typeof(ConstantSpeculativeExecutionPolicy).Namespace, defaultProfile.SpeculativeExecution.Namespace);
            Assert.AreEqual(nameof(ConstantSpeculativeExecutionPolicy), defaultProfile.SpeculativeExecution.Type);
            Assert.AreEqual(2, defaultProfile.SpeculativeExecution.Options.Count);
            Assert.AreEqual(10, defaultProfile.SpeculativeExecution.Options["maxSpeculativeExecutions"]);
            Assert.AreEqual(1213, defaultProfile.SpeculativeExecution.Options["delay"]);
        }

        private IInternalCluster GetCluster()
        {
            var cluster = Mock.Of<IInternalCluster>();
            var config = GetConfig();
            var metadata = new Metadata(config)
            {
                ControlConnection = Mock.Of<IControlConnection>()
            };
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.ProtocolVersion).Returns(ProtocolVersion.V4);
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.EndPoint).Returns(new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse("10.10.10.10"), 9011), null));
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.LocalAddress).Returns(new IPEndPoint(IPAddress.Parse("10.10.10.2"), 9015));
            var hostIp = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
            metadata.SetResolvedContactPoints(new Dictionary<string, IEnumerable<IPEndPoint>>
            {
                { "localhost", new IPEndPoint[] {hostIp}}
            });
            metadata.AddHost(hostIp);
            metadata.Hosts.ToCollection().First().Datacenter = "dc123";
            Mock.Get(cluster).SetupGet(m => m.Configuration).Returns(config);
            Mock.Get(cluster).SetupGet(m => m.Metadata).Returns(metadata);
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(metadata.AllHosts);
            return cluster;
        }

        private Configuration GetConfig()
        {
            return new TestConfigurationBuilder
            {
                Policies = new Cassandra.Policies(
                    new RoundRobinPolicy(),
                    new ConstantReconnectionPolicy(150),
#pragma warning disable 618
                    DowngradingConsistencyRetryPolicy.Instance,
#pragma warning restore 618
                    new ConstantSpeculativeExecutionPolicy(1213, 10),
                    null),
                ProtocolOptions = new ProtocolOptions().SetCompression(CompressionType.Snappy),
                PoolingOptions = new PoolingOptions()
                                  .SetCoreConnectionsPerHost(HostDistance.Remote, 5)
                                  .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                                  .SetHeartBeatInterval(10000),
                AuthProvider = new NoneAuthProvider(),
                AuthInfoProvider = new SimpleAuthInfoProvider(),
                SocketOptions = new SocketOptions().SetReadTimeoutMillis(1505),
                QueryOptions = new QueryOptions()
                               .SetConsistencyLevel(ConsistencyLevel.All)
                               .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>(),
                GraphOptions = new GraphOptions(),
                ClusterId = Guid.Parse("BECFE098-E462-47E7-B6A7-A21CD316D4C0"),
                ApplicationVersion = "appv1",
                ApplicationName = "appname",
                MonitorReportingOptions = new MonitorReportingOptions().SetMonitorReportingEnabled(true).SetStatusEventDelayMilliseconds(2000),
            }.Build();
        }
    }
}