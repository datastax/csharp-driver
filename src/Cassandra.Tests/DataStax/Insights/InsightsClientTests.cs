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
using System.Linq.Expressions;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Insights;
using Cassandra.DataStax.Insights.InfoProviders;
using Cassandra.DataStax.Insights.InfoProviders.StartupMessage;
using Cassandra.DataStax.Insights.InfoProviders.StatusMessage;
using Cassandra.DataStax.Insights.MessageFactories;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.DataStax.Insights.Schema.StatusMessage;
using Cassandra.ExecutionProfiles;
using Cassandra.Responses;
using Cassandra.SessionManagement;
using Cassandra.Tasks;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.DataStax.Insights
{
    [TestFixture]
    public class InsightsClientTests
    {
        private TestTraceListener _listener;

        [TearDown]
        public void TearDown()
        {
            if (_listener != null)
            {
                Trace.Listeners.Remove(_listener);
                _listener = null;
            }
        }

        [Test]
        public void Should_LogFiveTimes_When_ThereAreMoreThanFiveErrorsOnStartupMessageSend()
        {
            _listener = new TestTraceListener();
            Trace.Listeners.Add(_listener);
            var logLevel = Diagnostics.CassandraTraceSwitch.Level;
            try
            {
                Cassandra.Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;

                var cluster = GetCluster(false);
                var session = GetSession(cluster);
                using (var target = InsightsClientTests.GetInsightsClient(cluster, session))
                {
                    Expression<Func<IControlConnection, Task<Response>>> mockExpression =
                        cc => cc.UnsafeSendQueryRequestAsync(
                            "CALL InsightsRpc.reportInsight(?)",
                            It.IsAny<QueryProtocolOptions>());
                    Mock.Get(cluster.Metadata.ControlConnection).Setup(mockExpression).ReturnsAsync((Response)null);

                    target.Init();

                    TestHelper.RetryAssert(
                        () => { Mock.Get(cluster.Metadata.ControlConnection).Verify(mockExpression, Times.AtLeast(10)); },
                        30);

                    Trace.Flush();
                    Assert.AreEqual(5, _listener.Queue.Count, string.Join(" ; ", _listener.Queue.ToArray()));
                    var messages = _listener.Queue.ToArray();
                    Assert.AreEqual(messages.Count(m => m.Contains("Could not send insights startup event. Exception:")), 5);
                }
            }
            finally
            {
                Diagnostics.CassandraTraceSwitch.Level = logLevel;
            }
        }

        [Test]
        public void Should_ResetErrorCounterForLogging_When_ThereSendMessageIsSuccessful()
        {
            _listener = new TestTraceListener();
            Trace.Listeners.Add(_listener);
            var logLevel = Diagnostics.CassandraTraceSwitch.Level;
            Cassandra.Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            try
            {
                var cluster = GetCluster(false);
                var session = GetSession(cluster);
                using (var target = InsightsClientTests.GetInsightsClient(cluster, session))
                {
                    Expression<Func<IControlConnection, Task<Response>>> mockExpression =
                        cc => cc.UnsafeSendQueryRequestAsync(
                            "CALL InsightsRpc.reportInsight(?)",
                            It.IsAny<QueryProtocolOptions>());
                    Mock.Get(cluster.Metadata.ControlConnection)
                        .SetupSequence(mockExpression)
                        .ReturnsAsync((Response)null)
                        .ReturnsAsync(new FakeResultResponse(ResultResponse.ResultResponseKind.Void))
                        .ReturnsAsync((Response)null)
                        .ReturnsAsync((Response)null)
                        .ReturnsAsync(new FakeResultResponse(ResultResponse.ResultResponseKind.Void))
                        .ReturnsAsync((Response)null);

                    target.Init();

                    TestHelper.RetryAssert(
                        () => { Mock.Get(cluster.Metadata.ControlConnection).Verify(mockExpression, Times.AtLeast(20)); },
                        30);

                    Trace.Flush();
                    Assert.AreEqual(8, _listener.Queue.Count, string.Join(" ; ", _listener.Queue.ToArray()));
                    var messages = _listener.Queue.ToArray();
                    Assert.AreEqual(messages.Count(m => m.Contains("Could not send insights startup event. Exception:")), 1);
                    Assert.AreEqual(messages.Count(m => m.Contains("Could not send insights status event. Exception:")), 7);
                }
            }
            finally
            {
                Diagnostics.CassandraTraceSwitch.Level = logLevel;
            }
        }

        [Test]
        public void Should_ReturnCompletedTask_When_InitIsCalledAndInsightsMonitoringEnabledIsFalse()
        {
            var cluster = Mock.Of<IInternalCluster>();
            var session = Mock.Of<IInternalSession>();
            var config = new TestConfigurationBuilder
            {
                GraphOptions = new GraphOptions()
            }.Build();
            config.MonitorReportingOptions.SetMonitorReportingEnabled(false);
            Mock.Get(cluster).SetupGet(c => c.Configuration).Returns(config);

            var insightsClient = new InsightsClient(
                cluster, session, Mock.Of<IInsightsMessageFactory<InsightsStartupData>>(), Mock.Of<IInsightsMessageFactory<InsightsStatusData>>());
            var task = insightsClient.ShutdownAsync();

            Assert.AreSame(TaskHelper.Completed, task);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ReturnCompletedTask_When_InitIsNotCalled(bool enabled)
        {
            var cluster = Mock.Of<IInternalCluster>();
            var session = Mock.Of<IInternalSession>();
            var config = new TestConfigurationBuilder
            {
                GraphOptions = new GraphOptions()
            }.Build();
            config.MonitorReportingOptions.SetMonitorReportingEnabled(enabled);
            Mock.Get(cluster).SetupGet(c => c.Configuration).Returns(config);

            var insightsClient = new InsightsClient(
                cluster, session, Mock.Of<IInsightsMessageFactory<InsightsStartupData>>(), Mock.Of<IInsightsMessageFactory<InsightsStatusData>>());
            var task = insightsClient.ShutdownAsync();

            Assert.AreSame(TaskHelper.Completed, task);
        }

        [Test]
        public void Should_InvokeRpcCallCorrectlyAndImmediately_When_SendStartupMessageIsInvoked()
        {
            const string expectedJson =
                "{\"metadata\":{" +
                    "\"name\":\"driver.startup\"," +
                    "\"timestamp\":124219041," +
                    "\"tags\":{\"language\":\"csharp\"}," +
                    "\"insightType\":\"EVENT\"," +
                    "\"insightMappingId\":\"v1\"}," +
                "\"data\":{" +
                    "\"clientId\":\"becfe098-e462-47e7-b6a7-a21cd316d4c0\"," +
                    "\"sessionId\":\"e21eab96-d91e-4790-80bd-1d5fb5472258\"," +
                    "\"applicationName\":\"appname\"," +
                    "\"applicationVersion\":\"appv1\"," +
                    "\"contactPoints\":{\"localhost\":[\"127.0.0.1:9042\"]}," +
                    "\"initialControlConnection\":\"10.10.10.10:9011\"," +
                    "\"protocolVersion\":4," +
                    "\"localAddress\":\"10.10.10.2:9015\"," +
                    "\"executionProfiles\":{" +
                        "\"default\":{" +
                            "\"readTimeout\":1505," +
                            "\"retry\":{" +
                                "\"type\":\"DowngradingConsistencyRetryPolicy\"," +
                                "\"namespace\":\"Cassandra\"}," +
                            "\"loadBalancing\":{" +
                                "\"type\":\"RoundRobinPolicy\"," +
                                "\"namespace\":\"Cassandra\"}," +
                            "\"speculativeExecution\":{" +
                                "\"type\":\"ConstantSpeculativeExecutionPolicy\"," +
                                "\"namespace\":\"Cassandra\"," +
                                "\"options\":{" +
                                    "\"delay\":1213," +
                                    "\"maxSpeculativeExecutions\":10}}," +
                            "\"consistency\":\"ALL\"," +
                            "\"serialConsistency\":\"LOCAL_SERIAL\"," +
                            "\"graphOptions\":{" +
                                "\"language\":\"gremlin-groovy\"," +
                                "\"source\":\"g\"," +
                                "\"name\":\"testGraphName\"," +
                                "\"readConsistency\":\"ALL\"," +
                                "\"writeConsistency\":null," +
                                "\"readTimeout\":-1}}}," +
                    "\"poolSizeByHostDistance\":{" +
                        "\"local\":1," +
                        "\"remote\":5}," +
                    "\"heartbeatInterval\":10000," +
                    "\"compression\":\"SNAPPY\"," +
                    "\"reconnectionPolicy\":{" +
                        "\"type\":\"ConstantReconnectionPolicy\"," +
                        "\"namespace\":\"Cassandra\"," +
                        "\"options\":{" +
                            "\"constantDelayMs\":150}}," +
                    "\"ssl\":{\"enabled\":false}," +
                    "\"authProvider\":{" +
                        "\"type\":\"NoneAuthProvider\"," +
                        "\"namespace\":\"Cassandra\"}," +
                    "\"configAntiPatterns\":{" +
                        "\"downgradingConsistency\":\"Downgrading consistency retry policy in use\"}," +
                    "\"periodicStatusInterval\":5," +
                    "\"platformInfo\":{" +
                        "\"os\":{\"name\":\"os name\",\"version\":\"os version\",\"arch\":\"os arch\"}," +
                        "\"cpus\":{\"length\":2,\"model\":\"Awesome CPU\"}," +
                        "\"runtime\":{" +
                            "\"runtimeFramework\":\"runtime-framework\"," +
                            "\"targetFramework\":\"target-framework\"," +
                            "\"dependencies\":{" +
                                "\"Assembly1\":{" +
                                    "\"fullName\":\"Assembly1FullName\"," +
                                    "\"name\":\"Assembly1\"," +
                                    "\"version\":\"1.2.0\"}}}}," +
                    "\"hostName\":\"awesome_hostname\"," +
                    "\"driverName\":\"Driver Name\"," +
                    "\"applicationNameWasGenerated\":false," +
                    "\"driverVersion\":\"1.1.2\"," +
                    "\"dataCenters\":[\"dc123\"]}}";
            var cluster = GetCluster(false, eventDelayMilliseconds: 5000);
            var session = GetSession(cluster);
            using (var target = InsightsClientTests.GetInsightsClient(cluster, session))
            {
                var queryProtocolOptions = new ConcurrentQueue<QueryProtocolOptions>();
                Mock.Get(cluster.Metadata.ControlConnection).Setup(cc => cc.UnsafeSendQueryRequestAsync(
                        "CALL InsightsRpc.reportInsight(?)",
                        It.IsAny<QueryProtocolOptions>()))
                    .ReturnsAsync(new FakeResultResponse(ResultResponse.ResultResponseKind.Void))
                    .Callback<string, QueryProtocolOptions>((query, opts) => { queryProtocolOptions.Enqueue(opts); });

                target.Init();

                TestHelper.RetryAssert(
                    () => { Assert.GreaterOrEqual(queryProtocolOptions.Count, 1); }, 10, 50);
                queryProtocolOptions.TryPeek(out var result);
                Assert.AreEqual(expectedJson, result.Values[0], "Actual: " + Environment.NewLine + result.Values[0]);
            }
        }

        [Test]
        public void Should_InvokeRpcCallCorrectlyAndImmediatelyWithExecutionProfiles_When_SendStartupMessageIsInvoked()
        {
            const string expectedJson =
                "{\"metadata\":{" +
                    "\"name\":\"driver.startup\"," +
                    "\"timestamp\":124219041," +
                    "\"tags\":{\"language\":\"csharp\"}," +
                    "\"insightType\":\"EVENT\"," +
                    "\"insightMappingId\":\"v1\"}," +
                "\"data\":{" +
                    "\"clientId\":\"becfe098-e462-47e7-b6a7-a21cd316d4c0\"," +
                    "\"sessionId\":\"e21eab96-d91e-4790-80bd-1d5fb5472258\"," +
                    "\"applicationName\":\"appname\"," +
                    "\"applicationVersion\":\"appv1\"," +
                    "\"contactPoints\":{\"localhost\":[\"127.0.0.1:9042\"]}," +
                    "\"initialControlConnection\":\"10.10.10.10:9011\"," +
                    "\"protocolVersion\":4," +
                    "\"localAddress\":\"10.10.10.2:9015\"," +
                    "\"executionProfiles\":{" +
                        "\"default\":{" +
                            "\"readTimeout\":1505," +
                            "\"retry\":{" +
                                "\"type\":\"DowngradingConsistencyRetryPolicy\"," +
                                "\"namespace\":\"Cassandra\"}," +
                            "\"loadBalancing\":{" +
                                "\"type\":\"RoundRobinPolicy\"," +
                                "\"namespace\":\"Cassandra\"}," +
                            "\"speculativeExecution\":{" +
                                "\"type\":\"ConstantSpeculativeExecutionPolicy\"," +
                                "\"namespace\":\"Cassandra\"," +
                                "\"options\":{" +
                                    "\"delay\":1213," +
                                    "\"maxSpeculativeExecutions\":10}}," +
                            "\"consistency\":\"ALL\"," +
                            "\"serialConsistency\":\"LOCAL_SERIAL\"," +
                            "\"graphOptions\":{" +
                                "\"language\":\"gremlin-groovy\"," +
                                "\"source\":\"g\"," +
                                "\"name\":\"testGraphName\"," +
                                "\"readConsistency\":\"ALL\"," +
                                "\"writeConsistency\":null," +
                                "\"readTimeout\":-1}}," +
                        "\"profile2\":{" +
                            "\"readTimeout\":501," +
                            "\"retry\":{" +
                                "\"type\":\"IdempotenceAwareRetryPolicy\"," +
                                "\"namespace\":\"Cassandra\"," +
                                "\"options\":{" +
                                    "\"childPolicy\":{" +
                                        "\"type\":\"DefaultRetryPolicy\"," +
                                        "\"namespace\":\"Cassandra\"}}}," +
                            "\"loadBalancing\":{" +
                                "\"type\":\"TokenAwarePolicy\"," +
                                "\"namespace\":\"Cassandra\"," +
                                "\"options\":{" +
                                    "\"childPolicy\":{" +
                                        "\"type\":\"RoundRobinPolicy\"," +
                                        "\"namespace\":\"Cassandra\"}}}," +
                            "\"speculativeExecution\":{" +
                                "\"type\":\"ConstantSpeculativeExecutionPolicy\"," +
                                "\"namespace\":\"Cassandra\"," +
                                "\"options\":{" +
                                    "\"delay\":230," +
                                    "\"maxSpeculativeExecutions\":5}}," +
                            "\"serialConsistency\":\"SERIAL\"" +
                        "}," +
                        "\"profile3\":{" +
                            "\"consistency\":\"EACH_QUORUM\"" +
                        "}" +
                    "}," +
                    "\"poolSizeByHostDistance\":{" +
                        "\"local\":1," +
                        "\"remote\":5}," +
                    "\"heartbeatInterval\":10000," +
                    "\"compression\":\"SNAPPY\"," +
                    "\"reconnectionPolicy\":{" +
                        "\"type\":\"ConstantReconnectionPolicy\"," +
                        "\"namespace\":\"Cassandra\"," +
                        "\"options\":{" +
                            "\"constantDelayMs\":150}}," +
                    "\"ssl\":{\"enabled\":false}," +
                    "\"authProvider\":{" +
                        "\"type\":\"NoneAuthProvider\"," +
                        "\"namespace\":\"Cassandra\"}," +
                    "\"configAntiPatterns\":{" +
                        "\"downgradingConsistency\":\"Downgrading consistency retry policy in use\"}," +
                    "\"periodicStatusInterval\":5," +
                    "\"platformInfo\":{" +
                        "\"os\":{\"name\":\"os name\",\"version\":\"os version\",\"arch\":\"os arch\"}," +
                        "\"cpus\":{\"length\":2,\"model\":\"Awesome CPU\"}," +
                        "\"runtime\":{" +
                            "\"runtimeFramework\":\"runtime-framework\"," +
                            "\"targetFramework\":\"target-framework\"," +
                            "\"dependencies\":{" +
                                "\"Assembly1\":{" +
                                    "\"fullName\":\"Assembly1FullName\"," +
                                    "\"name\":\"Assembly1\"," +
                                    "\"version\":\"1.2.0\"}}}}," +
                    "\"hostName\":\"awesome_hostname\"," +
                    "\"driverName\":\"Driver Name\"," +
                    "\"applicationNameWasGenerated\":false," +
                    "\"driverVersion\":\"1.1.2\"," +
                    "\"dataCenters\":[\"dc123\"]}}";
            var cluster = GetCluster(true, eventDelayMilliseconds: 5000);
            var session = GetSession(cluster);
            using (var target = InsightsClientTests.GetInsightsClient(cluster, session))
            {
                var queryProtocolOptions = new ConcurrentQueue<QueryProtocolOptions>();
                Mock.Get(cluster.Metadata.ControlConnection).Setup(cc => cc.UnsafeSendQueryRequestAsync(
                        "CALL InsightsRpc.reportInsight(?)",
                        It.IsAny<QueryProtocolOptions>()))
                    .ReturnsAsync(new FakeResultResponse(ResultResponse.ResultResponseKind.Void))
                    .Callback<string, QueryProtocolOptions>((query, opts) => { queryProtocolOptions.Enqueue(opts); });

                target.Init();

                TestHelper.RetryAssert(
                    () => { Assert.GreaterOrEqual(queryProtocolOptions.Count, 1); }, 10, 50);
                queryProtocolOptions.TryPeek(out var result);
                Assert.AreEqual(expectedJson, result.Values[0], "Actual: " + Environment.NewLine + result.Values[0]);
            }
        }

        [Test]
        public void Should_InvokeRpcCallCorrectlyAndPeriodically_When_SendStatusMessageIsInvoked()
        {
            const string expectedJson =
                "{\"metadata\":{" +
                    "\"name\":\"driver.status\"," +
                    "\"timestamp\":124219041," +
                    "\"tags\":{\"language\":\"csharp\"}," +
                    "\"insightType\":\"EVENT\"," +
                    "\"insightMappingId\":\"v1\"}," +
                "\"data\":{" +
                    "\"clientId\":\"becfe098-e462-47e7-b6a7-a21cd316d4c0\"," +
                    "\"sessionId\":\"e21eab96-d91e-4790-80bd-1d5fb5472258\"," +
                    "\"controlConnection\":\"10.10.10.10:9011\"," +
                    "\"connectedNodes\":{" +
                        "\"127.0.0.1:9042\":{\"connections\":3,\"inFlightQueries\":1}," +
                        "\"127.0.0.2:9042\":{\"connections\":4,\"inFlightQueries\":2}}}}";
            var cluster = GetCluster(false);
            var session = GetSession(cluster);
            using (var target = InsightsClientTests.GetInsightsClient(cluster, session))
            {
                var queryProtocolOptions = new ConcurrentQueue<QueryProtocolOptions>();
                Mock.Get(cluster.Metadata.ControlConnection).Setup(cc => cc.UnsafeSendQueryRequestAsync(
                        "CALL InsightsRpc.reportInsight(?)",
                        It.IsAny<QueryProtocolOptions>()))
                    .ReturnsAsync(new FakeResultResponse(ResultResponse.ResultResponseKind.Void))
                    .Callback<string, QueryProtocolOptions>((query, opts) => { queryProtocolOptions.Enqueue(opts); });

                target.Init();

                TestHelper.RetryAssert(() => { Assert.GreaterOrEqual(queryProtocolOptions.Count, 5); }, 5, 400);
                queryProtocolOptions.TryDequeue(out var result); // ignore startup message
                queryProtocolOptions.TryPeek(out result);
                Assert.AreEqual(expectedJson, result.Values[0], "Actual: " + Environment.NewLine + result.Values[0]);
            }
        }

        private static InsightsClient GetInsightsClient(IInternalCluster cluster, IInternalSession session)
        {
            var hostnameInfoMock = Mock.Of<IInsightsInfoProvider<string>>();
            var driverInfoMock = Mock.Of<IInsightsInfoProvider<DriverInfo>>();
            var timestampGeneratorMock = Mock.Of<IInsightsMetadataTimestampGenerator>();
            var platformInfoMock = Mock.Of<IInsightsInfoProvider<InsightsPlatformInfo>>();

            Mock.Get(hostnameInfoMock).Setup(m => m.GetInformation(cluster, session)).Returns("awesome_hostname");
            Mock.Get(driverInfoMock).Setup(m => m.GetInformation(cluster, session)).Returns(new DriverInfo
            {
                DriverVersion = "1.1.2",
                DriverName = "Driver Name"
            });
            Mock.Get(timestampGeneratorMock).Setup(m => m.GenerateTimestamp()).Returns(124219041);
            Mock.Get(platformInfoMock).Setup(m => m.GetInformation(cluster, session)).Returns(new InsightsPlatformInfo
            {
                CentralProcessingUnits = new CentralProcessingUnitsInfo
                {
                    Length = 2,
                    Model = "Awesome CPU"
                },
                Runtime = new RuntimeInfo
                {
                    Dependencies = new Dictionary<string, AssemblyInfo>
                    {
                        { "Assembly1", new AssemblyInfo { Version = "1.2.0", Name = "Assembly1", FullName = "Assembly1FullName" } }
                    },
                    RuntimeFramework = "runtime-framework",
                    TargetFramework = "target-framework"
                },
                OperatingSystem = new OperatingSystemInfo
                {
                    Version = "os version",
                    Name = "os name",
                    Arch = "os arch"
                }
            });

            var target = new InsightsClient(
                cluster,
                session,
                new InsightsStartupMessageFactory(
                    new InsightsMetadataFactory(timestampGeneratorMock),
                    new InsightsInfoProvidersCollection(
                        platformInfoMock,
                        new ExecutionProfileInfoProvider(
                            new LoadBalancingPolicyInfoProvider(new ReconnectionPolicyInfoProvider()),
                            new SpeculativeExecutionPolicyInfoProvider(),
                            new RetryPolicyInfoProvider()),
                        new PoolSizeByHostDistanceInfoProvider(),
                        new AuthProviderInfoProvider(),
                        new DataCentersInfoProvider(),
                        new OtherOptionsInfoProvider(),
                        new ConfigAntiPatternsInfoProvider(),
                        new ReconnectionPolicyInfoProvider(),
                        driverInfoMock,
                        hostnameInfoMock)),
                new InsightsStatusMessageFactory(
                    new InsightsMetadataFactory(timestampGeneratorMock),
                    new NodeStatusInfoProvider()));

            return target;
        }

        private IInternalSession GetSession(IInternalCluster cluster)
        {
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
            Mock.Get(cluster).Setup(m => m.GetHost(host1)).Returns(new Host(host1, contactPoint: null));
            Mock.Get(cluster).Setup(m => m.GetHost(host2)).Returns(new Host(host2, contactPoint: null));
            Mock.Get(session).Setup(s => s.GetPools()).Returns(pools.ToArray());
            Mock.Get(session).Setup(m => m.Cluster).Returns(cluster);
            Mock.Get(session).SetupGet(m => m.InternalSessionId).Returns(Guid.Parse("E21EAB96-D91E-4790-80BD-1D5FB5472258"));
            return session;
        }

        private IInternalCluster GetCluster(bool withProfiles, int eventDelayMilliseconds = 5)
        {
            var cluster = Mock.Of<IInternalCluster>();
            var config = GetConfig(eventDelayMilliseconds, withProfiles);
            var metadata = new Metadata(config)
            {
                ControlConnection = Mock.Of<IControlConnection>()
            };
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.ProtocolVersion).Returns(ProtocolVersion.V4);
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.EndPoint)
                .Returns(new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse("10.10.10.10"), 9011), config.ServerNameResolver, null));
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.LocalAddress).Returns(new IPEndPoint(IPAddress.Parse("10.10.10.2"), 9015));
            var hostIp = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
            var hostIp2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042);
            metadata.SetResolvedContactPoints(new Dictionary<IContactPoint, IEnumerable<IConnectionEndPoint>>
            {
                {
                    new HostnameContactPoint(
                        config.DnsResolver,
                        config.ProtocolOptions,
                        config.ServerNameResolver,
                        config.KeepContactPointsUnresolved,
                        "localhost"),
                    new[] { new ConnectionEndPoint(hostIp, config.ServerNameResolver, null) } }
            });
            metadata.AddHost(hostIp);
            metadata.AddHost(hostIp2);
            metadata.Hosts.ToCollection().First().Datacenter = "dc123";
            Mock.Get(cluster).SetupGet(m => m.Configuration).Returns(config);
            Mock.Get(cluster).SetupGet(m => m.Metadata).Returns(metadata);
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(metadata.AllHosts);
            return cluster;
        }

        private Configuration GetConfig(int eventDelayMilliseconds, bool withProfiles)
        {
            var graphOptions = new GraphOptions().SetName("testGraphName").SetReadConsistencyLevel(ConsistencyLevel.All);
            var supportVerifier = Mock.Of<IInsightsSupportVerifier>();
            Mock.Get(supportVerifier).Setup(m => m.SupportsInsights(It.IsAny<IInternalCluster>())).Returns(true);
            return new TestConfigurationBuilder
            {
                Policies = new Cassandra.Policies(
                    new RoundRobinPolicy(),
                    new ConstantReconnectionPolicy(150),
#pragma warning disable 618
                    DowngradingConsistencyRetryPolicy.Instance,
#pragma warning restore 618
                    new ConstantSpeculativeExecutionPolicy(1213, 10),
                    null,
                    null),
                ProtocolOptions = new ProtocolOptions().SetCompression(CompressionType.Snappy),
                PoolingOptions = new PoolingOptions()
                                  .SetCoreConnectionsPerHost(HostDistance.Remote, 5)
                                  .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                                  .SetHeartBeatInterval(10000),
                SocketOptions = new SocketOptions().SetReadTimeoutMillis(1505),
                QueryOptions = new QueryOptions()
                               .SetConsistencyLevel(ConsistencyLevel.All)
                               .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial),
                ExecutionProfiles = withProfiles
                    ? new Dictionary<string, IExecutionProfile>
                    {
                        {
                            "profile2",
                            new ExecutionProfileBuilder()
                                .WithReadTimeoutMillis(501)
                                .WithSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(230, 5))
                                .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                                .WithRetryPolicy(new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()))
                                .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                                .CastToClass()
                                .Build()
                        },
                        {
                            "profile3",
                            new ExecutionProfileBuilder()
                                .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                .CastToClass()
                                .Build()
                        }
                    }
                    : new Dictionary<string, IExecutionProfile>(),
                GraphOptions = graphOptions,
                ClusterId = Guid.Parse("BECFE098-E462-47E7-B6A7-A21CD316D4C0"),
                ApplicationVersion = "appv1",
                ApplicationName = "appname",
                MonitorReportingOptions = new MonitorReportingOptions().SetMonitorReportingEnabled(true).SetStatusEventDelayMilliseconds(eventDelayMilliseconds),
                InsightsSupportVerifier = supportVerifier
            }.Build();
        }
    }

    internal class FakeResultResponse : ResultResponse
    {
        internal FakeResultResponse(ResultResponseKind kind) : base(kind, Mock.Of<IOutput>())
        {
        }
    }
}