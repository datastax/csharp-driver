//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Insights.Schema;
using Dse.Insights.Schema.StartupMessage;
using Dse.Insights.Schema.StatusMessage;
using Dse.SessionManagement;
using Dse.Test.Integration.SimulacronAPI.Models.Logs;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.When;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Test.Integration.TestClusterManagement.Simulacron;
using Dse.Test.Unit;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Dse.Test.Integration.Insights
{
    [TestFixture, Category("short")]
    public class InsightsIntegrationTests
    {
        private static IPrimeRequest InsightsRpcPrime() =>
            new PrimeRequestBuilder().WhenQuery("CALL InsightsRpc.reportInsight(?)").ThenVoidSuccess().BuildRequest();

        private static readonly Guid clusterId = Guid.NewGuid();
        private static readonly string applicationName = "app 1";
        private static readonly string applicationVersion = "v1.2";

        private static DseCluster BuildCluster(SimulacronCluster simulacronCluster, int statusEventDelay)
        {
            return DseCluster.Builder()
                          .AddContactPoint(simulacronCluster.InitialContactPoint)
                          .WithApplicationName(InsightsIntegrationTests.applicationName)
                          .WithApplicationVersion(InsightsIntegrationTests.applicationVersion)
                          .WithClusterId(clusterId)
                          .WithSocketOptions(
                              new SocketOptions()
                                  .SetReadTimeoutMillis(5000)
                                  .SetConnectTimeoutMillis(10000))
                          .WithMonitorReporting(new MonitorReportingOptions().SetStatusEventDelayMilliseconds(statusEventDelay))
                          .Build();
        }

        [Test]
        [TestInsightsVersion]
        public void Should_InvokeInsightsRpcCall_When_SessionIsCreated()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { IsDse = true, Nodes = "3" }))
            {
                simulacronCluster.Prime(InsightsIntegrationTests.InsightsRpcPrime());
                using (var cluster = InsightsIntegrationTests.BuildCluster(simulacronCluster, 500))
                {
                    Assert.AreEqual(0, simulacronCluster.GetQueries("CALL InsightsRpc.reportInsight(?)").Count);
                    var session = (IInternalDseSession)cluster.Connect();
                    dynamic query = null;
                    TestHelper.RetryAssert(
                        () =>
                        {
                            query = simulacronCluster.GetQueries("CALL InsightsRpc.reportInsight(?)").FirstOrDefault();
                            Assert.IsNotNull(query);
                        },
                        5,
                        1000);
                    string json = string.Empty;
                    Insight<InsightsStartupData> message = null;
                    try
                    {
                        json = Encoding.UTF8.GetString(
                            Convert.FromBase64String(
                                (string) query.frame.message.options.positional_values[0].Value));
                        message = JsonConvert.DeserializeObject<Insight<InsightsStartupData>>(json);
                    }
                    catch (JsonReaderException ex)
                    {
                        Assert.Fail("failed to deserialize json: " + ex.Message + Environment.NewLine + json);
                    }

                    Assert.IsNotNull(message);
                    Assert.AreEqual(InsightType.Event, message.Metadata.InsightType);
                    Assert.IsFalse(string.IsNullOrWhiteSpace(message.Metadata.InsightMappingId));
                    Assert.AreEqual("driver.startup", message.Metadata.Name);
                    Assert.AreEqual(InsightsIntegrationTests.applicationName, message.Data.ApplicationName);
                    Assert.AreEqual(false, message.Data.ApplicationNameWasGenerated);
                    Assert.AreEqual(InsightsIntegrationTests.applicationVersion, message.Data.ApplicationVersion);
                    Assert.AreEqual(InsightsIntegrationTests.clusterId.ToString(), message.Data.ClientId);
                    Assert.AreEqual(session.InternalSessionId.ToString(), message.Data.SessionId);
                    Assert.Greater(message.Data.PlatformInfo.CentralProcessingUnits.Length, 0);
#if NETCORE
                    if (TestHelper.IsWin)
                    {
                        Assert.IsNull(message.Data.PlatformInfo.CentralProcessingUnits.Model);
                    }
                    else
                    {
                        Assert.IsFalse(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.CentralProcessingUnits.Model));
                    }
#else
                    Assert.IsFalse(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.CentralProcessingUnits.Model));
#endif
                    Assert.IsFalse(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.OperatingSystem.Version));
                    Assert.IsFalse(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.OperatingSystem.Arch));
                    Assert.IsFalse(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.OperatingSystem.Name));
                    Assert.IsFalse(message.Data.PlatformInfo.Runtime.Dependencies.Any(s => string.IsNullOrWhiteSpace(s.Value.FullName)));
                    Assert.IsFalse(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.Runtime.RuntimeFramework));
                    Assert.IsFalse(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.Runtime.TargetFramework));
                }
            }
        }
        
        [Test]
        [TestInsightsVersion]
        public void Should_InvokeInsightsRpcCallPeriodically_When_SessionIsCreatedAndEventDelayPasses()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { IsDse = true, Nodes = "3" }))
            {
                simulacronCluster.Prime(InsightsIntegrationTests.InsightsRpcPrime());
                using (var cluster = InsightsIntegrationTests.BuildCluster(simulacronCluster, 50))
                {
                    Assert.AreEqual(0, simulacronCluster.GetQueries("CALL InsightsRpc.reportInsight(?)").Count);
                    var session = (IInternalDseSession) cluster.Connect();
                    IList<RequestLog> queries = null;
                    TestHelper.RetryAssert(
                        () =>
                        {
                            queries = simulacronCluster.GetQueries("CALL InsightsRpc.reportInsight(?)");
                            var queryCount = queries.Count;
                            Assert.GreaterOrEqual(queryCount, 5);
                        },
                        250,
                        40);
                    
                    
                    string json = string.Empty;
                    Insight<InsightsStatusData> message = null;
                    try
                    {
                        json = Encoding.UTF8.GetString(
                            Convert.FromBase64String(
                                (string) queries[1].Frame.GetQueryMessage().Options.PositionalValues[0]));
                        message = JsonConvert.DeserializeObject<Insight<InsightsStatusData>>(json);
                    }
                    catch (JsonReaderException ex)
                    {
                        // simulacron issue multiple queries of the same type but different data causes data corruption
                        Assert.Inconclusive("failed to deserialize json (probably due to simulacron bug) : " + ex.Message + Environment.NewLine + json);
                    }
                    Assert.IsNotNull(message);
                    Assert.AreEqual(InsightType.Event, message.Metadata.InsightType);
                    Assert.IsFalse(string.IsNullOrWhiteSpace(message.Metadata.InsightMappingId));
                    Assert.AreEqual("driver.status", message.Metadata.Name);
                    Assert.AreEqual(InsightsIntegrationTests.clusterId.ToString(), message.Data.ClientId);
                    Assert.AreEqual(session.InternalSessionId.ToString(), message.Data.SessionId);
                }
            }
        }
    }
}