//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Cassandra.Tests;
using Dse;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Integration.Graph
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealClusterLong)]
    [TestDseVersion(5, 0)]
    class GraphMultiNodeTests : BaseIntegrationTest
    {
        private const string GraphName = "multiNodeGraph";
        private ITestCluster _testCluster;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _testCluster = TestClusterManager.CreateNew(1, new TestClusterOptions
            {
                Workloads = new[] { "graph", "spark" }
            });
            Trace.TraceInformation("Waiting additional time for test Cluster to be ready");
            Thread.Sleep(20000);
        }

        private void PrepareForSparkTest(ITestCluster testCluster)
        {
            const string replicationConfigStr = "{'class' : 'SimpleStrategy', 'replication_factor' : 2}";
            using (var cluster = DseCluster.Builder().AddContactPoint(TestClusterManager.InitialContactPoint).Build())
            {
                WaitForWorkers(1);

                var session = cluster.Connect();

                Trace.TraceInformation("GraphMultiNodeTests: Altering keyspace for dse_leases");
                session.Execute(
                    "ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'GraphAnalytics': '2'}");

                Trace.TraceInformation("GraphMultiNodeTests: Bootstrapping node 2");
                testCluster.BootstrapNode(2, false);
                Trace.TraceInformation("GraphMultiNodeTests: Setting workload");
                testCluster.SetNodeWorkloads(2, new[] {"graph", "spark"});
                Trace.TraceInformation("GraphMultiNodeTests: Starting node 2");
                testCluster.Start(2);
                Trace.TraceInformation("Waiting additional time for new node to be ready");
                Thread.Sleep(15000);
                WaitForWorkers(2);

                Trace.TraceInformation("GraphMultiNodeTests: Creating graph");
                session.ExecuteGraph(new SimpleGraphStatement(
                    "system.graph(name)" + 
                    ".option('graph.replication_config').set(replicationConfig)" +
                    ".option('graph.system_replication_config').set(replicationConfig)" +
                    ".ifNotExists()" + 
                    (TestClusterManager.DseVersion < new Version(6, 8) ? string.Empty : ".engine(Classic)") +
                    ".create()", 
                    new {name = GraphName, replicationConfig = replicationConfigStr}));
                Trace.TraceInformation("GraphMultiNodeTests: Created graph");

                var graphStatements = new StringBuilder();
                graphStatements.Append(MakeStrict + "\n");
                graphStatements.Append(AllowScans + "\n");
                graphStatements.Append(ClassicSchemaGremlinQuery + "\n");
                graphStatements.Append(ClassicLoadGremlinQuery);
                session.ExecuteGraph(new SimpleGraphStatement(graphStatements.ToString()).SetGraphName(GraphName));
            }
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            TestClusterManager.TryRemove();
        }

        public void WaitForWorkers(int expectedWorkers)
        {
            Trace.TraceInformation("GraphMultiNodeTests: WaitForWorkers");
            var master = FindSparkMaster();
            var client = new HttpClient();
            var count = 100;
            while (count > 0)
            {
                var task = client.GetAsync(string.Format("http://{0}:7080", master));
                task.Wait(5000);
                var response = task.Result;
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    var content = response.Content.ReadAsStringAsync();
                    content.Wait();
                    var body = content.Result;
                    var match = Regex.Match(body, "Alive\\s+Workers:.*(\\d+)</li>", RegexOptions.Multiline);
                    if (match.Success && match.Groups.Count > 1)
                    {
                        try
                        {
                            var workers = int.Parse(match.Groups[1].Value);
                            if (workers == expectedWorkers)
                            {
                                return;
                            }
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }
                count--;
                Thread.Sleep(500);
            }
        }

        public string FindSparkMaster()
        {
            Trace.TraceInformation("GraphMultiNodeTests: FindSparkMaster");
            using (var cluster = DseCluster.Builder()
                        .AddContactPoint(TestClusterManager.InitialContactPoint)
                        .WithLoadBalancingPolicy(DseLoadBalancingPolicy.CreateDefault())
                        .Build())
            {
                var session = cluster.Connect();

                var sparksRS = session.Execute(new SimpleStatement("call DseClientTool.getAnalyticsGraphServer();"));
                var result = sparksRS.ToArray();
                var sparkLocation = ((SortedDictionary<string, string>)result.First()[0])["location"];
                var sparkHost = sparkLocation.Split(':')[0];
                return sparkHost;
            }
        }
        
        [Test, TestDseVersion(5, 0)]
        public void Should_Contact_Spark_Master_Directly()
        {
            PrepareForSparkTest(_testCluster);
            Trace.TraceInformation("GraphMultiNodeTests: Should_Contact_Spark_Master_Directly");
            var sparkHost = FindSparkMaster();

            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .WithLoadBalancingPolicy(DseLoadBalancingPolicy.CreateDefault())
                .Build())
            {
                var session = cluster.Connect();
                for (var i = 0; i < 10; i++)
                {
                    var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().count()").SetGraphSourceAnalytics().SetReadTimeoutMillis(120000));
                    Assert.AreEqual(sparkHost, rs.Info.QueriedHost.Address.ToString());
                }
            }
        }

        [Test, Order(1)]
        public void Should_Parse_Dse_Workload()
        {
            TestUtils.VerifyCurrentClusterWorkloads(DseVersion >= Version.Parse("5.1")
                ? new[] {"Analytics", "Cassandra", "Graph"}
                : new[] {"Analytics"});
        }
    }
}
