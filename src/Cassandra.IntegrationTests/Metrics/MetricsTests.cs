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

#if !NET452

using System;
using System.Linq;
using System.Net;

using App.Metrics;
using App.Metrics.Gauge;

using Cassandra.Metrics;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Metrics
{
    [Category("short"), Category("realcluster")]
    public class MetricsTests : SharedClusterTest
    {
        private static readonly NodeMetric[] Counters = new[]
        {
            NodeMetric.Counters.AuthenticationErrors,
            NodeMetric.Counters.ClientTimeouts,
            NodeMetric.Counters.ConnectionInitErrors,
            NodeMetric.Counters.Ignores,
            NodeMetric.Counters.IgnoresOnOtherError,
            NodeMetric.Counters.IgnoresOnReadTimeout,
            NodeMetric.Counters.IgnoresOnUnavailable,
            NodeMetric.Counters.IgnoresOnWriteTimeout,
            NodeMetric.Counters.OtherErrors,
            NodeMetric.Counters.AbortedRequests,
            NodeMetric.Counters.ReadTimeouts,
            NodeMetric.Counters.Retries,
            NodeMetric.Counters.RetriesOnOtherError,
            NodeMetric.Counters.RetriesOnReadTimeout,
            NodeMetric.Counters.RetriesOnUnavailable,
            NodeMetric.Counters.RetriesOnWriteTimeout,
            NodeMetric.Counters.SpeculativeExecutions,
            NodeMetric.Counters.UnavailableErrors,
            NodeMetric.Counters.UnsentRequests,
            NodeMetric.Counters.WriteTimeouts
        };

        private static readonly NodeMetric[] Gauges = new[]
        {
            NodeMetric.Gauges.InFlight,
            NodeMetric.Gauges.OpenConnections
        };

        private static readonly NodeMetric[] Timers = new[]
        {
            NodeMetric.Timers.CqlMessages
        };

        private static readonly NodeMetric[] Meters = new[]
        {
            NodeMetric.Meters.BytesSent,
            NodeMetric.Meters.BytesReceived
        };

        private IMetricsRoot _metricsRoot;

        public MetricsTests() : base(3, true)
        {
        }

        [TearDown]
        public void Teardown()
        {
            if (_metricsRoot != null)
            {
                _metricsRoot.Manage.Disable();
                _metricsRoot = null;
            }
        }

        [Test]
        public void Should_RemoveNodeMetricsAndDisposeMetricsContext_When_HostIsRemoved()
        {
            _metricsRoot = new MetricsBuilder().Build();
            var cluster = GetNewCluster(b => b.WithMetrics(_metricsRoot.CreateDriverMetricsProvider()));
            var session = cluster.Connect();
            var metrics = session.GetMetrics();
            Assert.AreEqual(3, cluster.Metadata.Hosts.Count);
            Assert.AreEqual(3, metrics.NodeMetrics.Count);

            // get address for host that will be removed from the cluster in this test
            var address = TestCluster.ClusterIpPrefix + "2";
            var hostToBeRemoved = cluster.Metadata.Hosts.First(h => h.Address.Address.Equals(IPAddress.Parse(address)));

            // check node metrics are valid
            var gauge = metrics.GetNodeGauge(hostToBeRemoved, NodeMetric.Gauges.OpenConnections);
            var appMetricsGaugeValue = _metricsRoot.Snapshot.GetGaugeValue(gauge.Context, gauge.Name);
            Assert.Greater(gauge.GetValue().Value, 0);
            Assert.AreEqual(gauge.GetValue().Value, appMetricsGaugeValue);

            // check node metrics context in app metrics is valid
            var context = _metricsRoot.Snapshot.GetForContext(gauge.Context);
            Assert.True(context.IsNotEmpty());
            Assert.AreEqual(2, context.Gauges.Count());

                // remove host from cluster                
                if (TestClusterManagement.TestClusterManager.DseVersion.Major < 5 ||
                    (TestClusterManagement.TestClusterManager.DseVersion.Major == 5 && 
                     TestClusterManagement.TestClusterManager.DseVersion.Minor < 1))
                {
                    TestCluster.DecommissionNode(2);
                }
                else
                {
                    TestCluster.DecommissionNodeForcefully(2);
                }

                TestCluster.Stop(2);
                try
                {
                    TestHelper.RetryAssert(() => { Assert.AreEqual(2, cluster.Metadata.Hosts.Count, "metadata hosts count failed"); }, 200, 50);
                    TestHelper.RetryAssert(() => { Assert.AreEqual(2, metrics.NodeMetrics.Count, "Node metrics count failed"); }, 10, 500);
                }
                catch
                {
                    TestCluster.Start(2, "--jvm_arg=\"-Dcassandra.override_decommission=true\"");
                    throw;
                }

            // Check node's metrics were removed from app metrics registry
            context = _metricsRoot.Snapshot.GetForContext(gauge.Context);
            Assert.False(context.IsNotEmpty());
            Assert.AreEqual(0, context.Gauges.Count());

            TestCluster.Start(2, "--jvm_arg=\"-Dcassandra.override_decommission=true\"");

            TestHelper.RetryAssert(() => { Assert.AreEqual(3, cluster.Metadata.Hosts.Count, "metadata hosts count after bootstrap failed"); },
                200, 50);

            // when new host is chosen by LBP, connection pool is created
            foreach (var _ in Enumerable.Range(0, 5))
            {
                session.Execute("SELECT * FROM system.local");
            }

            TestHelper.RetryAssert(() => { Assert.AreEqual(3, metrics.NodeMetrics.Count, "Node metrics count after bootstrap failed"); }, 10,
                500);

            // Check node's metrics were added again
            context = _metricsRoot.Snapshot.GetForContext(gauge.Context);
            Assert.True(context.IsNotEmpty());
            Assert.AreEqual(2, context.Gauges.Count());
        }

        [Test]
        public void Should_AllMetricsHaveValidValues_When_AllNodesAreUp()
        {
            _metricsRoot = new MetricsBuilder().Build();
            var cluster = GetNewCluster(b =>
                b.WithMetrics(
                    _metricsRoot.CreateDriverMetricsProvider(),
                    new DriverMetricsOptions()
                        .SetEnabledNodeMetrics(NodeMetric.AllNodeMetrics)
                        .SetEnabledSessionMetrics(SessionMetric.AllSessionMetrics)));
            var session = cluster.Connect();
            Assert.AreEqual(25, NodeMetric.AllNodeMetrics.Count());
            Assert.AreEqual(25,
                MetricsTests.Counters.Concat(MetricsTests.Gauges.Concat(MetricsTests.Timers.Concat(MetricsTests.Meters)))
                            .Union(NodeMetric.DefaultNodeMetrics).Count());
            Assert.AreEqual(1, MetricsTests.Timers.Length);
            Assert.IsTrue(NodeMetric.AllNodeMetrics.SequenceEqual(NodeMetric.AllNodeMetrics.Distinct()));
            Assert.AreEqual(5, SessionMetric.AllSessionMetrics.Count());

            foreach (var i in Enumerable.Range(0, 1000))
            {
                session.Execute("SELECT * FROM system.local");
            }

            var metrics = session.GetMetrics();
            foreach (var h in cluster.AllHosts())
            {
                foreach (var c in MetricsTests.Counters)
                {
                    Assert.AreEqual(0, metrics.GetNodeCounter(h, c).GetValue());
                }

                Assert.AreEqual(2, MetricsTests.Gauges.Length);
                Assert.AreEqual(0, metrics.GetNodeGauge(h, NodeMetric.Gauges.InFlight).GetValue());
                Assert.Greater(metrics.GetNodeGauge(h, NodeMetric.Gauges.OpenConnections).GetValue(), 0);

                Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Max, 0);

                Assert.Greater(metrics.GetNodeMeter(h, NodeMetric.Meters.BytesSent).GetValue().Count, 0);
                Assert.Greater(metrics.GetNodeMeter(h, NodeMetric.Meters.BytesReceived).GetValue().Count, 0);
            }

            Assert.AreEqual(0, metrics.GetSessionCounter(SessionMetric.Counters.CqlClientTimeouts).GetValue());
            Assert.Greater(metrics.GetSessionTimer(SessionMetric.Timers.CqlRequests).GetValue().Histogram.Max, 0);
            Assert.Greater(metrics.GetSessionMeter(SessionMetric.Meters.BytesSent).GetValue().Count, 0);
            Assert.Greater(metrics.GetSessionMeter(SessionMetric.Meters.BytesReceived).GetValue().Count, 0);
            Assert.AreEqual(3, metrics.GetSessionGauge(SessionMetric.Gauges.ConnectedNodes).GetValue());
        }

        [Test]
        public void Should_DefaultMetricsHaveValidValuesAndTimersDisabled()
        {
            _metricsRoot = new MetricsBuilder().Build();
            var cluster = GetNewCluster(b => b.WithMetrics(_metricsRoot.CreateDriverMetricsProvider()));
            var session = cluster.Connect();
            Assert.AreEqual(24, NodeMetric.DefaultNodeMetrics.Count());
            Assert.IsTrue(NodeMetric.DefaultNodeMetrics.SequenceEqual(NodeMetric.DefaultNodeMetrics.Distinct()));
            Assert.AreEqual(NodeMetric.Timers.CqlMessages, NodeMetric.AllNodeMetrics.Except(NodeMetric.DefaultNodeMetrics).Single());
            Assert.IsTrue(NodeMetric.DefaultNodeMetrics.SequenceEqual(NodeMetric.DefaultNodeMetrics.Intersect(NodeMetric.AllNodeMetrics)));
            Assert.AreEqual(4, SessionMetric.DefaultSessionMetrics.Count());
            Assert.IsTrue(SessionMetric.DefaultSessionMetrics.SequenceEqual(SessionMetric.DefaultSessionMetrics.Distinct()));
            Assert.AreEqual(SessionMetric.Timers.CqlRequests,
                SessionMetric.AllSessionMetrics.Except(SessionMetric.DefaultSessionMetrics).Single());
            Assert.IsTrue(SessionMetric.DefaultSessionMetrics.SequenceEqual(
                SessionMetric.DefaultSessionMetrics.Intersect(SessionMetric.AllSessionMetrics)));

            foreach (var i in Enumerable.Range(0, 1000))
            {
                session.Execute("SELECT * FROM system.local");
            }

            var metrics = session.GetMetrics();
            foreach (var h in cluster.AllHosts())
            {
                foreach (var c in MetricsTests.Counters)
                {
                    Assert.AreEqual(0, metrics.GetNodeCounter(h, c).GetValue());
                }

                Assert.AreEqual(2, MetricsTests.Gauges.Length);
                Assert.AreEqual(0, metrics.GetNodeGauge(h, NodeMetric.Gauges.InFlight).GetValue());
                Assert.Greater(metrics.GetNodeGauge(h, NodeMetric.Gauges.OpenConnections).GetValue(), 0);

                Assert.Throws<ArgumentException>(() => metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages));
                Assert.Greater(metrics.GetNodeMeter(h, NodeMetric.Meters.BytesSent).GetValue().Count, 0);
                Assert.Greater(metrics.GetNodeMeter(h, NodeMetric.Meters.BytesReceived).GetValue().Count, 0);
            }

            Assert.AreEqual(0, metrics.GetSessionCounter(SessionMetric.Counters.CqlClientTimeouts).GetValue());
            Assert.Throws<ArgumentException>(() => metrics.GetSessionTimer(SessionMetric.Timers.CqlRequests));
            Assert.Greater(metrics.GetSessionMeter(SessionMetric.Meters.BytesSent).GetValue().Count, 0);
            Assert.Greater(metrics.GetSessionMeter(SessionMetric.Meters.BytesReceived).GetValue().Count, 0);
            Assert.AreEqual(3, metrics.GetSessionGauge(SessionMetric.Gauges.ConnectedNodes).GetValue());
        }

        [Test]
        public void Should_AllMetricsHaveValidValues_When_NodeIsDown()
        {
            _metricsRoot = new MetricsBuilder().Build();
            TestCluster.Stop(2);
            try
            {
                var cluster = GetNewCluster(b => b.WithMetrics(
                    _metricsRoot.CreateDriverMetricsProvider(),
                    new DriverMetricsOptions()
                        .SetEnabledSessionMetrics(SessionMetric.AllSessionMetrics)
                        .SetEnabledNodeMetrics(NodeMetric.AllNodeMetrics)));

                var session = cluster.Connect();

                foreach (var i in Enumerable.Range(0, 1000))
                {
                    session.Execute("SELECT * FROM system.local");
                }

                var metrics = session.GetMetrics();
                var downNode = session.Cluster.AllHosts().Single(h => h.Address.Address.ToString() == (TestCluster.ClusterIpPrefix + "2"));
                foreach (var h in cluster.AllHosts())
                {
                    foreach (var c in MetricsTests.Counters)
                    {
                        if (h.Address.Equals(downNode.Address))
                        {
                            Assert.GreaterOrEqual(metrics.GetNodeCounter(h, c).GetValue(), 0);
                        }
                        else
                        {
                            Assert.AreEqual(0, metrics.GetNodeCounter(h, c).GetValue());
                        }
                    }

                    Assert.AreEqual(2, MetricsTests.Gauges.Length);
                    Assert.AreEqual(0, metrics.GetNodeGauge(h, NodeMetric.Gauges.InFlight).GetValue());

                    if (h.Address.Equals(downNode.Address))
                    {
                        Assert.AreEqual(0, metrics.GetNodeGauge(h, NodeMetric.Gauges.OpenConnections).GetValue());
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Max);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Median);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Min);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Mean);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.StdDev);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Sum);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.SampleSize, 0);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.LastValue);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile999);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile75);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile95);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile98);
                        Assert.AreEqual(0, metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile99);
                        Assert.AreEqual(0, metrics.GetNodeMeter(h, NodeMetric.Meters.BytesSent).GetValue().Count);
                        Assert.AreEqual(0, metrics.GetNodeMeter(h, NodeMetric.Meters.BytesReceived).GetValue().Count);
                    }
                    else
                    {
                        Assert.Greater(metrics.GetNodeGauge(h, NodeMetric.Gauges.OpenConnections).GetValue(), 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Max, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Median, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Min, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Mean, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.StdDev, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Sum, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.SampleSize, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.LastValue, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile999, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile75, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile95, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile98, 0);
                        Assert.Greater(metrics.GetNodeTimer(h, NodeMetric.Timers.CqlMessages).GetValue().Histogram.Percentile99, 0);
                        Assert.Greater(metrics.GetNodeMeter(h, NodeMetric.Meters.BytesSent).GetValue().Count, 0);
                        Assert.Greater(metrics.GetNodeMeter(h, NodeMetric.Meters.BytesReceived).GetValue().Count, 0);
                    }
                }

                Assert.AreEqual(0, metrics.GetSessionCounter(SessionMetric.Counters.CqlClientTimeouts).GetValue());
                Assert.Greater(metrics.GetSessionTimer(SessionMetric.Timers.CqlRequests).GetValue().Histogram.Max, 0);
                Assert.Greater(metrics.GetSessionMeter(SessionMetric.Meters.BytesSent).GetValue().Count, 0);
                Assert.Greater(metrics.GetSessionMeter(SessionMetric.Meters.BytesReceived).GetValue().Count, 0);
                Assert.AreEqual(2, metrics.GetSessionGauge(SessionMetric.Gauges.ConnectedNodes).GetValue());
            }
            finally
            {
                TestCluster.Start(2);
            }
        }
    }
}

#endif