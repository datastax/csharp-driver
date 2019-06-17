//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    public class DseClusterBuilderTests : BaseUnitTest
    {
        [Test]
        public void Should_Build_A_Cluster_With_Graph_Options()
        {
            var graphOptions = new GraphOptions();
            IDseCluster cluster = DseCluster.Builder()
                .WithGraphOptions(graphOptions)
                .AddContactPoint("192.168.1.159")
                .Build();
            Assert.NotNull(cluster.Configuration);
            Assert.NotNull(cluster.Configuration.CassandraConfiguration);
            Assert.AreSame(graphOptions, cluster.Configuration.GraphOptions);
        }

        [Test]
        public void Should_Build_A_Cluster_With_Default_Graph_Options()
        {
            //without specifying graph options
            IDseCluster cluster = DseCluster.Builder().AddContactPoint("192.168.1.159").Build();
            Assert.NotNull(cluster.Configuration);
            Assert.NotNull(cluster.Configuration.CassandraConfiguration);
            Assert.NotNull(cluster.Configuration.GraphOptions);
        }

        [Test]
        public void Should_Build_A_Cluster_With_DseLoadBalancingPolicy()
        {
            //without specifying load balancing policy
            IDseCluster cluster = DseCluster.Builder().AddContactPoint("192.168.1.159").Build();
            Assert.NotNull(cluster.Configuration);
            Assert.IsInstanceOf<DseLoadBalancingPolicy>(
                cluster.Configuration.CassandraConfiguration.Policies.LoadBalancingPolicy);
        }

        [Test]
        public void Should_Build_A_Cluster_With_The_Specified_LoadBalancingPolicy()
        {
            var lbp = new TestLoadBalancingPolicy();
            IDseCluster cluster = DseCluster.Builder()
                .AddContactPoint("192.168.1.159")
                .WithLoadBalancingPolicy(lbp)
                .Build();
            Assert.NotNull(cluster.Configuration);
            Assert.AreSame(lbp, cluster.Configuration.CassandraConfiguration.Policies.LoadBalancingPolicy);
        }

        [Test]
        public void Should_ReturnDefaultInsightsMonitoringEnabled_When_NotProvidedToBuilder()
        {
            const bool expected = MonitorReportingOptions.DefaultMonitorReportingEnabled;
            var cluster = DseCluster.Builder()
                                .AddContactPoint("192.168.1.10")
                                .Build();
            Assert.AreEqual(expected, cluster.Configuration.MonitorReportingOptions.MonitorReportingEnabled);
            Assert.AreEqual(MonitorReportingOptions.DefaultStatusEventDelayMilliseconds, cluster.Configuration.MonitorReportingOptions.StatusEventDelayMilliseconds);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ReturnProvidedInsightsMonitoringEnabledFlag_When_ProvidedToBuilder(bool enabled)
        {
            var cluster = DseCluster.Builder()
                                .AddContactPoint("192.168.1.10")
                                .WithMonitorReporting(enabled)
                                .Build();
            Assert.AreEqual(enabled, cluster.Configuration.MonitorReportingOptions.MonitorReportingEnabled);
        }
    }
}