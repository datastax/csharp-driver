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
using System.Net;
using Cassandra.Graph;
using NUnit.Framework;

namespace Cassandra.Tests
{
    public class DseClusterBuilderTests : BaseUnitTest
    {
        [Test]
        public void Should_Build_A_Cluster_With_Graph_Options()
        {
            var graphOptions = new GraphOptions();
            ICluster cluster = Cluster.Builder()
                .WithGraphOptions(graphOptions)
                .AddContactPoint("192.168.1.159")
                .Build();
            Assert.NotNull(cluster.Configuration);
            Assert.AreSame(graphOptions, cluster.Configuration.GraphOptions);
        }

        [Test]
        public void Should_Build_A_Cluster_With_Default_Graph_Options()
        {
            //without specifying graph options
            ICluster cluster = Cluster.Builder().AddContactPoint("192.168.1.159").Build();
            Assert.NotNull(cluster.Configuration);
            Assert.NotNull(cluster.Configuration);
            Assert.NotNull(cluster.Configuration.GraphOptions);
        }

        [Test]
        public void Should_Build_A_Cluster_With_DefaultLoadBalancingPolicy()
        {
            //without specifying load balancing policy
            ICluster cluster = Cluster.Builder().AddContactPoint("192.168.1.159").Build();
            Assert.NotNull(cluster.Configuration);
            Assert.IsInstanceOf<DefaultLoadBalancingPolicy>(
                cluster.Configuration.Policies.LoadBalancingPolicy);
        }

        [Test]
        public void Should_Build_A_Cluster_With_The_Specified_LoadBalancingPolicy()
        {
            var lbp = new TestLoadBalancingPolicy();
            ICluster cluster = Cluster.Builder()
                .AddContactPoint("192.168.1.159")
                .WithLoadBalancingPolicy(lbp)
                .Build();
            Assert.NotNull(cluster.Configuration);
            Assert.AreSame(lbp, cluster.Configuration.Policies.LoadBalancingPolicy);
        }

        [Test]
        public void Should_ReturnDefaultInsightsMonitoringEnabled_When_NotProvidedToBuilder()
        {
            const bool expected = MonitorReportingOptions.DefaultMonitorReportingEnabled;
            var cluster = Cluster.Builder()
                                .AddContactPoint("192.168.1.10")
                                .Build();
            Assert.AreEqual(expected, cluster.Configuration.MonitorReportingOptions.MonitorReportingEnabled);
            Assert.AreEqual(MonitorReportingOptions.DefaultStatusEventDelayMilliseconds, cluster.Configuration.MonitorReportingOptions.StatusEventDelayMilliseconds);
        }
    }
}