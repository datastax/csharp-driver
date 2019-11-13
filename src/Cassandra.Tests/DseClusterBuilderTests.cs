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
        
        [Test]
        public void Should_ThrowException_When_ContactPointAndBundleAreProvided()
        {
            const string exceptionMsg = "Contact points can not be set when a secure connection bundle is provided.";
            var builder = DseCluster.Builder()
                                .AddContactPoint("192.168.1.10")
                                .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = DseCluster.Builder()
                                 .AddContactPoint(IPAddress.Parse("192.168.1.10"))
                                 .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = DseCluster.Builder()
                             .AddContactPoint(new IPEndPoint(IPAddress.Parse("192.168.1.10"), 9042))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = DseCluster.Builder()
                             .AddContactPoints(new IPEndPoint(IPAddress.Parse("192.168.1.10"), 9042))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = DseCluster.Builder()
                             .AddContactPoint(IPAddress.Parse("192.168.1.10"))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = DseCluster.Builder()
                             .WithCloudSecureConnectionBundle("bundle")
                             .AddContactPoint(IPAddress.Parse("192.168.1.10"));
            
            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_SslOptionsAndBundleAreProvided()
        {
            const string exceptionMsg = "SSL options can not be set when a secure connection bundle is provided.";
            var builder = DseCluster.Builder()
                                .WithSSL()
                                .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = DseCluster.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = DseCluster.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = DseCluster.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = DseCluster.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = DseCluster.Builder()
                             .WithCloudSecureConnectionBundle("bundle")
                             .WithSSL();
            
            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_SslOptionsAndContactPointAndBundleAreProvided()
        {
            const string exceptionMsg = "SSL options can not be set when a secure connection bundle is provided.";
            var builder = DseCluster.Builder()
                                 .AddContactPoints("127.0.0.1")
                                 .WithSSL()
                                 .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = DseCluster.Builder()
                             .WithSSL()
                             .AddContactPoints("127.0.0.1")
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }
    }
}