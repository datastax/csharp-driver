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
using System.Linq;
using System.Net;
using Cassandra.Connections;
using Cassandra.DataStax.Graph;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class BuilderTests : BaseUnitTest
    {
        [Test]
        public void WithConnectionStringCredentialsTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Cluster.Builder().WithConnectionString(string.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Cluster.Builder().WithConnectionString(string.Format("Contact Points={0};Username=user1", contactPoints));
            config = builder.GetConfiguration();
            //As there is no password, auth provider should be empty
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Cluster.Builder().WithConnectionString(string.Format("Contact Points={0};Username=user1;Password=P@ssword!", contactPoints));
            config = builder.GetConfiguration();
            Assert.IsInstanceOf<PlainTextAuthProvider>(config.AuthProvider);
            Assert.IsInstanceOf<SimpleAuthInfoProvider>(config.AuthInfoProvider);
        }

        [Test]
        public void WithConnectionStringPortTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Cluster.Builder().WithConnectionString(string.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.AreEqual(config.ProtocolOptions.Port, ProtocolOptions.DefaultPort);

            builder = Cluster.Builder().WithConnectionString(string.Format("Contact Points={0};Port=9000", contactPoints));
            config = builder.GetConfiguration();
            Assert.AreEqual(config.ProtocolOptions.Port, 9000);
        }

        [Test]
        public void WithConnectionStringDefaultKeyspaceTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Cluster.Builder().WithConnectionString(string.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.IsNull(config.ClientOptions.DefaultKeyspace);

            builder = Cluster.Builder().WithConnectionString(string.Format("Contact Points={0};Default Keyspace=ks1", contactPoints));
            config = builder.GetConfiguration();
            Assert.AreEqual(config.ClientOptions.DefaultKeyspace, "ks1");
        }

        [Test]
        public void WithCredentials()
        {
            var contactPoints = new string[] { "127.0.0.1", "127.0.0.2", "127.0.0.3" };
            var builder = Cluster.Builder().AddContactPoints(contactPoints);
            var config = builder.GetConfiguration();
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Cluster.Builder().AddContactPoints(contactPoints).WithCredentials("user1", "password");
            config = builder.GetConfiguration();
            Assert.IsInstanceOf<PlainTextAuthProvider>(config.AuthProvider);
            Assert.IsInstanceOf<SimpleAuthInfoProvider>(config.AuthInfoProvider);

            Exception ex = Assert.Throws<ArgumentNullException>(() =>
                Cluster.Builder().AddContactPoints(contactPoints).WithCredentials("user1", null));
            Assert.That(ex.Message, Contains.Substring("password"));

            ex = Assert.Throws<ArgumentNullException>(() =>
                Cluster.Builder().AddContactPoints(contactPoints).WithCredentials(null, null));
            Assert.That(ex.Message, Contains.Substring("username"));
        }
        
        [Test]
        public void Should_SetResolvedContactPoints_When_ClusterIsBuilt()
        {
            const string host1 = "127.0.0.1";
            const string host2 = "127.0.0.2";
            const string host3 = "localhost";
            
            var builder = Cluster.Builder().AddContactPoints(host1, host2, host3);
            var cluster = builder.Build();
            Assert.AreEqual(3, cluster.InternalRef.GetResolvedEndpoints().Count);
            CollectionAssert.AreEqual(
                new[] { new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse(host1), ProtocolOptions.DefaultPort), cluster.Configuration.ServerNameResolver, null) }, 
                cluster.InternalRef.GetResolvedEndpoints().Single(kvp => kvp.Key.StringRepresentation == host1).Value);
            CollectionAssert.AreEqual(
                new[] { new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse(host2), ProtocolOptions.DefaultPort), cluster.Configuration.ServerNameResolver, null) }, 
                cluster.InternalRef.GetResolvedEndpoints().Single(kvp => kvp.Key.StringRepresentation == host2).Value);

            var localhostAddress = new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse("127.0.0.1"), ProtocolOptions.DefaultPort), cluster.Configuration.ServerNameResolver, null);
            Assert.Contains(localhostAddress, cluster.InternalRef.GetResolvedEndpoints()
                                                     .Single(kvp => kvp.Key.StringRepresentation == host3)
                                                     .Value
                                                     .ToList());
        }

        [Test]
        public void WithMaxProtocolVersion_Sets_Configuration_MaxProtocolVersion()
        {
            var builder = Cluster.Builder()
                .AddContactPoint("192.168.1.10")
                .WithMaxProtocolVersion(100);
            var cluster = builder.Build();
            Assert.AreEqual(100, cluster.Configuration.ProtocolOptions.MaxProtocolVersion);
            builder = Cluster.Builder()
                .AddContactPoint("192.168.1.10")
                .WithMaxProtocolVersion(3);
            cluster = builder.Build();
            Assert.AreEqual(ProtocolVersion.V3, cluster.Configuration.ProtocolOptions.MaxProtocolVersionValue);
            builder = Cluster.Builder()
                .AddContactPoint("192.168.1.10")
                .WithMaxProtocolVersion(ProtocolVersion.V2);
            cluster = builder.Build();
            Assert.AreEqual(ProtocolVersion.V2, cluster.Configuration.ProtocolOptions.MaxProtocolVersionValue);
        }

        [Test]
        public void MaxProtocolVersion_Defaults_To_Cluster_Max()
        {
            var builder = Cluster.Builder()
                .AddContactPoint("192.168.1.10");
            var cluster = builder.Build();
            Assert.AreEqual(Cluster.MaxProtocolVersion, cluster.Configuration.ProtocolOptions.MaxProtocolVersion);
            //Defaults to null
            Assert.Null(new ProtocolOptions().MaxProtocolVersion);
        }

        [Test]
        public void WithMaxProtocolVersion_Validates_Greater_Than_Zero()
        {
            Assert.Throws<ArgumentException>(() => Cluster.Builder()
                .AddContactPoint("192.168.1.10")
                .WithMaxProtocolVersion((byte)0));
        }

        [Test]
        [TestCase(ProtocolVersion.MaxSupported, 1, 2)]
        [TestCase(ProtocolVersion.V2, 2, 8)]
        public void PoolingOptions_Create_Based_On_Protocol_Version(ProtocolVersion protocolVersion,
            int coreConnections, int maxConnections)
        {
            var options1 = PoolingOptions.Create(protocolVersion);
            var cluster1 = Cluster.Builder()
                                  .AddContactPoint("::1")
                                  .WithPoolingOptions(options1)
                                  .Build();
            Assert.AreEqual(coreConnections, cluster1.Configuration.PoolingOptions.GetCoreConnectionsPerHost(HostDistance.Local));
            Assert.AreEqual(maxConnections, cluster1.Configuration.PoolingOptions.GetMaxConnectionPerHost(HostDistance.Local));
        }

        [Test]
        public void Cluster_Builder_Should_Throw_When_No_Contact_Points_Have_Been_Defined()
        {
            var ex = Assert.Throws<ArgumentException>(() => Cluster.Builder().Build());
            Assert.That(ex.Message, Is.EqualTo("Cannot build a cluster without contact points"));
        }

        [Test]
        public void Builder_Build_Throws_When_Name_Could_Not_Be_Resolved()
        {
            const string hostName = "not-a-host";
            var builder = Cluster.Builder().AddContactPoint(hostName);
            var ex = Assert.Throws<NoHostAvailableException>(() => builder.Build());
            Assert.That(ex.Message, Does.StartWith("No host name could be resolved"));
        }

        [Test]
        public void Should_Throw_When_All_Contact_Points_Cant_Be_Resolved()
        {
            var ex = Assert.Throws<NoHostAvailableException>(() => Cluster.Builder()
                .AddContactPoint("not-a-host")
                .AddContactPoint("not-a-host2")
                .Build());
            Assert.That(ex.Message, Is.EqualTo("No host name could be resolved, attempted: not-a-host, not-a-host2"));
        }

        [Test]
        public void Cluster_Builder_Should_Use_Provided_Port()
        {
            const int port = 9099;
            var endpoint = new IPEndPoint(IPAddress.Parse("127.0.0.2"), port);

            // Provided as string
            using (var cluster = Cluster.Builder().AddContactPoint(endpoint.Address.ToString()).WithPort(port).Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.That(ex.Errors.Count, Is.EqualTo(1));
                Assert.That(ex.Errors.Keys.First(), Is.EqualTo(endpoint));
            }

            // Provided as an IPAddress
            using (var cluster = Cluster.Builder().AddContactPoint(endpoint.Address).WithPort(port).Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.That(ex.Errors.Count, Is.EqualTo(1));
                Assert.That(ex.Errors.Keys.First(), Is.EqualTo(endpoint));
            }
        }

        [Test]
        public void Cluster_Builder_Returns_Contact_Points_Provided_As_IPEndPoint_Instances()
        {
            var endpoint1 = new IPEndPoint(0x7000001L, 9042);
            var endpoint2 = new IPEndPoint(0x7000002L, 9042);
            var address = IPAddress.Parse("10.10.10.1");
            var addressString = "10.10.10.2";
            var builder = Cluster.Builder().AddContactPoint(endpoint1).AddContactPoint(address)
                                 .AddContactPoint(addressString).AddContactPoint(endpoint2);

            // Only IPEndPoint instances as IP addresses and host names must be resolved and assigned
            // the port number, which is performed on Build()
            Assert.AreEqual(new[] { endpoint1, endpoint2 }, builder.ContactPoints);
        }

        [Test]
        [TestCase(0)]
        [TestCase(-1)]
        public void Should_ThrowArgumentException_When_ProvidedMaxSchemaAgreementsWaitSecondsIsInvalid(int seconds)
        {
            var builder = Cluster.Builder();
            var ex = Assert.Throws<ArgumentException>(() => builder.WithMaxSchemaAgreementWaitSeconds(seconds));
            Assert.That(ex.Message, Is.EqualTo("Max schema agreement wait must be greater than zero"));
        }

        [Test]
        public void Should_ReturnCorrectMaxSchemaAgreementsWaitSeconds_When_ValueIsProvidedToBuilder()
        {
            var expected = 20;
            var config = Cluster.Builder()
                                 .AddContactPoint("192.168.1.10")
                                 .WithMaxSchemaAgreementWaitSeconds(expected)
                                 .GetConfiguration();
            Assert.AreEqual(expected, config.ProtocolOptions.MaxSchemaAgreementWaitSeconds);
        }

        [Test]
        public void Should_ReturnDefaultMaxSchemaAgreementWaitSeconds_When_NotProvidedToBuilder()
        {
            var expected = ProtocolOptions.DefaultMaxSchemaAgreementWaitSeconds;
            var config = Cluster.Builder()
                                 .AddContactPoint("192.168.1.10")
                                 .GetConfiguration();
            Assert.AreEqual(expected, config.ProtocolOptions.MaxSchemaAgreementWaitSeconds);
        }

        [Test]
        public void Should_ThrowException_When_ContactPointAndBundleAreProvided()
        {
            const string exceptionMsg = "Contact points can not be set when a secure connection bundle is provided.";
            var builder = Cluster.Builder()
                                .AddContactPoint("192.168.1.10")
                                .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Cluster.Builder()
                                 .AddContactPoint(IPAddress.Parse("192.168.1.10"))
                                 .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Cluster.Builder()
                             .AddContactPoint(new IPEndPoint(IPAddress.Parse("192.168.1.10"), 9042))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Cluster.Builder()
                             .AddContactPoints(new IPEndPoint(IPAddress.Parse("192.168.1.10"), 9042))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = Cluster.Builder()
                             .AddContactPoint(IPAddress.Parse("192.168.1.10"))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = Cluster.Builder()
                             .WithCloudSecureConnectionBundle("bundle")
                             .AddContactPoint(IPAddress.Parse("192.168.1.10"));
            
            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_SslOptionsAndBundleAreProvided()
        {
            const string exceptionMsg = "SSL options can not be set when a secure connection bundle is provided.";
            var builder = Cluster.Builder()
                                .WithSSL()
                                .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Cluster.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Cluster.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Cluster.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = Cluster.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = Cluster.Builder()
                             .WithCloudSecureConnectionBundle("bundle")
                             .WithSSL();
            
            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_SslOptionsAndContactPointAndBundleAreProvided()
        {
            const string exceptionMsg = "SSL options can not be set when a secure connection bundle is provided.";
            var builder = Cluster.Builder()
                                 .AddContactPoints("127.0.0.1")
                                 .WithSSL()
                                 .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Cluster.Builder()
                             .WithSSL()
                             .AddContactPoints("127.0.0.1")
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }

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