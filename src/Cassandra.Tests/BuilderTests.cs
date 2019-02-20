using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class BuilderTests
    {
        [Test]
        public void WithConnectionStringCredentialsTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0};Username=user1", contactPoints));
            config = builder.GetConfiguration();
            //As there is no password, auth provider should be empty
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0};Username=user1;Password=P@ssword!", contactPoints));
            config = builder.GetConfiguration();
            Assert.IsInstanceOf<PlainTextAuthProvider>(config.AuthProvider);
            Assert.IsInstanceOf<SimpleAuthInfoProvider>(config.AuthInfoProvider);
        }

        [Test]
        public void WithConnectionStringPortTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.AreEqual(config.ProtocolOptions.Port, ProtocolOptions.DefaultPort);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0};Port=9000", contactPoints));
            config = builder.GetConfiguration();
            Assert.AreEqual(config.ProtocolOptions.Port, 9000);
        }

        [Test]
        public void WithConnectionStringDefaultKeyspaceTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.IsNull(config.ClientOptions.DefaultKeyspace);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0};Default Keyspace=ks1", contactPoints));
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
        public void AddContactPointsWithPortShouldHaveCorrectPort()
        {
            const string host1 = "127.0.0.1";
            const string host2 = "127.0.0.2";

            int port = new Random().Next(9000, 9999);
            var builder = Cluster.Builder().AddContactPoint(host1).WithPort(port);
            var cluster = builder.Build();
            Assert.AreEqual(cluster.AllHosts().Last().Address.Port, port);

            builder = Cluster.Builder().AddContactPoints(host1, host2).WithPort(port);
            cluster = builder.Build();
            Assert.True(cluster.AllHosts().All(h => h.Address.Port == port));
        }

        [Test]
        public void AddContactPointsWithDefaultPort()
        {
            const string host1 = "127.0.0.1";
            const string host2 = "127.0.0.2";

            var builder = Cluster.Builder().AddContactPoint(host1);
            var cluster = builder.Build();
            Assert.AreEqual(ProtocolOptions.DefaultPort, cluster.AllHosts().Last().Address.Port);

            builder = Cluster.Builder().AddContactPoints(host1, host2);
            cluster = builder.Build();
            Assert.True(cluster.AllHosts().All(h => h.Address.Port == ProtocolOptions.DefaultPort));
        }

        [Test]
        public void AddContactPointsWithPortBeforeContactPoints()
        {
            const string host1 = "127.0.0.1";
            const string host2 = "127.0.0.2";

            const int port = 9999;
            var builder = Cluster.Builder().WithPort(port).AddContactPoint(host1);
            var cluster = builder.Build();
            Assert.AreEqual(port, cluster.AllHosts().Last().Address.Port);

            builder = Cluster.Builder().WithPort(port).AddContactPoints(host1, host2);
            cluster = builder.Build();
            Assert.True(cluster.AllHosts().All(h => h.Address.Port == port));
        }
        
        [Test]
        public void Should_SetResolvedContactPoints_When_ClusterIsBuilt()
        {
            const string host1 = "127.0.0.1";
            const string host2 = "127.0.0.2";
            const string host3 = "localhost";
            
            var builder = Cluster.Builder().AddContactPoints(host1, host2, host3);
            var cluster = builder.Build();
            Assert.AreEqual(3, cluster.GetResolvedEndpoints().Count);
            CollectionAssert.AreEqual(
                new[] { new IPEndPoint(IPAddress.Parse(host1), ProtocolOptions.DefaultPort) }, 
                cluster.GetResolvedEndpoints()[host1]);
            CollectionAssert.AreEqual(
                new[] { new IPEndPoint(IPAddress.Parse(host2), ProtocolOptions.DefaultPort) }, 
                cluster.GetResolvedEndpoints()[host2]);

            var localhostAddress = new IPEndPoint(IPAddress.Parse("127.0.0.1"), ProtocolOptions.DefaultPort);
            Assert.Contains(localhostAddress, cluster.GetResolvedEndpoints()[host3].ToList());
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
    }
}