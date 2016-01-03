using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
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
            Assert.AreEqual(String.Join(",", builder.ContactPoints.Select(i => i.Address)), contactPoints);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0};Username=user1", contactPoints));
            config = builder.GetConfiguration();
            //As there is no password, auth provider should be empty
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0};Username=user1;Password=P@ssword!", contactPoints));
            config = builder.GetConfiguration();
            Assert.IsInstanceOf<PlainTextAuthProvider>(config.AuthProvider);
            Assert.IsInstanceOf<SimpleAuthInfoProvider>(config.AuthInfoProvider);
            Assert.AreEqual(String.Join(",", builder.ContactPoints.Select(i => i.Address)), contactPoints);
        }

        [Test]
        public void WithConnectionStringPortTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.AreEqual(String.Join(",", builder.ContactPoints.Select(i => i.Address)), contactPoints);
            Assert.AreEqual(config.ProtocolOptions.Port, ProtocolOptions.DefaultPort);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points={0};Port=9000", contactPoints));
            config = builder.GetConfiguration();
            Assert.AreEqual(String.Join(",", builder.ContactPoints.Select(i => i.Address)), contactPoints);
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
            Assert.AreEqual(String.Join(",", builder.ContactPoints.Select(i => i.Address)), contactPoints);
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
            Assert.AreEqual(String.Join(",", builder.ContactPoints.Select(i => i.Address)), String.Join(",", contactPoints));

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
        public void AddContactPointsThrowsWhenNameCouldNotBeResolved()
        {
            const string hostName = "not_existent_host_100003030";
            var ex = Assert.Throws<SocketException>(() => Cluster.Builder()
                                                  .AddContactPoint(hostName)
                                                  .Build());
            Assert.AreEqual(ex.SocketErrorCode, SocketError.HostNotFound);
        }
        
        [Test]
        public void AddContactPointsWithPortShouldHaveCorrectPort()
        {
        	const string host1 = "127.0.0.1";
        	const string host2 = "127.0.0.2";
        	
        	int port = new Random().Next(9000, 9999);
        	var builder = Cluster.Builder().AddContactPoint(host1).WithPort(port);
        	var cluster = builder.Build();
        	Assert.AreEqual( cluster.AllHosts().Last().Address.Port, port);
        	
        	builder = Cluster.Builder().AddContactPoints(host1, host2).WithPort(port);
        	cluster = builder.Build();
        	Assert.True( cluster.AllHosts().All(h => h.Address.Port == port));
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
            Assert.AreEqual(3, cluster.Configuration.ProtocolOptions.MaxProtocolVersion);
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
                .WithMaxProtocolVersion(0));
        }
    }
}
