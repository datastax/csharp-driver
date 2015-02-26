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
        public void WithConnectionStringHeartbeatIntervalTest()
        {
            const int heartbeatInterval = 20000;
            var builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points=127.0.0.1;HeartbeatInterval={0};", heartbeatInterval));
            var config = builder.GetConfiguration();
            Assert.IsNotNull(config.PoolingOptions);
            Assert.AreEqual(heartbeatInterval, config.PoolingOptions.GetHeartBeatInterval().Value);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points=127.0.0.1;HeartbeatInterval={0};Username=user1", heartbeatInterval));
            config = builder.GetConfiguration();
            Assert.IsNotNull(config.PoolingOptions);
            Assert.AreEqual(heartbeatInterval, config.PoolingOptions.GetHeartBeatInterval().Value);

            builder = Cluster.Builder().WithConnectionString("Contact Points=127.0.0.1;Default Keyspace=ks1");
            config = builder.GetConfiguration();
            Assert.IsNull(config.PoolingOptions, "Please refactor the WithConnectionStringHeartbeatIntervalTest to check the HeartbeatInterval now that DefaultPoolingOptions is not null.");
        }

       [Test]
        public void WithConnectionStringConstantReconnectPolicyDelayTest()
        {
            const string reconnectPolicy = "CONSTANT";
            const int reconnectPolicyDelay = 20000;
            const long defaultReconnectPolicyDelay = 2000;
            var builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points=127.0.0.1;ReconnectPolicy={0};ConstantReconnectPolicyDelay={1};", reconnectPolicy, reconnectPolicyDelay));
            var config = builder.GetConfiguration();
            Assert.IsInstanceOf<ConstantReconnectionPolicy>(config.Policies.ReconnectionPolicy);
            ConstantReconnectionPolicy check = config.Policies.ReconnectionPolicy as ConstantReconnectionPolicy;
            Assert.AreEqual(reconnectPolicyDelay, check.ConstantDelayMs);

            builder = Cluster.Builder().WithConnectionString(String.Format("Contact Points=127.0.0.1;ReconnectPolicy={0};Username=user1", reconnectPolicy));
            config = builder.GetConfiguration();
            Assert.IsInstanceOf<ConstantReconnectionPolicy>(config.Policies.ReconnectionPolicy);
            check = config.Policies.ReconnectionPolicy as ConstantReconnectionPolicy;
            Assert.AreEqual(defaultReconnectPolicyDelay, check.ConstantDelayMs);

            builder = Cluster.Builder().WithConnectionString("Contact Points=127.0.0.1;ReconnectPolicy=Unknown;");
            config = builder.GetConfiguration();
            Assert.IsNotNull(config.Policies);
            Assert.IsNotInstanceOf<ConstantReconnectionPolicy>(config.Policies.ReconnectionPolicy);
        }
    }
}
