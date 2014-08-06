using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using Moq;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Test authentication on different versions of Cassandra using different configurations.
    /// For now, this Test Fixture must be run manually as there is no easy way to create Cassandra clusters with this type of configurations.
    /// </summary>
    [TestFixture]
    [Explicit] // <- That is why it is explicit!
    public class SessionAuthenticationTests
    {
        private string ContactPoint
        {
            get { return MyTestOptions.Default.IpPrefix + "1"; }
        }

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        [Test]
        public void Cassandra20OrAbove_PasswordAuthenticator()
        {
            var authProvider = new PlainTextAuthProvider("username", "password");
            var authProvideMockWrapper = new Mock<IAuthProvider>();
            authProvideMockWrapper
                .Setup(a => a.NewAuthenticator(It.IsAny<IPAddress>()))
                .Returns<IPAddress>(authProvider.NewAuthenticator)
                .Verifiable();
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(ContactPoint)
                .WithAuthProvider(authProvideMockWrapper.Object)
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.schema_keyspaces");
                Assert.Greater(rs.Count(), 0);
            }

            authProvideMockWrapper.Verify();
        }

        [Test]
        public void Cassandra20OrAbove_WithCredentials()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(ContactPoint)
                .WithCredentials("username", "password")
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.schema_keyspaces");
                Assert.Greater(rs.Count(), 0);
            }
        }

        [Test]
        public void Cassandra20OrAbove_PasswordAuthenticatorThrowsWhenWrongCredentials()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(ContactPoint)
                .WithAuthProvider(new PlainTextAuthProvider("wrong_username", "password"))
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
            }
        }
    }
}
