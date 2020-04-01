//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Dse.Test.Unit;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    /// <summary>
    /// Test Cassandra Authentication.
    /// </summary>
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class SessionAuthenticationTests : TestGlobals
    {
        // Test cluster object to be shared by tests in this class only
        private ITestCluster _testClusterForAuthTesting;
        
        public static void RetryUntilClusterAuthHealthy(ITestCluster cluster)
        {
            using (var c = Cluster
                           .Builder()
                           .AddContactPoint(cluster.InitialContactPoint)
                           .WithAuthProvider(new PlainTextAuthProvider("wrong_username", "password"))
                           .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                           .Build())
            {
                TestHelper.RetryAssert(
                    () =>
                    {
                        var ex = Assert.Throws<NoHostAvailableException>(() => c.Connect());
                        Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
                    },
                    500,
                    300);
            }
        }

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            _testClusterForAuthTesting = GetTestCcmClusterForAuthTests();
            //Wait 10 seconds as auth table needs to be created
            Thread.Sleep(10000);
            SessionAuthenticationTests.RetryUntilClusterAuthHealthy(_testClusterForAuthTesting);
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            _testClusterForAuthTesting.Remove();
        }

        private ITestCluster GetTestCcmClusterForAuthTests()
        {
            var testCluster = TestClusterManager.CreateNew(1, null, false);
            testCluster.UpdateConfig("authenticator: PasswordAuthenticator");
            testCluster.Start(new[] { "-Dcassandra.superuser_setup_delay_ms=0" });
            return testCluster;
        }

        [Test, TestCassandraVersion(2, 0)]
        public void PlainTextAuthProvider_AuthFail()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithAuthProvider(new PlainTextAuthProvider("wrong_username", "password"))
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsTrue(TestClusterManager.CassandraVersion.CompareTo(Version.Parse("3.1")) > 0
                    ? ex.Message.Contains("Provided username wrong_username and/or password are incorrect")
                    : ex.Message.Contains("Username and/or password are incorrect"));
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void PlainTextAuthProvider_AuthSuccess()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                .Build())
            {
                var session = cluster.Connect();
                var rowSet = session.Execute("SELECT * FROM system.local");
                Assert.Greater(rowSet.Count(), 0);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void PlainTextAuthProvider_With_Name_AuthSuccess()
        {
            var testAuthProvider = new PlainTextAuthTestProvider("cassandra", "cassandra");
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithAuthProvider(testAuthProvider)
                .Build())
            {
                var session = cluster.Connect();
                Assert.True(testAuthProvider.NameBeforeNewAuthenticator);
                Assert.AreEqual("org.apache.cassandra.auth.PasswordAuthenticator", testAuthProvider.Name);
                var rowSet = session.Execute("SELECT * FROM system.local");
                Assert.Greater(rowSet.Count(), 0);
            }
        }

        [Test]
        public void StandardCreds_AuthSuccess()
        {
            Builder builder = Cluster.Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.local");
                Assert.Greater(rs.Count(), 0);
            }
        }

        [Test]
        public void StandardCreds_AuthFail()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithCredentials("wrong_username", "password")
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsTrue(TestClusterManager.CassandraVersion.CompareTo(Version.Parse("3.1")) > 0
                    ? ex.Message.Contains("Provided username wrong_username and/or password are incorrect")
                    : ex.Message.Contains("Username and/or password are incorrect"));
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
            }
        }

        [Test]
        public void StandardCreds_AuthOmitted()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsTrue(ex.Message.Contains("requires authentication, but no authenticator found in Cluster configuration"));
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
            }
        }

        private class PlainTextAuthTestProvider : IAuthProviderNamed
        {
            private readonly string _username;
            private readonly string _password;

            public string Name { get; private set; }

            public bool NameBeforeNewAuthenticator { get; private set; }

            public PlainTextAuthTestProvider(string username, string password)
            {
                _username = username;
                _password = password;
            }

            public void SetName(string name)
            {
                Name = name;
            }

            public IAuthenticator NewAuthenticator(IPEndPoint host)
            {
                NameBeforeNewAuthenticator = Name != null;
                return new PlainTextAuthenticator(_username, _password);
            }

            private class PlainTextAuthenticator : IAuthenticator
            {
                private readonly byte[] _password;
                private readonly byte[] _username;

                public PlainTextAuthenticator(string username, string password)
                {
                    _username = Encoding.UTF8.GetBytes(username);
                    _password = Encoding.UTF8.GetBytes(password);
                }

                public byte[] InitialResponse()
                {
                    var initialToken = new byte[_username.Length + _password.Length + 2];
                    initialToken[0] = 0;
                    Buffer.BlockCopy(_username, 0, initialToken, 1, _username.Length);
                    initialToken[_username.Length + 1] = 0;
                    Buffer.BlockCopy(_password, 0, initialToken, _username.Length + 2, _password.Length);
                    return initialToken;
                }

                public byte[] EvaluateChallenge(byte[] challenge)
                {
                    return null;
                }
            }
        }
    }
}
