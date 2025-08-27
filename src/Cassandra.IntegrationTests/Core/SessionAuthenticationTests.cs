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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Test Cassandra Authentication.
    /// </summary>
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class SessionAuthenticationTests : TestGlobals
    {
        // Test cluster object to be shared by tests in this class only
        private ITestCluster _testClusterForAuthTesting;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _testClusterForAuthTesting = GetTestCcmClusterForAuthTests();
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
            Exception lastEx = null;
            const int maxAttempts = 50;
            const int delayPerAttemptMs = 200;
            for (var i = 0; i < maxAttempts; i++)
            {
                if (i > 0)
                {
                    Task.Delay(delayPerAttemptMs).GetAwaiter().GetResult();
                }
                try
                {
                    using (var cluster = ClusterBuilder()
                                         .AddContactPoint(testCluster.InitialContactPoint)
                                         .WithAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                                         .Build())
                    {
                        using (var session = cluster.Connect())
                        {
                            var rowSet = session.Execute("SELECT * FROM system.local WHERE key='local'");
                            if (rowSet.Count() > 0)
                            {
                                return testCluster;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    lastEx = ex;
                    if (i != maxAttempts - 1)
                    {
                        Trace.TraceError("Failure while setting up auth test, retrying: {0}", ex.ToString());
                    }
                }
            }

            throw new Exception("Failure while setting up auth test, see inner exception.", lastEx);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void PlainTextAuthProvider_AuthFail()
        {
            using (var cluster = ClusterBuilder()
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
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                                 .WithAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                                 .Build())
            {
                var session = cluster.Connect();
                var rowSet = session.Execute("SELECT * FROM system.local WHERE key='local'");
                Assert.Greater(rowSet.Count(), 0);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void PlainTextAuthProvider_With_Name_AuthSuccess()
        {
            var testAuthProvider = new PlainTextAuthTestProvider("cassandra", "cassandra");
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                                 .WithAuthProvider(testAuthProvider)
                                 .Build())
            {
                var session = cluster.Connect();
                Assert.True(testAuthProvider.NameBeforeNewAuthenticator);
                Assert.AreEqual("org.apache.cassandra.auth.PasswordAuthenticator", testAuthProvider.Name);
                var rowSet = session.Execute("SELECT * FROM system.local WHERE key='local'");
                Assert.Greater(rowSet.Count(), 0);
            }
        }

        [Test]
        public void StandardCreds_AuthSuccess()
        {
            Builder builder = ClusterBuilder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.local WHERE key='local'");
                Assert.Greater(rs.Count(), 0);
            }
        }

        [Test]
        public void StandardCreds_AuthFail()
        {
            using (var cluster = ClusterBuilder()
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
            using (var cluster = ClusterBuilder()
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
