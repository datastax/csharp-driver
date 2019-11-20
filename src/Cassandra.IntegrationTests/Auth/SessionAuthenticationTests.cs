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
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Auth
{
    /// <summary>
    /// Test Cassandra Authentication.
    /// </summary>
    [TestFixture, Category("short")]
    public class SessionAuthenticationTests : TestGlobals
    {
        // Test cluster objects to be shared by tests in this class only
        private Lazy<ITestCluster> _testClusterForAuthTesting;

        private Lazy<ITestCluster> _testClusterForDseAuthTesting;
        private ICluster _cluster;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            _testClusterForAuthTesting = new Lazy<ITestCluster>(() =>
            {
                var cluster = GetTestCcmClusterForAuthTests(false);
                //Wait 10 seconds as auth table needs to be created
                Thread.Sleep(10000);
                return cluster;
            });
            _testClusterForDseAuthTesting = new Lazy<ITestCluster>(() =>
            {
                var cluster = GetTestCcmClusterForAuthTests(true);
                //Wait 10 seconds as auth table needs to be created
                Thread.Sleep(10000);
                return cluster;
            });
        }

        [TearDown]
        public void TearDown()
        {
            _cluster?.Dispose();
            _cluster = null;
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            if (_testClusterForAuthTesting.IsValueCreated)
            {
                _testClusterForAuthTesting.Value.SwitchToThisCluster();
                TestClusterManager.TryRemove();
            }

            if (_testClusterForDseAuthTesting.IsValueCreated)
            {
                _testClusterForDseAuthTesting.Value.SwitchToThisCluster();
                TestClusterManager.TryRemove();
            }
        }

        private ITestCluster GetTestCcmClusterForAuthTests(bool dse)
        {
            if (dse)
            {
                return
                    TestClusterManager.CreateNew(1, new TestClusterOptions
                    {
                        DseYaml = new[] { "authentication_options.default_scheme: internal", "authentication_options.enabled: true" },
                        CassandraYaml = new[] { "authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator" },
                        JvmArgs = new[] { "-Dcassandra.superuser_setup_delay_ms=0" }
                    });
            }
            var testCluster = TestClusterManager.CreateNew(1, null, false);
            testCluster.UpdateConfig("authenticator: PasswordAuthenticator");
            testCluster.Start(new[] { "-Dcassandra.superuser_setup_delay_ms=0" });
            return testCluster;
        }

        [Test]
        public void StandardCreds_AuthSuccess()
        {
            _testClusterForAuthTesting.Value.SwitchToThisCluster();
            var builder = Cluster.Builder()
                .AddContactPoint(_testClusterForAuthTesting.Value.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            _cluster = builder.Build();

            var session = _cluster.Connect();
            var rs = session.Execute("SELECT * FROM system.local");
            Assert.Greater(rs.Count(), 0);
        }

        [Test]
        public void StandardCreds_AuthFail()
        {
            _testClusterForAuthTesting.Value.SwitchToThisCluster();
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForAuthTesting.Value.InitialContactPoint)
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
            _testClusterForAuthTesting.Value.SwitchToThisCluster();
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForAuthTesting.Value.InitialContactPoint)
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsTrue(ex.Message.Contains("requires authentication, but no authenticator found in Cluster configuration"));
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
            }
        }

        [Test, TestDseVersion(5, 0)]
        public void StandardCreds_DseAuth_AuthSuccess()
        {
            _testClusterForDseAuthTesting.Value.SwitchToThisCluster();
            var builder = Cluster.Builder()
                .AddContactPoint(_testClusterForDseAuthTesting.Value.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            _cluster = builder.Build();

            var session = _cluster.Connect();
            var rs = session.Execute("SELECT * FROM system.local");
            Assert.Greater(rs.Count(), 0);
        }

        [Test, TestDseVersion(5, 0)]
        public void StandardCreds_DseAuth_AuthFail()
        {
            _testClusterForDseAuthTesting.Value.SwitchToThisCluster();
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForDseAuthTesting.Value.InitialContactPoint)
                .WithCredentials("wrong_username", "password")
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsTrue(ex.Message.Contains("Failed to login. Please re-try."), ex.Message);
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
            }
        }

        [Test, TestDseVersion(5, 0)]
        public void StandardCreds_DseAuth_AuthOmitted()
        {
            _testClusterForDseAuthTesting.Value.SwitchToThisCluster();
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForDseAuthTesting.Value.InitialContactPoint)
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsTrue(ex.Message.Contains("requires authentication, but no authenticator found in Cluster configuration"));
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
            }
        }
    }
}