//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Dse.Auth;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Auth
{
    /// <summary>
    /// Test Cassandra Authentication.
    /// </summary>
    [TestFixture, Category("short")]
    public class DseSessionAuthenticationTests : TestGlobals
    {
        // Test cluster objects to be shared by tests in this class only
        private Lazy<ITestCluster> _testClusterForAuthTesting;
        private Lazy<ITestCluster> _testClusterForDseAuthTesting;

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

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            if (_testClusterForAuthTesting.IsValueCreated)
            {
                _testClusterForAuthTesting.Value.Remove();
            }
            if (_testClusterForDseAuthTesting.IsValueCreated)
            {
                _testClusterForDseAuthTesting.Value.Remove();
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
            var builder = DseCluster.Builder()
                .AddContactPoint(_testClusterForAuthTesting.Value.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            var cluster = builder.Build();

            var session = cluster.Connect();
            var rs = session.Execute("SELECT * FROM system.local");
            Assert.Greater(rs.Count(), 0);
        }

        [Test]
        public void StandardCreds_AuthFail()
        {
            _testClusterForAuthTesting.Value.SwitchToThisCluster();
            using (var cluster = DseCluster
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
            using (var cluster = DseCluster
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
            var builder = DseCluster.Builder()
                .AddContactPoint(_testClusterForDseAuthTesting.Value.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            var cluster = builder.Build();

            var session = cluster.Connect();
            var rs = session.Execute("SELECT * FROM system.local");
            Assert.Greater(rs.Count(), 0);
        }

        [Test, TestDseVersion(5, 0)]
        public void StandardCreds_DseAuth_AuthFail()
        {
            _testClusterForDseAuthTesting.Value.SwitchToThisCluster();
            using (var cluster = DseCluster
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
            using (var cluster = DseCluster
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