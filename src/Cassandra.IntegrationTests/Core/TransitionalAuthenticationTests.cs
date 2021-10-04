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

using System.Diagnostics;
using System.Linq;

using Cassandra.DataStax.Auth;
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
    [TestDseVersion(5, 0)]
    public class TransitionalAuthenticationTests : TestGlobals
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
            testCluster.UpdateConfig("authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator");
            testCluster.UpdateDseConfig(
                "authentication_options.enabled:true",
                "authentication_options.default_scheme:internal",
                "authentication_options.transitional_mode:normal");
            testCluster.Start(new[] { "-Dcassandra.superuser_setup_delay_ms=0" });
            return testCluster;
        }

        [Test]
        public void WrongCredentials_AuthSuccess()
        {
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                                 .WithCredentials("some_username", "password")
                                 .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.local");
                Assert.Greater(rs.Count(), 0);
            }
        }

        [Test]
        public void AuthOmitted_AuthSuccess()
        {
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                                 .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.local");
                Assert.Greater(rs.Count(), 0);
            }
        }

        [Test]
        public void CorrectCredentials_AuthSuccess()
        {
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                                 .WithCredentials("cassandra", "cassandra")
                                 .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.local");
                Assert.Greater(rs.Count(), 0);
            }
        }

        [Test]
        public void WrongCredentials_DsePlainText_AuthSuccess()
        {
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                                 .WithAuthProvider(new DsePlainTextAuthProvider("some_username", "password"))
                                 .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.local");
                Assert.Greater(rs.Count(), 0);
            }
        }
    }
}