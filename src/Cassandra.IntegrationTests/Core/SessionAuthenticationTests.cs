//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
﻿using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Test Cassandra Authentication.
    /// </summary>
    [TestFixture, Category("short")]
    public class SessionAuthenticationTests : TestGlobals
    {
        // Test cluster object to be shared by tests in this class only
        private ITestCluster _testClusterForAuthTesting;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            _testClusterForAuthTesting = GetTestCcmClusterForAuthTests();
            //Wait 10 seconds as auth table needs to be created
            Thread.Sleep(10000);
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            _testClusterForAuthTesting.Remove();
        }

        private ITestCluster GetTestCcmClusterForAuthTests()
        {
            var testCluster = TestClusterManager.CreateNew(1, null, false);
            testCluster.UpdateConfig("authenticator: PasswordAuthenticator");
            testCluster.Start();
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
        public void StandardCreds_AuthSuccess()
        {
            Builder builder = Cluster.Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            Cluster cluster = builder.Build();

            var session = cluster.Connect();
            var rs = session.Execute("SELECT * FROM system.local");
            Assert.Greater(rs.Count(), 0);
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
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
                Console.WriteLine(ex.Errors.First().Value);
            }
        }
    }
}
