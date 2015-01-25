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
    [TestFixture]
    public class SessionAuthenticationTests : TestGlobals
    {
        // Test cluster object to be shared by tests in this class only
        private ITestCluster _testClusterForAuthTesting;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
            _testClusterForAuthTesting = GetTestCcmClusterForAuthTests();
            WaitForAuthenticatedClusterToConnect(_testClusterForAuthTesting);
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            _testClusterForAuthTesting.Remove();
        }

        private CcmCluster GetTestCcmClusterForAuthTests()
        {
            CcmCluster customTestCluster = (CcmCluster)TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, false);
            customTestCluster.CcmBridge.ExecuteCcm("updateconf \"authenticator: PasswordAuthenticator\"", 3000, true);
            customTestCluster.CcmBridge.Start();
            return customTestCluster;
        }

        [Test, TestCassandraVersion(2, 0), Category(TestCategories.CcmOnly)]
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
        [TestCassandraVersion(2, 0), Category(TestCategories.CcmOnly)]
        public void PlainTextAuthProvider_AuthSuccess()
        {
            Cluster cluster = Cluster
                .Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                .Build();
            var session = cluster.Connect();
            var rowSet = session.Execute("SELECT * FROM system.schema_keyspaces");
            Assert.Greater(rowSet.Count(), 0);
        }

        [Test, Category(TestCategories.CcmOnly)]
        public void StandardCreds_AuthSuccess()
        {
            Builder builder = Cluster.Builder()
                .AddContactPoint(_testClusterForAuthTesting.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            Cluster cluster = builder.Build();

            var session = cluster.Connect();
            var rs = session.Execute("SELECT * FROM system.schema_keyspaces");
            Assert.Greater(rs.Count(), 0);
        }

        [Test, Category(TestCategories.CcmOnly)]
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

        [Test, Category(TestCategories.CcmOnly)]
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

        [Explicit(), Ignore("Not Implemented"), Category(TestCategories.CcmOnly)]
        public void CassandraAny_Ssl()
        {
            CcmCluster testCluster = GetTestCcmClusterForAuthTests();

            var sslOptions = new SSLOptions()
                .SetRemoteCertValidationCallback((s, cert, chain, policyErrors) =>
                {
                    if (policyErrors == SslPolicyErrors.RemoteCertificateChainErrors &&
                        chain.ChainStatus.Length == 1 &&
                        chain.ChainStatus[0].Status == X509ChainStatusFlags.UntrustedRoot)
                    {
                        //self issued
                        return true;
                    }
                    return policyErrors == SslPolicyErrors.None;
                });
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra")
                .WithSSL(sslOptions)
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.schema_keyspaces");
                Assert.Greater(rs.Count(), 0);
            }
        }

        /////////////////////////////////
        /// Test Helpers
        ////////////////////////////////
        
        private static void WaitForAuthenticatedClusterToConnect(ITestCluster testCluster)
        {
            DateTime timeInTheFuture = DateTime.Now.AddSeconds(60);
            ISession session = null;
            Trace.TraceInformation("Validating that test cluster with name: " + testCluster.Name + " can be connected to ... ");

            while (DateTime.Now < timeInTheFuture && session == null)
            {
                try
                {
                    Cluster cluster = Cluster
                        .Builder()
                        .AddContactPoint(testCluster.InitialContactPoint)
                        .WithAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                        .Build();
                    session = cluster.Connect();
                }
                catch (Exception e)
                {
                    Trace.TraceInformation("Failed to connect to authenticated cluster, error msg: " + e.Message);
                    Trace.TraceInformation("Waiting 1 second then trying again ... ");
                    Thread.Sleep(1000);
                    session = null;
                }
            }
        }

    }
}
