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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
﻿using System.Threading;
﻿using Moq;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Test authentication on different versions of Cassandra using different configurations.
    /// </summary>
    [TestFixture]
    public class SessionAuthenticationTests : TwoNodesClusterTest
    {
        private string ContactPoint
        {
            get { return MyTestOptions.Default.IpPrefix + "1"; }
        }

        public override void TestFixtureSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
            var ccmConfigDir = TestUtils.CreateTempDirectory();
            TestUtils.ExecuteLocalCcm("list", ccmConfigDir, 2000, true);
            var ccmCommand = String.Format("create test -v {0}", Options.Default.CASSANDRA_VERSION);
            TestUtils.ExecuteLocalCcm(ccmCommand, ccmConfigDir, 180000, true);
            TestUtils.ExecuteLocalCcm("updateconf \"authenticator: PasswordAuthenticator\"", ccmConfigDir, 3000, true);
            TestUtils.ExecuteLocalCcm("populate -n 2", ccmConfigDir, 20000, true);
            TestUtils.ExecuteLocalCcm("start", ccmConfigDir, 60000, true);
            //Cassandra takes at least 10 secs to add the default user cassandra
            Thread.Sleep(25000);
            CcmClusterInfo = new CcmClusterInfo
            {
                ConfigDir = ccmConfigDir
            };
        }

        public override void TestFixtureTearDown()
        {
            base.TestFixtureTearDown();
        }

        [Test]
        public void Cassandra20OrAbove_PasswordAuthenticatorWithWrongCredentialsThrows()
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

        [Test]
        public void Cassandra20OrAbove_PasswordAuthenticator()
        {
            var authProvider = new PlainTextAuthProvider("cassandra", "cassandra");
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
        public void CassandraAny_WithCredentials()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(ContactPoint)
                .WithCredentials("cassandra", "cassandra")
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.schema_keyspaces");
                Assert.Greater(rs.Count(), 0);
            }
        }

        [Test]
        public void CassandraAny_WithWrongCredentialsThrows()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(ContactPoint)
                .WithCredentials("wrong_username", "password")
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
            }
        }

        [Test]
        public void CassandraAny_WithNoAuthenticationThrows()
        {
            using (var cluster = Cluster
                .Builder()
                .AddContactPoint(ContactPoint)
                .Build())
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
                Console.WriteLine(ex.Errors.First().Value);
            }
        }

        [Explicit()]
        public void CassandraAny_Ssl()
        {
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
                .AddContactPoint(ContactPoint)
                .WithCredentials("cassandra", "cassandra")
                .WithSSL(sslOptions)
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.Execute("SELECT * FROM system.schema_keyspaces");
                Assert.Greater(rs.Count(), 0);
            }
        }
    }
}
