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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
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
                .Setup(a => a.NewAuthenticator(It.IsAny<IPEndPoint>()))
                .Returns<IPEndPoint>(authProvider.NewAuthenticator)
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
        public void CassandraAny_WithCredentials()
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

        [Test]
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
                .WithCredentials("username", "password")
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
