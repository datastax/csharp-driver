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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Auth;
using NUnit.Framework;

namespace Cassandra.Tests.Auth
{
    public class DsePlainTextAuthProviderTests : BaseUnitTest
    {
        private const string DseAuthenticatorName = "com.datastax.bdp.cassandra.auth.DseAuthenticator";

        [Test]
        public void Authenticator_InitialResponse_With_DseAuthenticator_Should_Return_Mechanism()
        {
            var authProvider = new DsePlainTextAuthProvider("u", "p");
            authProvider.SetName(DseAuthenticatorName);
            var authenticator = authProvider.NewAuthenticator(null);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("PLAIN"), authenticator.InitialResponse());
        }

        [Test]
        public void Authenticator_InitialResponse_With_Other_Authenticator_Should_Return_Credentials()
        {
            var authProvider = new DsePlainTextAuthProvider("u", "p");
            authProvider.SetName("org.other.authenticator");
            var authenticator = authProvider.NewAuthenticator(null);
            CollectionAssert.AreEqual(
                new byte[] { 0, Encoding.UTF8.GetBytes("u")[0], 0, Encoding.UTF8.GetBytes("p")[0] }, 
                authenticator.InitialResponse());
        }

        [Test]
        public void Should_CreatePlainTextAuthProvider_When_WithCredentialsIsCalled()
        {
            var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithCredentials("cassandra", "cassandra").Build();
            Assert.AreEqual(typeof(PlainTextAuthProvider), cluster.Configuration.AuthProvider.GetType());
        }
        
        [Test]
        public void Should_SetDsePlainTextAuthProvider_When_WithAuthProviderIsCalled()
        {
            var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithAuthProvider(new DsePlainTextAuthProvider("cassandra", "cassandra")).Build();
            Assert.AreEqual(typeof(DsePlainTextAuthProvider), cluster.Configuration.AuthProvider.GetType());
        }
    }
}
