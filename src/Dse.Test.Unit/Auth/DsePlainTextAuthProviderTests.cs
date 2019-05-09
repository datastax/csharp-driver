//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Auth;
using NUnit.Framework;

namespace Dse.Test.Unit.Auth
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
        public void Should_CreateDsePlainTextAuthProvider_When_WithCredentialsIsCalled()
        {
            var cluster = DseCluster.Builder().AddContactPoint("127.0.0.1").WithCredentials("cassandra", "cassandra").Build();
            Assert.AreEqual(typeof(DsePlainTextAuthProvider), cluster.Configuration.CassandraConfiguration.AuthProvider.GetType());
        }
    }
}
