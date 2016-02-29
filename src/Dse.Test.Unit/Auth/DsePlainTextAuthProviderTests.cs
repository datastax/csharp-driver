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
    }
}
