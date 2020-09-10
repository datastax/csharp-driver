//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Net;

namespace Dse
{
    /// <summary>
    ///  A provider that provides no authentication capability. <p> This is only
    ///  useful as a placeholder when no authentication is to be used. </p>
    /// </summary>
    public class NoneAuthProvider : IAuthProviderNamed
    {
        private const string DseAuthenticator = "com.datastax.bdp.cassandra.auth.DseAuthenticator";
        public static readonly NoneAuthProvider Instance = new NoneAuthProvider();

        private volatile string _name = null;

        public IAuthenticator NewAuthenticator(IPEndPoint host)
        {
            if (_name == NoneAuthProvider.DseAuthenticator) 
            {
                // Try to use transitional mode
                return new TransitionalModePlainTextAuthenticator();
            }

            throw new AuthenticationException(
                string.Format("Host {0} requires authentication, but no authenticator found in Cluster configuration", host),
                host);
        }

        public void SetName(string name)
        {
            _name = name;
        }

        /// <summary>
        /// Dummy Authenticator that accounts for DSE authentication configured with transitional mode.
        /// In this situation, the client is allowed to connect without authentication, but DSE
        /// would still send an AUTHENTICATE response. This Authenticator handles this situation
        /// by sending back a dummy credential.
        /// </summary>
        private class TransitionalModePlainTextAuthenticator : PlainTextAuthProvider.PlainTextAuthenticator {

            public TransitionalModePlainTextAuthenticator() : base(string.Empty, string.Empty)
            {
            }
        }
    }
}