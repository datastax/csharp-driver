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
    public class NoneAuthProvider : IAuthProvider
    {
        public static readonly NoneAuthProvider Instance = new NoneAuthProvider();

        public IAuthenticator NewAuthenticator(IPEndPoint host)
        {
            throw new AuthenticationException(
                string.Format("Host {0} requires authentication, but no authenticator found in Cluster configuration", host),
                host);
        }
    }
}