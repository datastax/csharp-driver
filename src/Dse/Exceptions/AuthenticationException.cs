//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  Indicates an error during the authentication phase while connecting to a node.
    /// </summary>
    public class AuthenticationException : DriverException
    {
        /// <summary>
        ///  Gets the host for which the authentication failed. 
        /// </summary>
        public IPEndPoint Host { get; private set; }

        public AuthenticationException(string message)
            : base(message)
        {
        }

        public AuthenticationException(string message, IPEndPoint host)
            : base(string.Format("Authentication error on host {0}: {1}", host, message))
        {
            Host = host;
        }
    }
}
