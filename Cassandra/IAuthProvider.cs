//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  Provides <link>Authenticator</link> instances for use when connecting to
    ///  Cassandra nodes. See <link>PlainTextAuthProvider</link> and
    ///  <link>SimpleAuthenticator</link> for an implementation which uses SASL PLAIN
    ///  mechanism to authenticate using username/password strings
    /// </summary>
    public interface IAuthProvider
    {
        /// <summary>
        ///  The <code>Authenticator</code> to use when connecting to <code>host</code>
        /// </summary>
        /// <param name="host"> the Cassandra host to connect to. </param>
        /// <returns>The authentication implmentation to use.</returns>
        IAuthenticator NewAuthenticator(IPAddress host);
    }

    /// <summary>
    ///  A provider that provides no authentication capability. <p> This is only
    ///  useful as a placeholder when no authentication is to be used.</p>
    /// </summary>
    public class NoneAuthProvider : IAuthProvider
    {
        public static readonly NoneAuthProvider Instance = new NoneAuthProvider();

        public IAuthenticator NewAuthenticator(IPAddress host)
        {
            throw new AuthenticationException(
                string.Format("Host {0} requires authentication, but no authenticator found in Cluster configuration", host),
                host);
        }
    }
}
