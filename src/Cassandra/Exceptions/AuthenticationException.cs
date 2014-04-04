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
    ///  Indicates an error during the authentication phase while connecting to a node.
    /// </summary>
    public class AuthenticationException : DriverException
    {
        /// <summary>
        ///  Gets the host for which the authentication failed. 
        /// </summary>
        public IPAddress Host { get; private set; }

        public AuthenticationException(string message)
            : base(message)
        {
        }

        public AuthenticationException(string message, IPAddress host)
            : base(string.Format("Authentication error on host {0}: {1}", host, message))
        {
            Host = host;
        }
    }
}