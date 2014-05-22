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

using System;
using System.Net;
using System.Text;

namespace Cassandra
{
    /// <summary>
    ///  A simple <c>AuthProvider</c> implementation. <p> This provider allows
    ///  to programmatically define authentication information that will then apply to
    ///  all hosts. The PlainTextAuthenticator instances it returns support SASL
    ///  authentication using the PLAIN mechanism for version 2 of the CQL __native__
    ///  protocol.</p>
    /// </summary>
    public class PlainTextAuthProvider : IAuthProvider
    {
        private readonly string _password;
        private readonly string _username;

        /// <summary>
        ///  Creates a new simple authentication information provider with the supplied
        ///  credentials.
        /// </summary>
        /// <param name="username"> to use for authentication requests </param>
        /// <param name="password"> to use for authentication requests</param>
        public PlainTextAuthProvider(string username, string password)
        {
            _username = username;
            _password = password;
        }

        /// <summary>
        ///  Uses the supplied credentials and the SASL PLAIN mechanism to login to the
        ///  server.
        /// </summary>
        /// <param name="host"> the Cassandra host with which we want to authenticate
        ///  </param>
        /// 
        /// <returns>an Authenticator instance which can be used to perform
        ///  authentication negotiations on behalf of the client </returns>
        /// <throws name="SaslException"> if an unsupported SASL mechanism is supplied or
        ///  an error is encountered when initialising the authenticator</throws>
        public IAuthenticator NewAuthenticator(IPAddress host)
        {
            return new PlainTextAuthenticator(_username, _password);
        }

        /// <summary>
        ///  Simple implementation of <link>Authenticator</link> which can perform
        ///  authentication against Cassandra servers configured with
        ///  PasswordAuthenticator.
        /// </summary>
        private class PlainTextAuthenticator : IAuthenticator
        {
            private readonly byte[] password;
            private readonly byte[] username;

            public PlainTextAuthenticator(string username, string password)
            {
                this.username = Encoding.UTF8.GetBytes(username);
                this.password = Encoding.UTF8.GetBytes(password);
            }

            public byte[] InitialResponse()
            {
                var initialToken = new byte[username.Length + password.Length + 2];
                initialToken[0] = 0;
                Buffer.BlockCopy(username, 0, initialToken, 1, username.Length);
                initialToken[username.Length + 1] = 0;
                Buffer.BlockCopy(password, 0, initialToken, username.Length + 2, password.Length);
                return initialToken;
            }

            public byte[] EvaluateChallenge(byte[] challenge)
            {
                return null;
            }
        }
    }
}