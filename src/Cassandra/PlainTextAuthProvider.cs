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
using System.Net;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// A simple <see cref="IAuthProvider"/> implementation. 
    /// <para>
    /// This provider allows to programmatically define authentication information that will then apply to all hosts.
    /// The PlainTextAuthenticator instances it returns support SASL authentication using the PLAIN mechanism for
    /// version 2 or above of the CQL native protocol.
    /// </para>
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
            if (string.IsNullOrEmpty(username))
            {
                throw new ArgumentNullException("username");
            }
            if (string.IsNullOrEmpty(password))
            {
                throw new ArgumentNullException("password");
            }
            _username = username;
            _password = password;
        }

        /// <summary>
        /// For testing purposes.
        /// </summary>
        internal string Username => _username;

        /// <summary>
        /// Uses the supplied credentials and the SASL PLAIN mechanism to login to the server.
        /// </summary>
        /// <param name="host"> the Cassandra host with which we want to authenticate</param>
        /// <returns>
        /// An Authenticator instance which can be used to perform authentication negotiations on behalf of the client.
        /// </returns>
        /// <throws name="SaslException"> if an unsupported SASL mechanism is supplied or
        ///  an error is encountered when initializing the authenticator</throws>
        public IAuthenticator NewAuthenticator(IPEndPoint host)
        {
            return new PlainTextAuthenticator(_username, _password);
        }

        /// <summary>
        ///  Simple implementation of <link>Authenticator</link> which can perform
        ///  authentication against Cassandra servers configured with
        ///  PasswordAuthenticator.
        /// </summary>
        internal class PlainTextAuthenticator : IAuthenticator
        {
            private readonly byte[] _password;
            private readonly byte[] _username;

            public PlainTextAuthenticator(string username, string password)
            {
                _username = Encoding.UTF8.GetBytes(username);
                _password = Encoding.UTF8.GetBytes(password);
            }

            public byte[] InitialResponse()
            {
                var initialToken = new byte[_username.Length + _password.Length + 2];
                initialToken[0] = 0;
                Buffer.BlockCopy(_username, 0, initialToken, 1, _username.Length);
                initialToken[_username.Length + 1] = 0;
                Buffer.BlockCopy(_password, 0, initialToken, _username.Length + 2, _password.Length);
                return initialToken;
            }

            public byte[] EvaluateChallenge(byte[] challenge)
            {
                return null;
            }
        }
    }
}
