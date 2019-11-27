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
using System.Linq;
using System.Net;
using System.Text;

namespace Cassandra.DataStax.Auth
{
    /// <summary>
    /// AuthProvider that provides plain text authenticator instances for clients to connect 
    /// to DSE clusters secured with the DseAuthenticator.
    /// </summary>
    /// <example>
    /// Creating a auth-enabled Cluster instance:
    /// <code>
    /// var cluster = Cluster.Builder()
    ///     .AddContactPoint(h1)
    ///     .WithAuthProvider(new DsePlainTextAuthProvider("user", "p@sword1"))
    ///     .Build();
    /// </code>
    /// </example>
    public class DsePlainTextAuthProvider : IAuthProviderNamed
    {
        private static readonly byte[] Mechanism = Encoding.UTF8.GetBytes("PLAIN");
        private static readonly byte[] InitialServerChallenge = Encoding.UTF8.GetBytes("PLAIN-START");
        private readonly string _password;
        private readonly string _authorizationId;
        private string _name;

        /// <summary>
        /// Creates a new instance of <see cref="DsePlainTextAuthProvider"/>.
        /// </summary>
        /// <param name="username">A not <c>null</c> string representing the username.</param>
        /// <param name="password">A not <c>null</c> string representing the username.</param>
        public DsePlainTextAuthProvider(string username, string password) : this(username, password, null)
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="DsePlainTextAuthProvider"/>, enabling proxy authentication.
        /// </summary>
        /// <param name="username">A not <c>null</c> string representing the username.</param>
        /// <param name="password">A not <c>null</c> string representing the username.</param>
        /// <param name="authorizationId">
        /// The optional authorization ID. Providing an authorization ID allows the currently authenticated user
        /// to act as a different user (a.k.a. proxy authentication).
        /// </param>
        public DsePlainTextAuthProvider(string username, string password, string authorizationId)
        {
            Username = username;
            _password = password;
            _authorizationId = authorizationId;
        }

        /// <summary>
        /// For testing purposes.
        /// </summary>
        internal string Username { get; }

        /// <inheritdoc />
        public IAuthenticator NewAuthenticator(IPEndPoint host)
        {
            return new PlainTextDseAuthenticator(_name, Username, _password, _authorizationId);
        }

        /// <inheritdoc />
        public void SetName(string name)
        {
            _name = name;
        }

        private class PlainTextDseAuthenticator : BaseDseAuthenticator
        {
            private readonly byte[] _username;
            private readonly byte[] _password;
            private readonly byte[] _authorizationId = new byte[0];

            public PlainTextDseAuthenticator(string authenticatorName, string username, string password,
                string authorizationId) : base(authenticatorName)
            {
                _username = Encoding.UTF8.GetBytes(username);
                _password = Encoding.UTF8.GetBytes(password);
                if (authorizationId != null)
                {
                    _authorizationId = Encoding.UTF8.GetBytes(authorizationId);
                }
            }

            protected override byte[] GetMechanism()
            {
                return DsePlainTextAuthProvider.Mechanism;
            }

            protected override byte[] GetInitialServerChallenge()
            {
                return DsePlainTextAuthProvider.InitialServerChallenge;
            }

            public override byte[] EvaluateChallenge(byte[] challenge)
            {
                if (challenge == null || !challenge.SequenceEqual(DsePlainTextAuthProvider.InitialServerChallenge))
                {
                    throw new AuthenticationException("Incorrect SASL challenge from server");
                }
                // The SASL plain text format is: authorizationId 0 username 0 password
                var buffer = new byte[_authorizationId.Length + _username.Length + _password.Length + 2];
                var offset = 0;
                Buffer.BlockCopy(_authorizationId, 0, buffer, offset, _authorizationId.Length);
                offset += _authorizationId.Length;
                buffer[offset++] = 0;
                Buffer.BlockCopy(_username, 0, buffer, offset, _username.Length);
                offset += _username.Length;
                buffer[offset++] = 0;
                Buffer.BlockCopy(_password, 0, buffer, offset, _password.Length);
                return buffer;
            }
        }
    }
}
