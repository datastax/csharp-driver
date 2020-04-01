//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Linq;
using System.Net;
using System.Text;

namespace Dse.Auth
{
    /// <summary>
    /// AuthProvider that provides plain text authenticator instances for clients to connect 
    /// to DSE clusters secured with the DseAuthenticator.
    /// </summary>
    /// <example>
    /// Creating a auth-enabled Cluster instance:
    /// <code>
    /// var cluster = DseCluster.Builder()
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
            return new PlainTextAuthenticator(_name, Username, _password, _authorizationId);
        }

        /// <inheritdoc />
        public void SetName(string name)
        {
            _name = name;
        }

        private class PlainTextAuthenticator : BaseAuthenticator
        {
            private readonly byte[] _username;
            private readonly byte[] _password;
            private readonly byte[] _authorizationId = new byte[0];

            public PlainTextAuthenticator(string authenticatorName, string username, string password,
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
                return Mechanism;
            }

            protected override byte[] GetInitialServerChallenge()
            {
                return InitialServerChallenge;
            }

            public override byte[] EvaluateChallenge(byte[] challenge)
            {
                if (challenge == null || !challenge.SequenceEqual(InitialServerChallenge))
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
