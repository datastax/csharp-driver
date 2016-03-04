using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra;

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
        private readonly string _username;
        private readonly string _password;
        private string _name;

        /// <summary>
        /// Creates a new instance of <see cref="DsePlainTextAuthProvider"/>
        /// </summary>
        public DsePlainTextAuthProvider(string username, string password)
        {
            _username = username;
            _password = password;
        }

        /// <inheritdoc />
        public IAuthenticator NewAuthenticator(IPEndPoint host)
        {
            return new PlainTextAuthenticator(_name, _username, _password);
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

            public PlainTextAuthenticator(string authenticatorName, string username, string password) : base(authenticatorName)
            {
                _username = Encoding.UTF8.GetBytes(username);
                _password = Encoding.UTF8.GetBytes(password);
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
                var buffer = new byte[_username.Length + _password.Length + 2];
                buffer[0] = 0;
                Buffer.BlockCopy(_username, 0, buffer, 1, _username.Length);
                buffer[_username.Length + 1] = 0;
                Buffer.BlockCopy(_password, 0, buffer, _username.Length + 2, _password.Length);
                return buffer;
            }
        }
    }
}
