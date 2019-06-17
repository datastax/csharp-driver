//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Net;
using System.Text;

namespace Dse
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
        private class PlainTextAuthenticator : IAuthenticator
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
