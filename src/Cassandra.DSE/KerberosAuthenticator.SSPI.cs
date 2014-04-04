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
using System.Text;
using System;
using SSPI;
using System.Security.Principal;

namespace Cassandra
{
    /// <summary>
    ///  Responsible for authenticating with secured DSE services using Kerberos over
    ///  SSPI
    /// </summary>

    public class KerberosAuthenticator : IAuthenticator
    {
        private readonly Logger _logger = new Logger(typeof(KerberosAuthenticator));

        SSPIHelper _sspi;
        string _username;

        public KerberosAuthenticator(string hostname, NetworkCredential credential, string principal)
        {
            if (credential == null)
                _username = Environment.UserName + "@" + System.Net.NetworkInformation.IPGlobalProperties.GetIPGlobalProperties().DomainName.ToUpper();
            else
                _username = credential.UserName + "@" + credential.Domain;
            _sspi = new SSPIHelper(hostname, credential, principal);
        }

        bool _continueChallenge = true;
        bool _finalHandshake = false;

        public byte[] InitialResponse()
        {
            byte[] token = null;
            byte[] challenge = null;
            _sspi.InitializeClient(out token, challenge, out _continueChallenge);
            return token;
        }

        public byte[] EvaluateChallenge(byte[] challenge)
        {
            if (_finalHandshake)
            {
                return SASL.FinalHandshake(_sspi, challenge, _username);
            }
            else
            {
                if (_continueChallenge)
                {
                    byte[] _token = null;
                    _sspi.InitializeClient(out _token, challenge, out _continueChallenge);
                    if (_continueChallenge == false)
                    {
                        _finalHandshake = true;
                        if (_token == null) // RFC 2222 7.2.1:  Client responds with no data
                            return new byte[0];
                    }
                    return _token;
                }
                else
                {
                    throw new InvalidOperationException("Authentication already complete");
                }
            }
        }
    }
}