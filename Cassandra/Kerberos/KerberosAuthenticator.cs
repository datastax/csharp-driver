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

        public KerberosAuthenticator(string principal, NetworkCredential credentials)
        {
            //KerberosSecurityTokenProvider _oProvider;

            //_oProvider = new KerberosSecurityTokenProvider("COGSERVER01", TokenImpersonationLevel.Identification, 
            //    new NetworkCredential(@"LinuxText1", "zaq12WSX", "COGNET.COGNITUM.EU"));

            _sspi = new SSPIHelper(principal, credentials);
        }

        bool _continueChallenge=true;

        public byte[] InitialResponse()
        {
            byte[] token = null;
            byte[] challenge = null;
            _sspi.InitializeClient(out token, challenge, out _continueChallenge);
            return token;

            //KerberosRequestorSecurityToken oToken = (KerberosRequestorSecurityToken)_oProvider.GetToken(TimeSpan.FromDays(365));
            //var abRequest = oToken.GetRequest();
            //return abRequest;
        }

        public byte[] EvaluateChallenge(byte[] challenge)
        {
            if (_continueChallenge)
            {
                byte[] _token = null;
                _sspi.InitializeClient(out _token, challenge, out _continueChallenge);
                return _token;
            }
            else
                return null;

            //if (challenge == null || challenge.Length == 0)
            //    return null;
            //else
            //{
            //    KerberosRequestorSecurityToken oToken = (KerberosRequestorSecurityToken)_oProvider.GetToken(TimeSpan.FromDays(365));
            //    var abRequest = oToken.GetRequest();
            //    return abRequest;
            //}
            //var oReceivedToken = new KerberosReceiverSecurityToken(challenge, sId);
            //return oReceivedToken.GetRequest();
        }
    }
}