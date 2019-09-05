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

using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Dse.Cloud
{
    /// <summary>
    /// Validates a certificate chain using a specific root CA.
    /// </summary>
    internal class CustomCaCertificateValidator : ICertificateValidator
    {
        private readonly X509Certificate2 _trustedRootCertificateAuthority;

        public CustomCaCertificateValidator(X509Certificate2 trustedRootCertificateAuthority)
        {
            _trustedRootCertificateAuthority = trustedRootCertificateAuthority;
        }

        public bool Validate(X509Certificate cert, X509Chain chain, SslPolicyErrors errors)
        {
            if (errors == SslPolicyErrors.None)
            {
                return true;
            }

            // verify if the chain is correct
            foreach (var status in chain.ChainStatus)
            {
                if (status.Status == X509ChainStatusFlags.NoError || status.Status == X509ChainStatusFlags.UntrustedRoot)
                {
                    //Acceptable Status
                }
                else
                {
                    return false;
                }
            }

            //Now that we have tested to see if the cert builds properly, we now will check if the thumbprint of the root ca matches our trusted one
            if (chain.ChainElements[chain.ChainElements.Count - 1].Certificate.Thumbprint != _trustedRootCertificateAuthority.Thumbprint)
            {
                return false;
            }

            return true;
        }
    }
}