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
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Cassandra.Cloud
{
    /// <summary>
    /// Validates a certificate chain using a specific root CA. Also validates that the server certificate has a specific CN.
    /// </summary>
    internal class CustomCaCertificateValidator : ICertificateValidator
    {
        private readonly X509Certificate2 _trustedRootCertificateAuthority;
        private readonly string _hostname;

        public CustomCaCertificateValidator(X509Certificate2 trustedRootCertificateAuthority, string hostname)
        {
            _trustedRootCertificateAuthority =
                trustedRootCertificateAuthority ?? throw new ArgumentNullException(nameof(trustedRootCertificateAuthority));
            _hostname = hostname ?? throw new ArgumentNullException(nameof(hostname));
        }

        public bool Validate(X509Certificate cert, X509Chain chain, SslPolicyErrors errors)
        {
            if (errors == SslPolicyErrors.None)
            {
                return true;
            }

            // validate server certificate's CN against the provided hostname
            if ((errors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
            {

#if NETSTANDARD1_5
                var cert2 = new X509Certificate2(cert.Export(X509ContentType.Cert));
#else
                var cert2 = new X509Certificate2(cert);
#endif

                var cn = cert2.GetNameInfo(X509NameType.SimpleName, false);

#if NET45
                cert2.Reset();
#else
                cert2.Dispose();
#endif

                if (cn != _hostname)
                {
                    return false;
                }
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