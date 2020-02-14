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

namespace Cassandra.DataStax.Cloud
{
    /// <summary>
    /// Validates a certificate chain using a specific root CA. Also validates that the server certificate has a specific CN.
    /// </summary>
    internal class CustomCaCertificateValidator : ICertificateValidator
    {
        private static readonly Logger Logger = new Logger(typeof(CustomCaCertificateValidator));
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

            if ((errors & SslPolicyErrors.RemoteCertificateNotAvailable) != 0)
            {
                CustomCaCertificateValidator.Logger.Error("SSL validation failed due to SslPolicyErrors.RemoteCertificateNotAvailable.");
                return false;
            }

            // validate server certificate's CN against the provided hostname
            if ((errors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
            {
                var cert2 = new X509Certificate2(cert);
                var cn = cert2.GetNameInfo(X509NameType.SimpleName, false);

#if NET452
                cert2.Reset();
#else
                cert2.Dispose();
#endif

                if (cn != _hostname)
                {
                    CustomCaCertificateValidator.Logger.Error(
                        "Failed to validate the server certificate's CN. Expected {0} but found {1}.", 
                        _hostname, 
                        cn ?? "null");
                    return false;
                }
            }

            if ((errors & SslPolicyErrors.RemoteCertificateChainErrors) != 0)
            {
                // verify if the chain is correct
                foreach (var status in chain.ChainStatus)
                {
                    if (status.Status == X509ChainStatusFlags.NoError || status.Status == X509ChainStatusFlags.UntrustedRoot)
                    {
                        //Acceptable Status
                    }
                    else
                    {
                        CustomCaCertificateValidator.Logger.Error(
                            "Certificate chain validation failed. Found chain status {0} ({1}).", status.Status, status.StatusInformation);
                        return false;
                    }
                }

                //Now that we have tested to see if the cert builds properly, we now will check if the thumbprint of the root ca matches our trusted one
                var rootCertThumbprint = chain.ChainElements[chain.ChainElements.Count - 1].Certificate.Thumbprint;
                if (rootCertThumbprint != _trustedRootCertificateAuthority.Thumbprint)
                {
                    CustomCaCertificateValidator.Logger.Error(
                        "Root certificate thumbprint mismatch. Expected {0} but found {1}.", _trustedRootCertificateAuthority.Thumbprint, rootCertThumbprint);
                    return false;
                }
            }
            
            return true;
        }
    }
}