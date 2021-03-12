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

            X509Certificate2 cert2 = null;
            var valid = true;

            if ((errors & SslPolicyErrors.RemoteCertificateNotAvailable) != 0)
            {
                valid = false;
                CustomCaCertificateValidator.Logger.Error("SSL validation failed due to SslPolicyErrors.RemoteCertificateNotAvailable.");
            }

            // validate server certificate's CN against the provided hostname
            if (valid && (errors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
            {
                GetOrCreateCert2(ref cert2, cert);
                var cn = cert2.GetNameInfo(X509NameType.SimpleName, false);
                var validName = false;
                if (cn == null || _hostname == null || !cn.StartsWith("*."))
                {
                    if (cn == _hostname)
                    {
                        validName = true;
                    }
                } else if (cn.StartsWith("*."))
                {
                    cn = cn.Remove(0, 1);
                    if (_hostname.EndsWith(cn))
                    {
                        validName = true;
                    }
                }

                if (!validName)
                {
                    CustomCaCertificateValidator.Logger.Error(
                        "Failed to validate the server certificate's CN. Expected {0} but found {1}.",
                        _hostname,
                        cn ?? "null");
                }

                valid = validName;
            }
            if (valid && (errors & SslPolicyErrors.RemoteCertificateChainErrors) != 0)
            {
                var oldChain = chain;
                chain = new X509Chain();
                chain.ChainPolicy.RevocationFlag = oldChain.ChainPolicy.RevocationFlag;
                chain.ChainPolicy.VerificationFlags = oldChain.ChainPolicy.VerificationFlags;
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                if (oldChain.ChainElements.Count > 0)
                {
                    var chainElements = new X509ChainElement[oldChain.ChainElements.Count];
                    oldChain.ChainElements.CopyTo(chainElements, 0);
                    chain.ChainPolicy.ExtraStore.AddRange(chainElements
                            .Where(elem => elem.Certificate != null)
                            .Select(elem => elem.Certificate)
                            .ToArray());
                }
                chain.ChainPolicy.ExtraStore.AddRange(oldChain.ChainPolicy.ExtraStore);

                // clone CA object because on Mono it gets reset for some reason after using it to build a new chain
                var clonedCa = new X509Certificate2(_trustedRootCertificateAuthority);
                chain.ChainPolicy.ExtraStore.Add(clonedCa);
                
                GetOrCreateCert2(ref cert2, cert);
                if (!chain.Build(cert2))
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
                            valid = false;
                            break;
                        }
                    }

                    if (valid)
                    {
                        //Now that we have tested to see if the cert builds properly, we now will check if the thumbprint of the root ca matches our trusted one
                        var rootCertThumbprint = chain.ChainElements[chain.ChainElements.Count - 1].Certificate.Thumbprint;
                        if (rootCertThumbprint != _trustedRootCertificateAuthority.Thumbprint)
                        {
                            CustomCaCertificateValidator.Logger.Error(
                                "Root certificate thumbprint mismatch. Expected {0} but found {1}.", _trustedRootCertificateAuthority.Thumbprint, rootCertThumbprint);
                            valid = false;
                        }
                    }

                }
                DisposeCert2(clonedCa);
            }

            DisposeCert2(cert2);
            return valid;
        }

        private void GetOrCreateCert2(ref X509Certificate2 cert2, X509Certificate cert)
        {
            if (cert2 != null)
            {
                return;
            }

            cert2 = new X509Certificate2(cert);
        }

        private void DisposeCert2(X509Certificate2 cert2)
        {
#if NET452
            cert2?.Reset();
#else
            cert2?.Dispose();
#endif
        }
    }
}