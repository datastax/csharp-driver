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
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;

namespace Cassandra.DataStax.Cloud
{
    /// <summary>
    /// Validates a certificate chain using a specific root CA. Also validates that the server certificate has a specific CN.
    /// </summary>
    internal class CustomCaCertificateValidator : ICertificateValidator
    {
        private const string SubjectAlternateNameOid = "2.5.29.17"; // Oid for the SAN extension

        private static readonly Logger Logger = new Logger(typeof(CustomCaCertificateValidator));

        private static readonly string PlatformIdentifier;
        private static readonly char PlatformDelimiter;
        private static readonly string PlatformSeparator;

        private readonly X509Certificate2 _trustedRootCertificateAuthority;
        private readonly string _hostname;

        // https://stackoverflow.com/a/59382929/10896275
        // MIT licensed
        static CustomCaCertificateValidator()
        {
            // Extracted a well-known X509Extension
            var x509ExtensionBytes = new byte[] {
                48, 36, 130, 21, 110, 111, 116, 45, 114, 101, 97, 108, 45, 115, 117, 98, 106, 101, 99,
                116, 45, 110, 97, 109, 101, 130, 11, 101, 120, 97, 109, 112, 108, 101, 46, 99, 111, 109
            };
            const string subjectName1 = "not-real-subject-name";

            var x509Extension = new X509Extension(SubjectAlternateNameOid, x509ExtensionBytes, true);
            var x509ExtensionFormattedString = x509Extension.Format(false);

            // Each OS has a different dNSName identifier and delimiter
            // On Windows, dNSName == "DNS Name" (localizable), on Linux, dNSName == "DNS"
            // e.g.,
            // Windows: x509ExtensionFormattedString is: "DNS Name=not-real-subject-name, DNS Name=example.com"
            // Linux:   x509ExtensionFormattedString is: "DNS:not-real-subject-name, DNS:example.com"
            // Parse: <identifier><delimter><value><separator(s)>

            var delimiterIndex = x509ExtensionFormattedString.IndexOf(subjectName1, StringComparison.Ordinal) - 1;
            PlatformDelimiter = x509ExtensionFormattedString[delimiterIndex];

            // Make an assumption that all characters from the the start of string to the delimiter 
            // are part of the identifier
            PlatformIdentifier = x509ExtensionFormattedString.Substring(0, delimiterIndex);

            var separatorFirstChar = delimiterIndex + subjectName1.Length + 1;
            var separatorLength = 1;
            for (var i = separatorFirstChar + 1; i < x509ExtensionFormattedString.Length; i++)
            {
                // We advance until the first character of the identifier to determine what the
                // separator is. This assumes that the identifier assumption above is correct
                if (x509ExtensionFormattedString[i] == PlatformIdentifier[0])
                {
                    break;
                }

                separatorLength++;
            }

            PlatformSeparator = x509ExtensionFormattedString.Substring(separatorFirstChar, separatorLength);
        }

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
                var subjectAlternativeNames = GetSubjectAlternativeNames(cert2).ToList();
                var names = new List<string> { cn }.Concat(subjectAlternativeNames);
                var validName = false;

                foreach (var name in names)
                {
                    validName = ValidateName(name);
                    if (validName)
                    {
                        break;
                    }
                }

                if (!validName)
                {
                    CustomCaCertificateValidator.Logger.Error(
                        "Failed to validate the server certificate's CN. Expected {0} but found CN={1} and SANs={2}.",
                        _hostname,
                        cn ?? "null", string.Join(",", subjectAlternativeNames));
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

        private bool ValidateName(string name)
        {
            if (name == null || _hostname == null || !name.StartsWith("*."))
            {
                if (name?.ToLowerInvariant() == _hostname?.ToLowerInvariant())
                {
                    return true;
                }
            }
            else if (name.StartsWith("*."))
            {
                name = name.Remove(0, 1);
                if (_hostname.EndsWith(name, StringComparison.InvariantCultureIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        private IEnumerable<string> GetSubjectAlternativeNames(X509Certificate2 cert)
        {
            // https://stackoverflow.com/a/59382929/10896275
            // MIT licensed
            return cert.Extensions
                       .Cast<X509Extension>()
                       .Where(ext => ext.Oid.Value == SubjectAlternateNameOid) // Only use SAN extensions
                       .Select(ext => new AsnEncodedData(ext.Oid, ext.RawData).Format(false)) // Decode from ASN
                       // This is dumb but AsnEncodedData.Format changes based on the platform, so our static initialization code handles making sure we parse it correctly
                       .SelectMany(text => text.Split(new[] {PlatformSeparator}, StringSplitOptions.RemoveEmptyEntries))
                       .Select(text => text.Split(PlatformDelimiter))
                       .Where(x => x[0] == PlatformIdentifier)
                       .Select(x => x[1]);
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