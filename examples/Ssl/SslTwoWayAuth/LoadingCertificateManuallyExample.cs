//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Cassandra;

namespace SslTwoWayAuth
{
    /// <summary>
    /// To enable mutual authentication, the client application must specify the CA and the client certificate when configuring the Builder.
    /// </summary>
    public static class LoadingCertificateManuallyExample
    {
        // Set these constants accordingly
        private static readonly string[] ContactPoints = { "127.0.0.1" };
        private const string LocalDatacenter = "datacenter1";

        // ## Client Certificate ##
        //
        // The X509Certificate2 API only provides a way to load single file certificates. Certificates with a separate pem file 
        // for its private key must be converted to a single file format like PKCS12/PFX.
        //
        // if you need to do this conversion you can use openssl:
        // openssl pkcs12 -export -in client_cert.pem -inkey client_key.pem -out client_cert.pfx -passout pass:123
        //
        // The command above converts the certificate into the PKCS12 format with '123' as its password.
        //
        // This is for demonstration purpose only, a stronger password should be used otherwise.
        private const string ClientCertificatePath = @"C:\path\to\client_cert.pfx";
        private const string ClientCertificatePassword = @"123"; // only necessary if the certificate is password protected

        private const string CertificateAuthorityPath = @"C:\path\to\ca.crt";

        public static void Run()
        {
            // validator that accepts certificates with an untrusted root CA, as long as that CA matches the one we provide
            var serverCertificateValidator = new CustomRootCaCertificateValidator(
                new X509Certificate2(LoadingCertificateManuallyExample.CertificateAuthorityPath));

            var cluster = Cluster.Builder()
                .AddContactPoints(LoadingCertificateManuallyExample.ContactPoints)
                .WithLocalDatacenter(LoadingCertificateManuallyExample.LocalDatacenter)
                .WithSSL(new SSLOptions()
                    // set client certificate collection
                    .SetCertificateCollection(new X509Certificate2Collection
                    {
                        // use the following constructor if the certificate is password protected
                        new X509Certificate2(
                            LoadingCertificateManuallyExample.ClientCertificatePath,
                            LoadingCertificateManuallyExample.ClientCertificatePassword),
                        // use the following constructor if the certificate is not password protected
                        //new X509Certificate2(LoadingCertificateManuallyExample.CertificatePath)
                    })
                    // Set server certificate validator for server auth
                    .SetRemoteCertValidationCallback(
                        (sender, certificate, chain, errors) => serverCertificateValidator.Validate(certificate, chain, errors)))
                .Build();

            var session = cluster.Connect();
            
            var rowSet = session.Execute("select * from system_schema.keyspaces");
            Console.WriteLine(string.Join(Environment.NewLine, rowSet.Select(row => row.GetValue<string>("keyspace_name"))));
        }

        /// <summary>
        /// Validates a certificate chain using a specific root CA.
        /// </summary>
        private class CustomRootCaCertificateValidator
        {
            private readonly X509Certificate2 _trustedRootCertificateAuthority;

            public CustomRootCaCertificateValidator(X509Certificate2 trustedRootCertificateAuthority)
            {
                _trustedRootCertificateAuthority = trustedRootCertificateAuthority;
            }

            public bool Validate(X509Certificate cert, X509Chain chain, SslPolicyErrors errors)
            {
                if (errors == SslPolicyErrors.None)
                {
                    return true;
                }

                if ((errors & SslPolicyErrors.RemoteCertificateNotAvailable) != 0)
                {
                    Console.WriteLine("SSL validation failed due to SslPolicyErrors.RemoteCertificateNotAvailable.");
                    return false;
                }

                if ((errors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
                {
                    Console.WriteLine("SSL validation failed due to SslPolicyErrors.RemoteCertificateNameMismatch.");
                    return false;
                }

                if ((errors & SslPolicyErrors.RemoteCertificateChainErrors) != 0)
                {
                    // verify if the chain is correct
                    foreach (var status in chain.ChainStatus)
                    {
                        if (status.Status == X509ChainStatusFlags.NoError ||
                            status.Status == X509ChainStatusFlags.UntrustedRoot)
                        {
                            //Acceptable Status
                        }
                        else
                        {
                            Console.WriteLine(
                                "Certificate chain validation failed. Found chain status {0} ({1}).", status.Status,
                                status.StatusInformation);
                            return false;
                        }
                    }

                    //Now that we have tested to see if the cert builds properly, we now will check if the thumbprint
                    //of the root ca matches our trusted one
                    var rootCertThumbprint = chain.ChainElements[chain.ChainElements.Count - 1].Certificate.Thumbprint;
                    if (rootCertThumbprint != _trustedRootCertificateAuthority.Thumbprint)
                    {
                        Console.WriteLine(
                            "Root certificate thumbprint mismatch. Expected {0} but found {1}.",
                            _trustedRootCertificateAuthority.Thumbprint, rootCertThumbprint);
                        return false;
                    }
                }

                return true;
            }
        }
    }
}