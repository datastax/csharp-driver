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

using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace Cassandra.DataStax.Cloud
{
    /// <inheritdoc />
    internal class SecureConnectionBundleParser : ISecureConnectionBundleParser
    {
        private const string CaName = "ca.crt";
        private const string ConfigName = "config.json";
        private const string CertName = "cert.pfx";

        private static readonly Logger Logger = new Logger(typeof(SecureConnectionBundleParser));

        private readonly CloudConfigurationParser _configParser;

        public SecureConnectionBundleParser()
        {
            _configParser = new CloudConfigurationParser();
        }

        /// <inheritdoc />
        public SecureConnectionBundle ParseBundle(string path)
        {
            var archive = ZipFile.OpenRead(path);

            var caCert = LoadCertificateAuthority(archive);
            var config = ParseConfig(archive);
            var clientCert = LoadClientCertificate(archive, config.CertificatePassword);

            return new SecureConnectionBundle(caCert, clientCert, config);
        }

        private CloudConfiguration ParseConfig(ZipArchive archive)
        {
            var config = archive.Entries.FirstOrDefault(entry => entry.Name.Equals(SecureConnectionBundleParser.ConfigName));

            if (config == null)
            {
                throw new SecureConnectionBundleException(
                    $"Could not get {SecureConnectionBundleParser.ConfigName} from the secure connection bundle.");
            }

            return _configParser.ParseConfig(config.Open());
        }

        private X509Certificate2 LoadCertificateAuthority(ZipArchive archive)
        {
            var caEntry = archive.Entries.FirstOrDefault(entry => entry.Name.Equals(SecureConnectionBundleParser.CaName));
            if (caEntry == null)
            {
                throw new SecureConnectionBundleException(
                    $"Could not get {SecureConnectionBundleParser.CaName} from the secure connection bundle.");
            }

            X509Certificate2 caCert;
            using (var memoryStream = new MemoryStream())
            {
                using (var caStream = caEntry.Open())
                {
                    caStream.CopyTo(memoryStream);
                }

                caCert = new X509Certificate2(memoryStream.ToArray());
            }

            return caCert;
        }

        private X509Certificate2 LoadClientCertificate(ZipArchive archive, string password)
        {
            var clientCertEntry = archive.Entries.FirstOrDefault(entry => entry.Name.Equals(SecureConnectionBundleParser.CertName));
            if (clientCertEntry == null)
            {
                SecureConnectionBundleParser.Logger.Warning(
                    $"Could not get {SecureConnectionBundleParser.CertName} from the secure connection bundle. " +
                    "The driver will attempt to connect without client authentication.");
                return null;
            }

            if (password == null)
            {
                SecureConnectionBundleParser.Logger.Warning(
                    "The certificate password that was obtained from the bundle is null. " +
                    "The driver will assume that the certificate is not password protected.");
            }

            X509Certificate2 clientCert;
            using (var memoryStream = new MemoryStream())
            {
                using (var clientCertStream = clientCertEntry.Open())
                {
                    clientCertStream.CopyTo(memoryStream);
                }

                clientCert = new X509Certificate2(memoryStream.ToArray(), password);
            }
            return clientCert;
        }
    }
}