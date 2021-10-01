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
using System.Security.Cryptography.X509Certificates;

namespace Cassandra.DataStax.Cloud
{
    internal class SecureConnectionBundle : IEquatable<SecureConnectionBundle>
    {
        public SecureConnectionBundle(X509Certificate2 caCert, X509Certificate2 clientCert, CloudConfiguration config)
        {
            CaCert = caCert;
            ClientCert = clientCert;
            Config = config;
        }

        public X509Certificate2 CaCert { get; }

        public X509Certificate2 ClientCert { get; }

        public CloudConfiguration Config { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as SecureConnectionBundle);
        }

        public bool Equals(SecureConnectionBundle other)
        {
            return other != null &&
                   EqualityComparer<X509Certificate2>.Default.Equals(CaCert, other.CaCert) &&
                   EqualityComparer<X509Certificate2>.Default.Equals(ClientCert, other.ClientCert) &&
                   EqualityComparer<CloudConfiguration>.Default.Equals(Config, other.Config);
        }

        public override int GetHashCode()
        {
            int hashCode = 899271478;
            hashCode = hashCode * -1521134295 + EqualityComparer<X509Certificate2>.Default.GetHashCode(CaCert);
            hashCode = hashCode * -1521134295 + EqualityComparer<X509Certificate2>.Default.GetHashCode(ClientCert);
            hashCode = hashCode * -1521134295 + EqualityComparer<CloudConfiguration>.Default.GetHashCode(Config);
            return hashCode;
        }

        public static bool operator ==(SecureConnectionBundle left, SecureConnectionBundle right)
        {
            return EqualityComparer<SecureConnectionBundle>.Default.Equals(left, right);
        }

        public static bool operator !=(SecureConnectionBundle left, SecureConnectionBundle right)
        {
            return !(left == right);
        }
    }
}