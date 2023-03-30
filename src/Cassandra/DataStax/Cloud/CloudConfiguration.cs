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

using Newtonsoft.Json;

namespace Cassandra.DataStax.Cloud
{
    [JsonObject]
    internal class CloudConfiguration : IEquatable<CloudConfiguration>
    {
        [JsonProperty("host")]
        public string Host { get; private set; }

        [JsonProperty("port")]
        public int Port { get; private set; }
        
        [JsonProperty("pfxCertPassword")]
        public string CertificatePassword { get; private set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as CloudConfiguration);
        }

        public bool Equals(CloudConfiguration other)
        {
            return other != null &&
                   Host == other.Host &&
                   Port == other.Port &&
                   CertificatePassword == other.CertificatePassword;
        }

        public override int GetHashCode()
        {
            int hashCode = 1018287022;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Host);
            hashCode = hashCode * -1521134295 + Port.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(CertificatePassword);
            return hashCode;
        }

        public static bool operator ==(CloudConfiguration left, CloudConfiguration right)
        {
            return EqualityComparer<CloudConfiguration>.Default.Equals(left, right);
        }

        public static bool operator !=(CloudConfiguration left, CloudConfiguration right)
        {
            return !(left == right);
        }
    }
}