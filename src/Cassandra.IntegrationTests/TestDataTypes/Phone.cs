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

namespace Cassandra.IntegrationTests.TestDataTypes
{
    internal class Phone : IEquatable<Phone>
    {
        public const string CreateUdtCql =
            "CREATE TYPE phone (alias text, number text, country_code int, verified_at timestamp, phone_type text)";
        
        public string Alias { get; set; }

        public string Number { get; set; }

        public int CountryCode { get; set; }

        public DateTime VerifiedAt { get; set; }

        public PhoneType PhoneType { get; set; }

        public override bool Equals(object obj)
        {
            if (object.ReferenceEquals(null, obj)) return false;
            if (object.ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals(obj as Phone);
        }

        public override int GetHashCode()
        {
            // We are not looking to use equality based on hashcode
            return base.GetHashCode();
        }

        public bool Equals(Phone other)
        {
            if (object.ReferenceEquals(null, other)) return false;
            if (object.ReferenceEquals(this, other)) return true;
            return Alias == other.Alias && Number == other.Number && CountryCode == other.CountryCode &&
                   VerifiedAt.Equals(other.VerifiedAt) && PhoneType == other.PhoneType;
        }

        public override string ToString()
        {
            return $"{nameof(Phone.Alias)}: {Alias}, {nameof(Phone.Number)}: {Number}, {nameof(Phone.CountryCode)}: {CountryCode}, " +
                   $"{nameof(Phone.VerifiedAt)}: {VerifiedAt}, {nameof(Phone.PhoneType)}: {PhoneType}";
        }
    }
}