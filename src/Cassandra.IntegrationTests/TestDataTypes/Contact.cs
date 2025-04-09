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
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.TestDataTypes
{
    internal class Contact : IEquatable<Contact>
    {
        public const string CreateUdtCql =
            "CREATE TYPE contact (first_name text, last_name text, birth_date timestamp, phones set<frozen<phone>>, emails set<text>, nullable_long bigint)";

        public string FirstName { get; set; }

        public string LastName { get; set; }

        public DateTimeOffset? Birth { get; set; }

        public string NotMappedProp { get; set; }

        public IEnumerable<Phone> Phones { get; set; }

        public IEnumerable<string> Emails { get; set; }

        public long? NullableLong { get; set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as Contact);
        }

        public override int GetHashCode()
        {
            // We are not looking to use equality based on hashcode
            return base.GetHashCode();
        }

        public bool Equals(Contact other)
        {
            if (object.ReferenceEquals(null, other)) return false;
            if (object.ReferenceEquals(this, other)) return true;
            return FirstName == other.FirstName && LastName == other.LastName && Birth == other.Birth &&
                   NotMappedProp == other.NotMappedProp && TestHelper.SequenceEqual(Phones.OrderBy(p => p.Alias), other.Phones.OrderBy(p => p.Alias)) &&
                   TestHelper.SequenceEqual(Emails, other.Emails) && NullableLong == other.NullableLong;
        }

        public override string ToString()
        {
            return $"{nameof(Contact.FirstName)}: {FirstName}, {nameof(Contact.LastName)}: {LastName}, {nameof(Contact.Birth)}: {Birth}, " +
                   $"{nameof(Contact.NotMappedProp)}: {NotMappedProp}, {nameof(Contact.Phones)}: {Phones}, " +
                   $"{nameof(Contact.Emails)}: {Emails}, {nameof(Contact.NullableLong)}: {NullableLong}";
        }
    }
}