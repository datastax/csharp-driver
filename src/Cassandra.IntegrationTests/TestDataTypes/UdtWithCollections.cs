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

namespace Cassandra.IntegrationTests.TestDataTypes
{
    internal class UdtWithCollections : IEquatable<UdtWithCollections>
    {
        public const string CreateUdtCql = "CREATE TYPE udt_collections (Id int, NullableId int, IntEnumerable list<int>, IntEnumerableSet set<int>, NullableIntEnumerable list<int>, " +
                                "NullableIntList list<int>, IntReadOnlyList list<int>, IntIList list<int>, IntList list<int>)";
        public int Id { get; set; }

        public int? NullableId { get; set; }

        public IEnumerable<int> IntEnumerable { get; set; }

        public IEnumerable<int> IntEnumerableSet { get; set; }

        public IEnumerable<int?> NullableIntEnumerable { get; set; }

        public List<int?> NullableIntList { get; set; }

        public IReadOnlyList<int> IntReadOnlyList { get; set; }

        public IList<int> IntIList { get; set; }

        public List<int> IntList { get; set; }

        public bool Equals(UdtWithCollections other)
        {
            if (object.ReferenceEquals(null, other)) return false;
            if (object.ReferenceEquals(this, other)) return true;
            return Id == other.Id
                   && NullableId == other.NullableId
                   && UdtWithCollections.CollectionEquals(IntEnumerable, other.IntEnumerable)
                   && UdtWithCollections.CollectionEquals(IntEnumerableSet, other.IntEnumerableSet)
                   && UdtWithCollections.CollectionEquals(NullableIntEnumerable, other.NullableIntEnumerable)
                   && UdtWithCollections.CollectionEquals(NullableIntList, other.NullableIntList)
                   && UdtWithCollections.CollectionEquals(IntReadOnlyList, other.IntReadOnlyList)
                   && UdtWithCollections.CollectionEquals(IntIList, other.IntIList)
                   && UdtWithCollections.CollectionEquals(IntList, other.IntList);
        }

        internal static bool CollectionEquals<T>(IEnumerable<T> list1, IEnumerable<T> list2)
        {
            if (list1 == null)
            {
                return list2 == null;
            }

            if (list2 == null)
            {
                return false;
            }

            return list1.SequenceEqual(list2);
        }

        public override bool Equals(object obj)
        {
            if (object.ReferenceEquals(null, obj)) return false;
            if (object.ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((UdtWithCollections)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Id;
                hashCode = (hashCode * 397) ^ (NullableId != null ? NullableId.Value : 0);
                hashCode = (hashCode * 397) ^ (IntEnumerable != null ? IntEnumerable.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (IntEnumerableSet != null ? IntEnumerableSet.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (NullableIntEnumerable != null ? NullableIntEnumerable.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (NullableIntList != null ? NullableIntList.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (IntReadOnlyList != null ? IntReadOnlyList.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (IntIList != null ? IntIList.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (IntList != null ? IntList.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}