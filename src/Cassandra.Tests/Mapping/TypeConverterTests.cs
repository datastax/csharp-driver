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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Mapping.TypeConversion;
using Cassandra.Tests.TestAttributes;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class TypeConverterTests : MappingTestBase
    {
        [Test]
        public void GetFromDbConverter_Should_Convert_From_Guid_Array_To_Sorted_Set()
        {
            TestGetFromDbConverter<IEnumerable<Guid>, SortedSet<Guid>>(new[] { Guid.NewGuid() });
        }

        [Test]
        public void GetFromDbConverter_Should_Convert_From_Guid_Array_To_List_Set()
        {
            TestGetFromDbConverter<IEnumerable<Guid>, List<Guid>>(new[] { Guid.NewGuid() });
        }

        [Test]
        public void GetFromDbConverter_Should_Convert_From_TimeUuid_Array_To_Sorted_Set()
        {
            TestGetFromDbConverter<IEnumerable<Guid>, SortedSet<TimeUuid>>(new Guid[] { TimeUuid.NewId() });
        }

        [Test]
        public void GetFromDbConverter_Should_Convert_From_TimeUuid_Array_To_Array()
        {
            TestGetFromDbConverter<IEnumerable<Guid>, TimeUuid[]>(new Guid[] { TimeUuid.NewId() });
        }

        [Test]
        public void GetFromDbConverter_Should_Convert_From_TimeUuid_Array_To_HashSet()
        {
            TestGetFromDbConverter<IEnumerable<Guid>, HashSet<TimeUuid>>(new Guid[] { TimeUuid.NewId() });
        }

        [Test]
        public void GetFromDbConverter_Should_Convert_From_Dictionary_With_TimeUuid_Keys()
        {
            var value = new SortedDictionary<Guid, string>
            {
                { TimeUuid.NewId(), "First" }
            };
            var result = TestGetFromDbConverter<IDictionary<Guid, string>, Dictionary<TimeUuid, string>>(value, false);
            Assert.AreEqual(value.First().Key.ToString(), result.First().Key.ToString());
            Assert.AreEqual(value.First().Value, result.First().Value);
        }

        [Test]
        public void GetFromDbConverter_Should_Convert_From_Dictionary_With_TimeUuid_Keys_Sorted()
        {
            var value = new SortedDictionary<Guid, string>
            {
                { TimeUuid.NewId(), "First" }
            };
            var result = TestGetFromDbConverter<IDictionary<Guid, string>, SortedDictionary<TimeUuid, string>>(value, false);
            Assert.AreEqual(value.First().Key.ToString(), result.First().Key.ToString());
            Assert.AreEqual(value.First().Value, result.First().Value);
        }

        private static object[] ListSourceData =>
            new object[]
            {
                new object[]
                {
                    (IEnumerable<int>) new List<int> {1, 2, 3},
                    new List<int> {1, 2, 3}
                },
                new object[]
                {
                    null,
                    null
                }
            };

        private static object[] ListSourceDataNullable =>
            new object[]
            {
                new object[]
                {
                    (IEnumerable<int?>) new List<int?> {1, 2, null},
                    new List<int?> {1, 2, null}
                },
                new object[]
                {
                    null,
                    null
                }
            };

        private static object[] ListSourceDataNullableToNonNullable =>
            new object[]
            {
                new object[]
                {
                    (IEnumerable<int?>) new List<int?> {1, 2, null}
                }
            };

        [Test]
        [TestCaseSourceGeneric(nameof(ListSourceData), TypeArguments = new[] { typeof(IEnumerable<int>), typeof(List<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceData), TypeArguments = new[] { typeof(IEnumerable<int>), typeof(IReadOnlyList<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceData), TypeArguments = new[] { typeof(IEnumerable<int>), typeof(IList<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceData), TypeArguments = new[] { typeof(IEnumerable<int>), typeof(ICollection<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceData), TypeArguments = new[] { typeof(IEnumerable<int>), typeof(IEnumerable<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(List<int?>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(IReadOnlyList<int?>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(IList<int?>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(ICollection<int?>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(IEnumerable<int?>) })]
        public void GetFromDbConverter_Should_Convert_Collections<TSource, TResult>(TSource src, TResult expected)
            where TSource : IEnumerable where TResult : IEnumerable
        {
            var result = TestGetFromDbConverter<TSource, TResult>(src, false);
            if (expected == null)
            {
                Assert.AreEqual(expected, result);
            }
            else
            {
                var expectedList = expected.Cast<object>().ToList();
                var resultList = result.Cast<object>().ToList();
                Assert.AreEqual(expectedList.Count, resultList.Count);
                for (var i = 0; i < resultList.Count; i++)
                {
                    Assert.AreEqual(expectedList[i], resultList[i]);
                }
            }
        }

        [Test]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullableToNonNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(List<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullableToNonNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(IReadOnlyList<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullableToNonNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(IList<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullableToNonNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(ICollection<int>) })]
        [TestCaseSourceGeneric(nameof(ListSourceDataNullableToNonNullable), TypeArguments = new[] { typeof(IEnumerable<int?>), typeof(IEnumerable<int>) })]
        public void GetFromDbConverter_Should_ThrowInvalidCastException_When_NullableCollectionToNonNullable<TSource, TResult>(TSource src)
        {
            Assert.Throws<InvalidCastException>(() => TestGetFromDbConverter<TSource, TResult>(src, false));
        }

        private static TResult TestGetFromDbConverter<TSource, TResult>(TSource value, bool compare = true)
        {
            var converter = new DefaultTypeConverter();
            var result = (Func<TSource, TResult>)converter.TryGetFromDbConverter(typeof(TSource), typeof(TResult));
            Assert.NotNull(result);
            var convertedValue = result(value);
            if (compare)
            {
                CollectionAssert.AreEqual((IEnumerable)value, (IEnumerable)convertedValue, new LooseComparer());
            }
            return convertedValue;
        }
    }

    public class LooseComparer : IComparer
    {
        public int Compare(object x, object y)
        {
            return string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
        }
    }
}
