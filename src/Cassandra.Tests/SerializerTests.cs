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

using System.Collections;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Reflection;
using Cassandra.Serialization;
using Cassandra.Serialization.Primitive;

namespace Cassandra.Tests
{
    [TestFixture]
    public class SerializerTests
    {
        private readonly ProtocolVersion[] _protocolVersions =
        {
            ProtocolVersion.V1, ProtocolVersion.V2, ProtocolVersion.V3, ProtocolVersion.V4
        };

        private static readonly MapColumnInfo MapColumnInfoStringString = new MapColumnInfo() { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Text };

        [Test]
        public void EncodeDecodeSingleValuesTest()
        {

            var initialValues = new object[]
            {
                "utf8 text mañana",
                1234,
                3129L,
                1234F,
                1.14D,
                double.MinValue,
                -1.14,
                0d,
                double.MaxValue,
                double.NaN,
                1.01M,
                72.727272727272727272727272727M,
                -72.727272727272727272727272727M,
                -256M,
                256M,
                0M,
                -1.333333M,
                -256.512M,
                Decimal.MaxValue,
                Decimal.MinValue,
                new DateTimeOffset(new DateTime(2015, 10, 21)),
                new IPAddress(new byte[] { 1, 1, 5, 255}),
                true,
                new byte[] {16},
                Guid.NewGuid(),
                Guid.NewGuid()
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (var value in initialValues)
                {
                    byte[] encoded = serializer.Serialize(value);
                    Assert.AreEqual(value, serializer.Deserialize(encoded, serializer.GetCqlTypeForPrimitive(value.GetType()), null));
                }
            }
        }

        /// <summary>
        /// Tests that the default target type when is not provided
        /// </summary>
        [Test]
        public void EncodeDecodeSingleValuesDefaultsFactory()
        {
            var initialValues = new object[]
            {
                new object[] {"just utf8 text olé!", ColumnTypeCode.Text},
                new object[] {123, ColumnTypeCode.Int},
                new object[] {Int64.MinValue + 100, ColumnTypeCode.Bigint},
                new object[] {-144F, ColumnTypeCode.Float},
                new object[] {1120D, ColumnTypeCode.Double},
                new object[] {-9999.89770M, ColumnTypeCode.Decimal},
                new object[] {-256M, ColumnTypeCode.Decimal},
                new object[] {new DateTimeOffset(new DateTime(2010, 4, 29)), ColumnTypeCode.Timestamp},
                new object[] {new IPAddress(new byte[] { 10, 0, 5, 5}), ColumnTypeCode.Inet},
                new object[] {Guid.NewGuid(), ColumnTypeCode.Uuid},
                new object[] {true, ColumnTypeCode.Boolean},
                new object[] {new byte [] { 255, 128, 64, 32, 16, 9, 9}, ColumnTypeCode.Blob}
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    byte[] encoded = serializer.Serialize(value[0]);
                    //Set object as the target CSharp type, it should get the default value
                    Assert.AreEqual(value[0], serializer.Deserialize(encoded, (ColumnTypeCode)value[1], null));
                }   
            }
        }

        [Test]
        public void EncodeDecodeListSetFactoryTest()
        {
            var initialValues = new object[]
            {
                //Lists
                new object[] {new List<int>(new [] {1, 2, 1000}), ColumnTypeCode.List, new ListColumnInfo() {ValueTypeCode = ColumnTypeCode.Int}},
                new object[] {new List<double>(new [] {-1D, 2.333D, 1.2D}), ColumnTypeCode.List, new ListColumnInfo() {ValueTypeCode = ColumnTypeCode.Double}},
                new object[] {new [] {5D, 4.333D, 1.2D}, ColumnTypeCode.List, new ListColumnInfo() {ValueTypeCode = ColumnTypeCode.Double}},
                //Sets
                new object[] {new List<decimal>(new [] {-1M, 2.333M, 1.2M, 256M}), ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Decimal}},
                new object[] {new SortedSet<string>(new [] {"a", "b", "c"}), ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Text}},
                new object[] {new HashSet<string>(new [] {"ADADD", "AA", "a"}), ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Text}},
                new object[] {new [] {"ADADD", "AA", "a"}, ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Text}}
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    Assert.IsInstanceOf<Array>(decoded);
                    CollectionAssert.AreEqual(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void EncodeListSetInvalid()
        {
            var values = new object[]
            {
                //any class that is not a valid primitive
                new List<object> { new object()},
                new List<Action> {() => { }}
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (var value in values)
                {
                    Assert.Throws<InvalidTypeException>(() => serializer.Serialize(value));
                }
            }
        }

        [Test]
        public void EncodeDecodeMapFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[] {new SortedDictionary<string, string>(), ColumnTypeCode.Map, MapColumnInfoStringString},
                new object[] {new SortedDictionary<string, string>{{"key100","value100"}}, ColumnTypeCode.Map, MapColumnInfoStringString},
                new object[] {new SortedDictionary<string, string>{{"key1","value1"}, {"key2","value2"}}, ColumnTypeCode.Map, MapColumnInfoStringString},
                new object[] {new SortedDictionary<string, int>{{"key1", 1}, {"key2", 2}}, ColumnTypeCode.Map, new MapColumnInfo() {KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Int}},
                new object[] {new SortedDictionary<Guid, string>{{Guid.NewGuid(),"value1"}, {Guid.NewGuid(),"value2"}}, ColumnTypeCode.Map, new MapColumnInfo() {KeyTypeCode = ColumnTypeCode.Uuid, ValueTypeCode = ColumnTypeCode.Text}},
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var valueToEncode = (IDictionary)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IDictionary)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    CollectionAssert.AreEquivalent(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void EncodeDecodeTupleFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[] {new Tuple<string>("val1"), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}}}},
                new object[] {new Tuple<string, int>("val2", 2), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}, new ColumnDesc(){TypeCode = ColumnTypeCode.Int}}}},
                new object[] {new Tuple<string, int>(null, -1234), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}, new ColumnDesc(){TypeCode = ColumnTypeCode.Int}}}}
            };
            var serializer = NewInstance();
            foreach (object[] value in initialValues)
            {
                var valueToEncode = (IStructuralEquatable)value[0];
                var encoded = serializer.Serialize(valueToEncode);
                var decoded = (IStructuralEquatable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                Assert.AreEqual(valueToEncode, decoded);
            }
        }

        [Test]
        public void EncodeDecodeTupleAsSubtypeFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[]
                {
                    new List<Tuple<string>>{new Tuple<string>("val1")}, 
                    ColumnTypeCode.List, 
                    new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Tuple, ValueTypeInfo = new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}}}}
                },
                new object[]
                {
                    new List<Tuple<string, int>>{new Tuple<string, int>("val2ZZ", 0)}, 
                    ColumnTypeCode.List, 
                    new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Tuple, ValueTypeInfo = new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}, new ColumnDesc(){TypeCode = ColumnTypeCode.Int}}}}
                }
            };
            var serializer = NewInstance();
            foreach (object[] value in initialValues)
            {
                var valueToEncode = (IList)value[0];
                var encoded = serializer.Serialize(valueToEncode);
                var decoded = (IList)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                Assert.AreEqual(valueToEncode, decoded);
            }
        }

        [Test]
        public void Encode_Decode_Nested_List()
        {
            var initialValues = new object[]
            {
                new object[] {new IEnumerable<int>[]{new List<int>(new [] {1, 2, 1000})}, ColumnTypeCode.List, GetNestedListColumnInfo(1, ColumnTypeCode.Int)},
                new object[] {new IEnumerable<IEnumerable<int>>[]{new List<IEnumerable<int>> {new List<int>(new [] {1, 2, 1000})}}, ColumnTypeCode.List, GetNestedListColumnInfo(2, ColumnTypeCode.Int)}
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var originalType = value[0].GetType();
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    Assert.IsInstanceOf(originalType, decoded);
                    CollectionAssert.AreEqual(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void Encode_Decode_Nested_Set()
        {
            var initialValues = new object[]
            {
                new object[] {new SortedSet<IEnumerable<int>>{new SortedSet<int>(new [] {1, 2, 1000})}, ColumnTypeCode.Set, GetNestedSetColumnInfo(1, ColumnTypeCode.Int)},
                new object[] {new SortedSet<IEnumerable<IEnumerable<int>>>{new SortedSet<IEnumerable<int>> {new SortedSet<int>(new [] {1, 2, 1000})}}, ColumnTypeCode.Set, GetNestedSetColumnInfo(2, ColumnTypeCode.Int)}
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    //The return type is not respected
                    CollectionAssert.AreEqual(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void Encode_Decode_Nested_Map()
        {
            var initialValues = new object[]
            {
                new object[] {
                    new SortedDictionary<string, IEnumerable<int>>{{"first", new List<int>(new [] {1, 2, 1000})}}, 
                    ColumnTypeCode.Map,
                    new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.List, ValueTypeInfo = new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Int}}
                },
                new object[] {
                    new SortedDictionary<int, IEnumerable<string>>{{120, new SortedSet<string>(new [] {"a", "b", "c"})}}, 
                    ColumnTypeCode.Map,
                    new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Int, ValueTypeCode = ColumnTypeCode.Set, ValueTypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Text}}
                },
                new object[] {
                    new SortedDictionary<string, IDictionary<string, int>>{{"first-b", new SortedDictionary<string, int> {{"A", 1}, {"B", 2}}}}, 
                    ColumnTypeCode.Map,
                    new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Map, ValueTypeInfo = new MapColumnInfo{ KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Int}}
                }
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var originalType = value[0].GetType();
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    Assert.IsInstanceOf(originalType, decoded);
                    CollectionAssert.AreEqual(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void Encode_Decode_TinyInt()
        {
            var values = new[]
            {
                Tuple.Create<sbyte, byte>(-1, 0xff),
                Tuple.Create<sbyte, byte>(-2, 0xfe),
                Tuple.Create<sbyte, byte>(0, 0),
                Tuple.Create<sbyte, byte>(1, 1),
                Tuple.Create<sbyte, byte>(2, 2),
                Tuple.Create<sbyte, byte>(127, 127)
            };
            var serializer = NewInstance();
            foreach (var v in values)
            {
                var encoded = serializer.Serialize(v.Item1);
                CollectionAssert.AreEqual(encoded, new[] { v.Item2 });
                var decoded = (sbyte)serializer.Deserialize(encoded, ColumnTypeCode.TinyInt, null);
                Assert.AreEqual(v.Item1, decoded);
            }
        }

        [Test]
        public void Encode_Decode_Date()
        {
            var values = new[]
            {
                new LocalDate(2010, 4, 29),
                new LocalDate(2005, 8, 5),
                new LocalDate(0, 3, 12),
                new LocalDate(-10, 2, 4),
                new LocalDate(5881580, 7, 11),
                new LocalDate(-5877641, 6, 23)
            };
            var serializer = NewInstance();
            foreach (var v in values)
            {
                var encoded = serializer.Serialize(v);
                var decoded = (LocalDate)serializer.Deserialize(encoded, ColumnTypeCode.Date, null);
                Assert.AreEqual(v, decoded);
            }
        }

        [Test]
        public void Encode_Decode_SmallInt()
        {
            var serializer = NewInstance();
            for (var i = Int16.MinValue; ; i++ )
            {
                var encoded = serializer.Serialize(i);
                var decoded = (short)serializer.Deserialize(encoded, ColumnTypeCode.SmallInt, null);
                Assert.AreEqual(i, decoded);
                if (i == Int16.MaxValue)
                {
                    break;
                }
            }
        }

        [Test]
        public void Encode_Map_With_Null_Value_Throws_ArgumentNullException()
        {
            var value = new Dictionary<string, string>
            {
                {"k1", "value1"},
                {"k2", null}
            };
            var serializer = NewInstance();
            //null value within a dictionary
            var ex = Assert.Throws<ArgumentNullException>(() => serializer.Serialize(value));
            StringAssert.Contains("collections", ex.Message);
        }

        [Test]
        public void Encode_List_With_Null_Value_Throws_ArgumentNullException()
        {
            var value = new List<string>
            {
                "one",
                null,
                "two"
            };
            var serializer = NewInstance();
            //null value within a list
            var ex = Assert.Throws<ArgumentNullException>(() => serializer.Serialize(value));
            StringAssert.Contains("collections", ex.Message);
        }

        [Test]
        public void Encode_Decode_With_Binary_Representation()
        {
            var values = new[]
            {
                Tuple.Create<object, byte[]>(1D, new byte[] {0x3f, 0xf0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create<object, byte[]>(2D, new byte[] {0x40, 0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create<object, byte[]>(2.2D, new byte[] {0x40, 1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a}),
                Tuple.Create<object, byte[]>(-1D, new byte[] {0xbf, 0xf0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create<object, byte[]>(-1F, new byte[] {0xbf, 0x80, 0, 0}),
                Tuple.Create<object, byte[]>(1.3329F, new byte[] {0x3f, 0xaa, 0x9c, 0x78}),
                Tuple.Create<object, byte[]>("abc", new byte[] {0x61, 0x62, 0x63})
            };
            var serializer = NewInstance();
            foreach (var val in values)
            {
                var encoded = serializer.Serialize(val.Item1);
                CollectionAssert.AreEqual(val.Item2, encoded);
                var padEncoded = new byte[] {0xFF, 0xFA}.Concat(encoded).ToArray();
                Assert.AreEqual(val.Item1, serializer.Deserialize(padEncoded, 2, encoded.Length, serializer.GetCqlTypeForPrimitive(val.Item1.GetType()), null));
            }
        }

        [Test]
        public void GetClrType_Should_Get_Clr_Type_For_Primitive_Cql_Types()
        {
            var notPrimitive = new[] { ColumnTypeCode.List, ColumnTypeCode.Set, ColumnTypeCode.Map, ColumnTypeCode.Udt, ColumnTypeCode.Tuple, ColumnTypeCode.Custom };
            var serializer = NewInstance();
            foreach (ColumnTypeCode typeCode in Enum.GetValues(typeof(ColumnTypeCode)))
            {
                if (notPrimitive.Contains(typeCode))
                {
                    continue;
                }
                var type = serializer.GetClrType(typeCode, null);
                Assert.NotNull(type);
                if (type.GetTypeInfo().IsValueType)
                {
                    Assert.NotNull(serializer.Serialize(Activator.CreateInstance(type)));
                }
            }
        }

        [Test]
        public void GetClrType_Should_Get_Clr_Type_For_Non_Primitive_Cql_Types()
        {
            var notPrimitive = new []
            {
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(IEnumerable<string>), ColumnTypeCode.List, new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Text}),
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(IEnumerable<int>), ColumnTypeCode.Set, new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Int}),
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(IEnumerable<IEnumerable<DateTimeOffset>>), ColumnTypeCode.List, 
                    new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Set, ValueTypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Timestamp}}),
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(IDictionary<string, int>), ColumnTypeCode.Map, 
                    new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Int }),
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(Tuple<string, int, LocalDate>), ColumnTypeCode.Tuple, 
                    new TupleColumnInfo(new [] { ColumnTypeCode.Text, ColumnTypeCode.Int, ColumnTypeCode.Date}.Select(c => new ColumnDesc {TypeCode = c})))
            };
            var serializer = NewInstance();
            foreach (var item in notPrimitive)
            {
                var type = serializer.GetClrType(item.Item2, item.Item3);
                Assert.AreEqual(item.Item1, type);
            }
        }

        [Test]
        public void DecimalSerializer_ToDecimal_Converts_Test()
        {
            var values = new[]
            {
                Tuple.Create(BigInteger.Parse("1000"), 1, 100M),
                Tuple.Create(BigInteger.Parse("1000"), 0, 1000M),
                Tuple.Create(BigInteger.Parse("9223372036854776"), -1, 92233720368547760M),
                Tuple.Create(BigInteger.Parse("12345678901234567890"), 2, 123456789012345678.9M),
                Tuple.Create(BigInteger.Parse("79228162514264337593543950335"), 0, 79228162514264337593543950335M),
                Tuple.Create(BigInteger.Parse("79228162514264337593543950335"), 27, 79.228162514264337593543950335M),
                Tuple.Create(BigInteger.Parse("1"), -28, 10000000000000000000000000000m)
            };
            foreach (var v in values)
            {
                var decimalValue = DecimalSerializer.ToDecimal(v.Item1, v.Item2);
                Assert.AreEqual(v.Item3, decimalValue);
            }
        }

        [Test]
        public void DecimalSerializer_ToDecimal_Throws_OverflowException_When_Value_Can_Not_Be_Represented_Test()
        {
            var values = new[]
            {
                Tuple.Create(BigInteger.Parse("123"), -28),
                Tuple.Create(BigInteger.Parse("123"), -27),
                Tuple.Create(BigInteger.Parse("1"), 29),
                Tuple.Create(BigInteger.Parse("123456789012345678901234567890"), 0)
            };
            foreach (var v in values)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => DecimalSerializer.ToDecimal(v.Item1, v.Item2), "For value: " + v.Item1);
            }
        }

        private static Serializer NewInstance(ProtocolVersion protocolVersion = ProtocolVersion.MaxSupported)
        {
            return new Serializer(protocolVersion);
        }

        /// <summary>
        /// Helper method to generate a list column info of nested lists
        /// </summary>
        private static ListColumnInfo GetNestedListColumnInfo(int level, ColumnTypeCode singleType)
        {
            var columnInfo = new ListColumnInfo();
            if (level == 0)
            {
                columnInfo.ValueTypeCode = singleType;
                columnInfo.ValueTypeInfo = null;
            }
            else
            {
                columnInfo.ValueTypeCode = ColumnTypeCode.List;
                columnInfo.ValueTypeInfo = GetNestedListColumnInfo(level - 1, singleType);
            }
            return columnInfo;
        }

        /// <summary>
        /// Helper method to generate a set column info of nested sets
        /// </summary>
        private static SetColumnInfo GetNestedSetColumnInfo(int level, ColumnTypeCode singleType)
        {
            var columnInfo = new SetColumnInfo();
            if (level == 0)
            {
                columnInfo.KeyTypeCode = singleType;
                columnInfo.KeyTypeInfo = null;
            }
            else
            {
                columnInfo.KeyTypeCode = ColumnTypeCode.Set;
                columnInfo.KeyTypeInfo = GetNestedSetColumnInfo(level - 1, singleType);
            }
            return columnInfo;
        }
    }

    public static class SerializedExtensions
    {
        internal static object Deserialize(this Serializer serializer, byte[] buffer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return serializer.Deserialize(buffer, 0, buffer.Length, typeCode, typeInfo);
        }

        internal static ColumnTypeCode GetCqlTypeForPrimitive(this Serializer serializer, Type type)
        {
            IColumnInfo dummyInfo;
            return serializer.GetCqlType(type, out dummyInfo);
        }
    }
}
