//
//      Copyright (C) 2012-2014 DataStax Inc.
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

namespace Cassandra.Tests
{
    [TestFixture]
    public class TypeCodecTests
    {
        private readonly byte[] _protocolVersions = new byte[] {1, 2, 3};
        private static readonly MapColumnInfo MapColumnInfoStringString = new MapColumnInfo() { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Text };

        [Test]
        public void EncodeDecodeSingleValuesTest()
        {
            var initialValues = new []
            {
                new Tuple<object, DecodeHandler, EncodeHandler>("utf8 text mañana", TypeCodec.DecodeText, TypeCodec.EncodeText),
                new Tuple<object, DecodeHandler, EncodeHandler>("ascii text", TypeCodec.DecodeAscii, TypeCodec.EncodeAscii),
                new Tuple<object, DecodeHandler, EncodeHandler>(1234, TypeCodec.DecodeInt, TypeCodec.EncodeInt),
                new Tuple<object, DecodeHandler, EncodeHandler>((long)3129, TypeCodec.DecodeBigint, TypeCodec.EncodeBigint),
                new Tuple<object, DecodeHandler, EncodeHandler>(1234F, TypeCodec.DecodeFloat, TypeCodec.EncodeFloat),

                new Tuple<object, DecodeHandler, EncodeHandler>(1.14D, TypeCodec.DecodeDouble, TypeCodec.EncodeDouble),
                new Tuple<object, DecodeHandler, EncodeHandler>(double.MinValue, TypeCodec.DecodeDouble, TypeCodec.EncodeDouble),
                new Tuple<object, DecodeHandler, EncodeHandler>(-1.14, TypeCodec.DecodeDouble, TypeCodec.EncodeDouble),
                new Tuple<object, DecodeHandler, EncodeHandler>(0d, TypeCodec.DecodeDouble, TypeCodec.EncodeDouble),
                new Tuple<object, DecodeHandler, EncodeHandler>(double.MaxValue, TypeCodec.DecodeDouble, TypeCodec.EncodeDouble),
                new Tuple<object, DecodeHandler, EncodeHandler>(double.NaN, TypeCodec.DecodeDouble, TypeCodec.EncodeDouble),

                new Tuple<object, DecodeHandler, EncodeHandler>(1.01M, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                
                new Tuple<object, DecodeHandler, EncodeHandler>(72.727272727272727272727272727M, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                new Tuple<object, DecodeHandler, EncodeHandler>(-72.727272727272727272727272727M, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                new Tuple<object, DecodeHandler, EncodeHandler>(-256M, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                new Tuple<object, DecodeHandler, EncodeHandler>(256M, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                new Tuple<object, DecodeHandler, EncodeHandler>(0M, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                new Tuple<object, DecodeHandler, EncodeHandler>(-1.333333M, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                new Tuple<object, DecodeHandler, EncodeHandler>(-256.512M, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                new Tuple<object, DecodeHandler, EncodeHandler>(Decimal.MaxValue, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                new Tuple<object, DecodeHandler, EncodeHandler>(Decimal.MinValue, TypeCodec.DecodeDecimal, TypeCodec.EncodeDecimal),
                
                new Tuple<object, DecodeHandler, EncodeHandler>(new DateTimeOffset(new DateTime(2015, 10, 21)), TypeCodec.DecodeTimestamp, TypeCodec.EncodeTimestamp),
                new Tuple<object, DecodeHandler, EncodeHandler>(new IPAddress(new byte[] { 1, 1, 5, 255}), TypeCodec.DecodeInet, TypeCodec.EncodeInet),
                new Tuple<object, DecodeHandler, EncodeHandler>(true, TypeCodec.DecodeBoolean, TypeCodec.EncodeBoolean),
                new Tuple<object, DecodeHandler, EncodeHandler>(new byte[] {16}, TypeCodec.DecodeBlob, TypeCodec.EncodeBlob),
                new Tuple<object, DecodeHandler, EncodeHandler>(Guid.NewGuid(), TypeCodec.DecodeUuid, TypeCodec.EncodeUuid),
                new Tuple<object, DecodeHandler, EncodeHandler>(Guid.NewGuid(), TypeCodec.DecodeTimeuuid, TypeCodec.EncodeTimeuuid),
            };
            foreach (var version in _protocolVersions)
            {
                foreach (var valueToConvert in initialValues)
                {
                    var value = valueToConvert.Item1;
                    var encoder = valueToConvert.Item3;
                    var decoder = valueToConvert.Item2;
                    byte[] encoded = encoder(version, null, value);
                    Assert.AreEqual(value, decoder(version, null, encoded, value.GetType()));
                }
            }
        }

        [Test]
        public void EncodeDecodeSingleValuesFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[] {"just utf8 text olé!", ColumnTypeCode.Text},
                new object[] {"just ascii text", ColumnTypeCode.Ascii},
                new object[] {123, ColumnTypeCode.Int},
                new object[] {Int64.MinValue + 100, ColumnTypeCode.Bigint},
                new object[] {44F, ColumnTypeCode.Float},
                new object[] {-320D, ColumnTypeCode.Double},
                new object[] {99.89770M, ColumnTypeCode.Decimal},
                new object[] {Decimal.MaxValue, ColumnTypeCode.Decimal},
                new object[] {new DateTime(2010, 4, 29), ColumnTypeCode.Timestamp},
                new object[] {new DateTimeOffset(new DateTime(2010, 4, 29)), ColumnTypeCode.Timestamp},
                new object[] {new IPAddress(new byte[] { 10, 0, 5, 5}), ColumnTypeCode.Inet},
                new object[] {Guid.NewGuid(), ColumnTypeCode.Uuid},
                new object[] {Guid.NewGuid(), ColumnTypeCode.Timeuuid},
                new object[] {TimeUuid.NewId(), ColumnTypeCode.Timeuuid},
                new object[] {false, ColumnTypeCode.Boolean},
                new object[] {new byte [] { 1, 2}, ColumnTypeCode.Blob}
            };
            foreach (var version in _protocolVersions)
            {
                foreach (object[] value in initialValues)
                {
                    byte[] encoded = TypeCodec.Encode(version, value[0]);
                    object decoded = TypeCodec.Decode(version, encoded, (ColumnTypeCode) value[1], null);
                    if (decoded.GetType() != value[0].GetType())
                    {
                        if (decoded is IConvertible)
                        {
                            decoded = Convert.ChangeType(decoded, value[0].GetType());   
                        }
                        else if (value[0] is DateTime)
                        {
                            decoded = ((DateTimeOffset)decoded).DateTime;
                        }
                        else if (value[0] is TimeUuid)
                        {
                            decoded = (TimeUuid) (Guid) decoded;
                        }
                    }
                    Assert.AreEqual(value[0], decoded);
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
                foreach (object[] value in initialValues)
                {
                    byte[] encoded = TypeCodec.Encode(version, value[0]);
                    //Set object as the target CSharp type, it should get the default value
                    Assert.AreEqual(value[0], TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], null, typeof(object)));
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
                new object[] {new double[] {5D, 4.333D, 1.2D}, ColumnTypeCode.List, new ListColumnInfo() {ValueTypeCode = ColumnTypeCode.Double}},
                //Sets
                new object[] {new List<decimal>(new [] {-1M, 2.333M, 1.2M, 256M}), ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Decimal}},
                new object[] {new SortedSet<string>(new [] {"a", "b", "c"}), ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Text}},
                new object[] {new HashSet<string>(new [] {"ADADD", "AA", "a"}), ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Text}},
                new object[] {new string[] {"ADADD", "AA", "a"}, ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Text}}
            };
            foreach (var version in _protocolVersions)
            {
                foreach (object[] value in initialValues)
                {
                    var originalType = value[0].GetType();
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = TypeCodec.Encode(version, valueToEncode);
                    var decoded = (IEnumerable)TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2], originalType);
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
                new List<object>(),
                //any class
                new List<TypeCodecTests>()
            };
            foreach (var version in _protocolVersions)
            {
                foreach (var value in values)
                {
                    Assert.Throws<InvalidTypeException>(() => TypeCodec.Encode(version, value));
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
                foreach (object[] value in initialValues)
                {
                    var valueToEncode = (IDictionary)value[0];
                    var encoded = TypeCodec.Encode(version, valueToEncode);
                    var decoded = (IDictionary)TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2], typeof(IDictionary));
                    CollectionAssert.AreEquivalent(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void EncodeDecodeTupleFactoryTest()
        {
            const int version = 3;
            var initialValues = new object[]
            {
                new object[] {new Tuple<string>("val1"), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}}}},
                new object[] {new Tuple<string, int>("val2", 2), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}, new ColumnDesc(){TypeCode = ColumnTypeCode.Int}}}},
                new object[] {new Tuple<string, int>(null, -1234), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}, new ColumnDesc(){TypeCode = ColumnTypeCode.Int}}}}
            };
            foreach (object[] value in initialValues)
            {
                var valueToEncode = (IStructuralEquatable)value[0];
                var encoded = TypeCodec.Encode(version, valueToEncode);
                var decoded = (IStructuralEquatable)TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                Assert.AreEqual(valueToEncode, decoded);
            }
        }

        [Test]
        public void EncodeDecodeTupleAsSubtypeFactoryTest()
        {
            const int version = 3;
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
            foreach (object[] value in initialValues)
            {
                var valueToEncode = (IList)value[0];
                var encoded = TypeCodec.Encode(version, valueToEncode);
                var decoded = (IList)TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                Assert.AreEqual(valueToEncode, decoded);
            }
        }

        [Test]
        public void ParseDataTypeNameSingleTest()
        {
            var dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.Int32Type");
            Assert.AreEqual(ColumnTypeCode.Int, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.UUIDType");
            Assert.AreEqual(ColumnTypeCode.Uuid, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.UTF8Type");
            Assert.AreEqual(ColumnTypeCode.Varchar, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.BytesType");
            Assert.AreEqual(ColumnTypeCode.Blob, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.FloatType");
            Assert.AreEqual(ColumnTypeCode.Float, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.DoubleType");
            Assert.AreEqual(ColumnTypeCode.Double, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.BooleanType");
            Assert.AreEqual(ColumnTypeCode.Boolean, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.InetAddressType");
            Assert.AreEqual(ColumnTypeCode.Inet, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.DateType");
            Assert.AreEqual(ColumnTypeCode.Timestamp, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.TimestampType");
            Assert.AreEqual(ColumnTypeCode.Timestamp, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.LongType");
            Assert.AreEqual(ColumnTypeCode.Bigint, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.DecimalType");
            Assert.AreEqual(ColumnTypeCode.Decimal, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.IntegerType");
            Assert.AreEqual(ColumnTypeCode.Varint, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.CounterColumnType");
            Assert.AreEqual(ColumnTypeCode.Counter, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.TimeUUIDType");
            Assert.AreEqual(ColumnTypeCode.Timeuuid, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.AsciiType");
            Assert.AreEqual(ColumnTypeCode.Ascii, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.SimpleDateType");
            Assert.AreEqual(ColumnTypeCode.Date, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.TimeType");
            Assert.AreEqual(ColumnTypeCode.Time, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.ShortType");
            Assert.AreEqual(ColumnTypeCode.SmallInt, dataType.TypeCode);
            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.ByteType");
            Assert.AreEqual(ColumnTypeCode.TinyInt, dataType.TypeCode);
        }

        [Test]
        public void Parse_DataType_Name_Multiple_Test()
        {
            var dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)");
            Assert.AreEqual(ColumnTypeCode.List, dataType.TypeCode);
            Assert.IsInstanceOf<ListColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Int, (dataType.TypeInfo as ListColumnInfo).ValueTypeCode);

            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UUIDType)");
            Assert.AreEqual(ColumnTypeCode.Set, dataType.TypeCode);
            Assert.IsInstanceOf<SetColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Uuid, (dataType.TypeInfo as SetColumnInfo).KeyTypeCode);

            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.TimeUUIDType)");
            Assert.AreEqual(ColumnTypeCode.Set, dataType.TypeCode);
            Assert.IsInstanceOf<SetColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Timeuuid, (dataType.TypeInfo as SetColumnInfo).KeyTypeCode);

            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.LongType)");
            Assert.AreEqual(ColumnTypeCode.Map, dataType.TypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Varchar, (dataType.TypeInfo as MapColumnInfo).KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.Bigint, (dataType.TypeInfo as MapColumnInfo).ValueTypeCode);
        }

        [Test]
        public void Parse_DataType_Name_Frozen_Test()
        {
            var dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.TimeUUIDType))");
            Assert.AreEqual(ColumnTypeCode.List, dataType.TypeCode);
            Assert.IsInstanceOf<ListColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Timeuuid, (dataType.TypeInfo as ListColumnInfo).ValueTypeCode);

            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)))");
            Assert.AreEqual(ColumnTypeCode.Map, dataType.TypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Varchar, (dataType.TypeInfo as MapColumnInfo).KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.List, (dataType.TypeInfo as MapColumnInfo).ValueTypeCode);
            var subType = (ListColumnInfo) ((dataType.TypeInfo as MapColumnInfo).ValueTypeInfo);
            Assert.AreEqual(ColumnTypeCode.Int, subType.ValueTypeCode);
        }

        [Test]
        public void Parse_DataType_Name_Udt_Test()
        {
            var typeText =
                "org.apache.cassandra.db.marshal.UserType(" +
                    "tester,70686f6e65,616c696173:org.apache.cassandra.db.marshal.UTF8Type,6e756d626572:org.apache.cassandra.db.marshal.UTF8Type" +
                ")";
            var dataType = TypeCodec.ParseDataType(typeText);
            Assert.AreEqual(ColumnTypeCode.Udt, dataType.TypeCode);
            //Udt name
            Assert.AreEqual("phone", dataType.Name);
            Assert.IsInstanceOf<UdtColumnInfo>(dataType.TypeInfo);
            var subTypes = (dataType.TypeInfo as UdtColumnInfo).Fields;
            Assert.AreEqual(2, subTypes.Count);
            Assert.AreEqual("alias", subTypes[0].Name);
            Assert.AreEqual(ColumnTypeCode.Varchar, subTypes[0].TypeCode);
            Assert.AreEqual("number", subTypes[1].Name);
            Assert.AreEqual(ColumnTypeCode.Varchar, subTypes[1].TypeCode);
        }

        [Test]
        public void Parse_DataType_Name_Udt_Nested_Test()
        {
            var typeText =
                "org.apache.cassandra.db.marshal.UserType(" +
                    "tester," +
                    "61646472657373," +
                    "737472656574:org.apache.cassandra.db.marshal.UTF8Type," +
                    "5a4950:org.apache.cassandra.db.marshal.Int32Type," +
                    "70686f6e6573:org.apache.cassandra.db.marshal.SetType(" +
                    "org.apache.cassandra.db.marshal.UserType(" +
                        "tester," +
                        "70686f6e65," +
                        "616c696173:org.apache.cassandra.db.marshal.UTF8Type," +
                        "6e756d626572:org.apache.cassandra.db.marshal.UTF8Type))" +
                ")";
            var dataType = TypeCodec.ParseDataType(typeText);
            Assert.AreEqual(ColumnTypeCode.Udt, dataType.TypeCode);
            Assert.IsInstanceOf<UdtColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual("address", dataType.Name);
            Assert.AreEqual("tester.address", (dataType.TypeInfo as UdtColumnInfo).Name);
            var subTypes = (dataType.TypeInfo as UdtColumnInfo).Fields;
            Assert.AreEqual(3, subTypes.Count);
            Assert.AreEqual("street,ZIP,phones", String.Join(",", subTypes.Select(s => s.Name)));
            Assert.AreEqual(ColumnTypeCode.Varchar, subTypes[0].TypeCode);
            Assert.AreEqual(ColumnTypeCode.Set, subTypes[2].TypeCode);
            //field name
            Assert.AreEqual("phones", subTypes[2].Name);

            var phonesSubType = (UdtColumnInfo)((SetColumnInfo)subTypes[2].TypeInfo).KeyTypeInfo;
            Assert.AreEqual("tester.phone", phonesSubType.Name);
            Assert.AreEqual(2, phonesSubType.Fields.Count);
            Assert.AreEqual("alias", phonesSubType.Fields[0].Name);
            Assert.AreEqual("number", phonesSubType.Fields[1].Name);
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
                foreach (object[] value in initialValues)
                {
                    var originalType = value[0].GetType();
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = TypeCodec.Encode(version, valueToEncode);
                    var decoded = (IEnumerable)TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2], originalType);
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
                foreach (object[] value in initialValues)
                {
                    var originalType = value[0].GetType();
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = TypeCodec.Encode(version, valueToEncode);
                    var decoded = (IEnumerable)TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2], originalType);
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
                foreach (object[] value in initialValues)
                {
                    var originalType = value[0].GetType();
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = TypeCodec.Encode(version, valueToEncode);
                    var decoded = (IEnumerable)TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2], originalType);
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
            foreach (var v in values)
            {
                var encoded = TypeCodec.EncodeSByte(4, null, v.Item1);
                CollectionAssert.AreEqual(encoded, new[] { v.Item2 });
                var decoded = (sbyte)TypeCodec.DecodeSByte(4, null, encoded, null);
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
            foreach (var v in values)
            {
                var encoded = TypeCodec.EncodeDate(4, null, v);
                var decoded = (LocalDate)TypeCodec.DecodeDate(4, null, encoded, null);
                Assert.AreEqual(v, decoded);
            }
        }

        [Test]
        public void Encode_Decode_SmallInt()
        {
            for (var i = Int16.MinValue; ; i++ )
            {
                var encoded = TypeCodec.EncodeShort(4, null, i);
                var decoded = (short)TypeCodec.DecodeShort(4, null, encoded, null);
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
            var ex = Assert.Throws<ArgumentNullException>(() => TypeCodec.EncodeMap(2, MapColumnInfoStringString, value));
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
            var ex = Assert.Throws<ArgumentNullException>(() => TypeCodec.EncodeList(2, new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Text}, value));
            StringAssert.Contains("collections", ex.Message);
        }

        [Test]
        public void Encode_Decode_With_Binary_Representation()
        {
            var values = new[]
            {
                Tuple.Create<object, byte[], EncodeHandler, DecodeHandler>(1D, new byte[] {0x3f, 0xf0, 0, 0, 0, 0, 0, 0}, TypeCodec.EncodeDouble, TypeCodec.DecodeDouble),
                Tuple.Create<object, byte[], EncodeHandler, DecodeHandler>(2D, new byte[] {0x40, 0, 0, 0, 0, 0, 0, 0}, TypeCodec.EncodeDouble, TypeCodec.DecodeDouble),
                Tuple.Create<object, byte[], EncodeHandler, DecodeHandler>(2.2D, new byte[] {0x40, 1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a}, TypeCodec.EncodeDouble, TypeCodec.DecodeDouble),
                Tuple.Create<object, byte[], EncodeHandler, DecodeHandler>(-1D, new byte[] {0xbf, 0xf0, 0, 0, 0, 0, 0, 0}, TypeCodec.EncodeDouble, TypeCodec.DecodeDouble),
                Tuple.Create<object, byte[], EncodeHandler, DecodeHandler>(-1F, new byte[] {0xbf, 0x80, 0, 0}, TypeCodec.EncodeFloat, TypeCodec.DecodeFloat),
                Tuple.Create<object, byte[], EncodeHandler, DecodeHandler>(1.3329F, new byte[] {0x3f, 0xaa, 0x9c, 0x78}, TypeCodec.EncodeFloat, TypeCodec.DecodeFloat)
            };
            foreach (var val in values)
            {
                var encoded = val.Item3(4, null, val.Item1);
                CollectionAssert.AreEqual(val.Item2, encoded);
                Assert.AreEqual(val.Item1, val.Item4(4, null, encoded, null));
            }
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
}
