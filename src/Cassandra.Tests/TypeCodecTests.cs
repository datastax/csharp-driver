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

﻿using System.Collections;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TypeCodecTests
    {
        private readonly byte[] _protocolVersions = new byte[] {1, 2, 3};

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
                
                new Tuple<object, DecodeHandler, EncodeHandler>(new DateTime(1983, 2, 24), TypeCodec.DecodeTimestamp, TypeCodec.EncodeTimestamp),
                new Tuple<object, DecodeHandler, EncodeHandler>(new DateTimeOffset(new DateTime(2015, 10, 21)), TypeCodec.DecodeTimestamp, TypeCodec.EncodeTimestamp),
                new Tuple<object, DecodeHandler, EncodeHandler>(new IPAddress(new byte[] { 1, 1, 5, 255}), TypeCodec.DecodeInet, TypeCodec.EncodeInet),
                new Tuple<object, DecodeHandler, EncodeHandler>(true, TypeCodec.DecodeBoolean, TypeCodec.EncodeBoolean),
                new Tuple<object, DecodeHandler, EncodeHandler>(new byte[] {16}, TypeCodec.DecodeBlob, TypeCodec.EncodeBlob)
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
                new object[] {false, ColumnTypeCode.Boolean},
                new object[] {new byte [] { 1, 2}, ColumnTypeCode.Blob}
            };
            foreach (var version in _protocolVersions)
            {
                foreach (object[] value in initialValues)
                {
                    byte[] encoded = TypeCodec.Encode(version, value[0]);
                    Assert.AreEqual(value[0], TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], null, value[0].GetType()));
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
                new object[] {new List<int>(new [] {1, 2, 1000}), ColumnTypeCode.List, new ListColumnInfo() {ValueTypeCode = ColumnTypeCode.Int}},
                new object[] {new List<double>(new [] {-1D, 2.333D, 1.2D}), ColumnTypeCode.List, new ListColumnInfo() {ValueTypeCode = ColumnTypeCode.Double}},
                new object[] {new List<decimal>(new [] {-1M, 2.333M, 1.2M, 256M}), ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Decimal}}
            };
            foreach (var version in _protocolVersions)
            {
                foreach (object[] value in initialValues)
                {
                    var valueToEncode = (IList)value[0];
                    var encoded = TypeCodec.Encode(version, valueToEncode);
                    var decoded = (IList)TypeCodec.Decode(version, encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2], value[0].GetType());
                    Assert.AreEqual(valueToEncode.Count, decoded.Count);
                    Assert.AreEqual(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void EncodeDecodeMapFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[] {new SortedDictionary<string, string>(), ColumnTypeCode.Map, new MapColumnInfo() {KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Text}},
                new object[] {new SortedDictionary<string, string>{{"key100","value100"}}, ColumnTypeCode.Map, new MapColumnInfo() {KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Text}},
                new object[] {new SortedDictionary<string, string>{{"key1","value1"}, {"key2","value2"}}, ColumnTypeCode.Map, new MapColumnInfo() {KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Text}},
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
        }

        [Test]
        public void ParseDataTypeNameMultipleTest()
        {
            var dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)");
            Assert.AreEqual(ColumnTypeCode.List, dataType.TypeCode);
            Assert.IsInstanceOf<ListColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Int, (dataType.TypeInfo as ListColumnInfo).ValueTypeCode);

            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UUIDType)");
            Assert.AreEqual(ColumnTypeCode.Set, dataType.TypeCode);
            Assert.IsInstanceOf<SetColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Uuid, (dataType.TypeInfo as SetColumnInfo).KeyTypeCode);

            dataType = TypeCodec.ParseDataType("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.LongType)");
            Assert.AreEqual(ColumnTypeCode.Map, dataType.TypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Varchar, (dataType.TypeInfo as MapColumnInfo).KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.Bigint, (dataType.TypeInfo as MapColumnInfo).ValueTypeCode);
        }

        [Test]
        public void ParseDataTypeNameUdtTest()
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
        public void ParseDataTypeNameUdtNestedTest()
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
    }
}
