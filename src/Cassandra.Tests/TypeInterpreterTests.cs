using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TypeInterpreterTests
    {
        [Test]
        public void EncodeDecodeSingleValuesTest()
        {
            var initialValues = new []
            {
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>("utf8 text mañana", TypeInterpreter.ConvertFromText, TypeInterpreter.InvConvertFromText),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>("ascii text", TypeInterpreter.ConvertFromAscii, TypeInterpreter.InvConvertFromAscii),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(1234, TypeInterpreter.ConvertFromInt, TypeInterpreter.InvConvertFromInt),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>((long)3129, TypeInterpreter.ConvertFromBigint, TypeInterpreter.InvConvertFromBigint),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(1234F, TypeInterpreter.ConvertFromFloat, TypeInterpreter.InvConvertFromFloat),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(1.14D, TypeInterpreter.ConvertFromDouble, TypeInterpreter.InvConvertFromDouble),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(1.01M, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(72.727272727272727272727272727M, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(-72.727272727272727272727272727M, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(-256M, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(256M, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(0M, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(-1.333333M, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(-256.512M, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(Decimal.MaxValue, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(Decimal.MinValue, TypeInterpreter.ConvertFromDecimal, TypeInterpreter.InvConvertFromDecimal),
                
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(new DateTime(1983, 2, 24), TypeInterpreter.ConvertFromTimestamp, TypeInterpreter.InvConvertFromTimestamp),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(new DateTimeOffset(new DateTime(2015, 10, 21)), TypeInterpreter.ConvertFromTimestamp, TypeInterpreter.InvConvertFromTimestamp),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(new IPAddress(new byte[] { 1, 1, 5, 255}), TypeInterpreter.ConvertFromInet, TypeInterpreter.InvConvertFromInet),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(true, TypeInterpreter.ConvertFromBoolean, TypeInterpreter.InvConvertFromBoolean),
                new Tuple<object, CqlConvertDelegate, InvCqlConvertDelegate>(new byte[] {16}, TypeInterpreter.ConvertFromBlob, TypeInterpreter.InvConvertFromBlob)
            };

            foreach (var valueToConvert in initialValues)
            {
                var value = valueToConvert.Item1;
                var encoder = valueToConvert.Item3;
                var decoder = valueToConvert.Item2;
                byte[] encoded = encoder(null, value);
                Assert.AreEqual(value, decoder(null, encoded, value.GetType()));
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
            foreach (object[] value in initialValues)
            {
                byte[] encoded = TypeInterpreter.InvCqlConvert(value[0]);
                Assert.AreEqual(value[0], TypeInterpreter.CqlConvert(encoded, (ColumnTypeCode)value[1], null, value[0].GetType()));
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
            foreach (object[] value in initialValues)
            {
                byte[] encoded = TypeInterpreter.InvCqlConvert(value[0]);
                //Set object as the target CSharp type, it should get the default value
                Assert.AreEqual(value[0], TypeInterpreter.CqlConvert(encoded, (ColumnTypeCode)value[1], null, typeof(object)));
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
            foreach (object[] value in initialValues)
            {
                byte[] encoded = TypeInterpreter.InvCqlConvert(value[0]);
                Assert.AreEqual(value[0], TypeInterpreter.CqlConvert(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2], value[0].GetType()));
            }
        }

        [Test]
        public void ParseDataTypeNameSingleTest()
        {
            var dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.Int32Type");
            Assert.AreEqual(ColumnTypeCode.Int, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.UUIDType");
            Assert.AreEqual(ColumnTypeCode.Uuid, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.UTF8Type");
            Assert.AreEqual(ColumnTypeCode.Varchar, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.BytesType");
            Assert.AreEqual(ColumnTypeCode.Blob, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.FloatType");
            Assert.AreEqual(ColumnTypeCode.Float, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.DoubleType");
            Assert.AreEqual(ColumnTypeCode.Double, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.BooleanType");
            Assert.AreEqual(ColumnTypeCode.Boolean, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.InetAddressType");
            Assert.AreEqual(ColumnTypeCode.Inet, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.DateType");
            Assert.AreEqual(ColumnTypeCode.Timestamp, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.TimestampType");
            Assert.AreEqual(ColumnTypeCode.Timestamp, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.LongType");
            Assert.AreEqual(ColumnTypeCode.Bigint, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.DecimalType");
            Assert.AreEqual(ColumnTypeCode.Decimal, dataType.TypeCode);
            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.IntegerType");
            Assert.AreEqual(ColumnTypeCode.Varint, dataType.TypeCode);
        }

        [Test]
        public void ParseDataTypeNameMultipleTest()
        {
            var dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)");
            Assert.AreEqual(ColumnTypeCode.List, dataType.TypeCode);
            Assert.IsInstanceOf<ListColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Int, (dataType.TypeInfo as ListColumnInfo).ValueTypeCode);

            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UUIDType)");
            Assert.AreEqual(ColumnTypeCode.Set, dataType.TypeCode);
            Assert.IsInstanceOf<SetColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Uuid, (dataType.TypeInfo as SetColumnInfo).KeyTypeCode);

            dataType = TypeInterpreter.ParseDataType("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.LongType)");
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
            var dataType = TypeInterpreter.ParseDataType(typeText);
            Assert.AreEqual(ColumnTypeCode.Udt, dataType.TypeCode);
            //Udt name
            Assert.AreEqual("phone", dataType.Name);
            Assert.IsInstanceOf<UdtColumnInfo>(dataType.TypeInfo);
            var subTypes = (dataType.TypeInfo as UdtColumnInfo).Types;
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
            var dataType = TypeInterpreter.ParseDataType(typeText);
            Assert.AreEqual(ColumnTypeCode.Udt, dataType.TypeCode);
            Assert.IsInstanceOf<UdtColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual("address", dataType.Name);
            Assert.AreEqual("tester.address", (dataType.TypeInfo as UdtColumnInfo).Name);
            var subTypes = (dataType.TypeInfo as UdtColumnInfo).Types;
            Assert.AreEqual(3, subTypes.Count);
            Assert.AreEqual("street,ZIP,phones", String.Join(",", subTypes.Select(s => s.Name)));
            Assert.AreEqual(ColumnTypeCode.Varchar, subTypes[0].TypeCode);
            Assert.AreEqual(ColumnTypeCode.Set, subTypes[2].TypeCode);
            //field name
            Assert.AreEqual("phones", subTypes[2].Name);

            var phonesSubType = (UdtColumnInfo)((SetColumnInfo)subTypes[2].TypeInfo).KeyTypeInfo;
            Assert.AreEqual("tester.phone", phonesSubType.Name);
            Assert.AreEqual(2, phonesSubType.Types.Count);
            Assert.AreEqual("alias", phonesSubType.Types[0].Name);
            Assert.AreEqual("number", phonesSubType.Types[1].Name);
        }

        [Test]
        public void ParseJsonArrayOfStrings()
        {
            var result = TypeInterpreter.ParseJsonArrayOfStrings("[]");
            Assert.AreEqual(0, result.Count);
            result = TypeInterpreter.ParseJsonArrayOfStrings("");
            Assert.AreEqual(0, result.Count);
            result = TypeInterpreter.ParseJsonArrayOfStrings(null);
            Assert.AreEqual(0, result.Count);
            result = TypeInterpreter.ParseJsonArrayOfStrings("['one', 'TWO', 'three']");
            Assert.AreEqual(3, result.Count);
            Assert.AreEqual("one, TWO, three", String.Join(", ", result));
            result = TypeInterpreter.ParseJsonArrayOfStrings("[\"What's\", \"happening\", \" Peter \"]");
            Assert.AreEqual(3, result.Count);
            Assert.AreEqual("What's|happening| Peter ", String.Join("|", result));
        }
    }
}
