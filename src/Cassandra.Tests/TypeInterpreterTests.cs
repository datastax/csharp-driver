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
                new object[] {new List<decimal>(new [] {-1M, 2.333M, 1.2M}), ColumnTypeCode.Set, new SetColumnInfo() {KeyTypeCode = ColumnTypeCode.Decimal}}
            };
            foreach (object[] value in initialValues)
            {
                byte[] encoded = TypeInterpreter.InvCqlConvert(value[0]);
                Assert.AreEqual(value[0], TypeInterpreter.CqlConvert(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2], value[0].GetType()));
            }
        }
    }
}
