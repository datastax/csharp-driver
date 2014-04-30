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
        public void EncodeDecodeSingleValuesFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[] {"just utf8 text olé!", ColumnTypeCode.Text},
                new object[] {"just ascii text", ColumnTypeCode.Ascii},
                new object[] {123, ColumnTypeCode.Int},
                new object[] {44F, ColumnTypeCode.Float},
                new object[] {-320D, ColumnTypeCode.Double},
                new object[] {99.89770M, ColumnTypeCode.Decimal},
                new object[] {new DateTime(2010, 4, 29), ColumnTypeCode.Timestamp},
                new object[] {new IPAddress(new byte[] { 10, 0, 5, 5}), ColumnTypeCode.Inet},
                new object[] {Guid.NewGuid(), ColumnTypeCode.Uuid},
                new object[] {false, ColumnTypeCode.Boolean},
                new object[] {Int32.MinValue + 100, ColumnTypeCode.Bigint},
                new object[] {new byte [] { 1, 2}, ColumnTypeCode.Blob}
            };
            foreach (object[] value in initialValues)
            {
                byte[] encoded = TypeInterpreter.InvCqlConvert(value[0]);
                Assert.AreEqual(value[0], TypeInterpreter.CqlConvert(encoded, (ColumnTypeCode)value[1], null, value[0].GetType()));
            }
        }
    }
}
