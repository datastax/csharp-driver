//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Serialization;
using Dse.Test.Unit.Extensions.Serializers;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    [TestFixture]
    public class CustomTypeSerializerTests
    {
        public CustomTypeSerializerTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        [Test]
        public void Should_Allow_Custom_Primitive_Serializers()
        {
            var serializer = new Serializer(ProtocolVersion.MaxSupported, new [] {new BigDecimalSerializer()});
            var value = new BigDecimal(5, 1);
            var buffer = serializer.Serialize(value);
            CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 5, 1 }, buffer);
            var deserializedValue = serializer.Deserialize(buffer, ColumnTypeCode.Decimal, null);
            Assert.IsInstanceOf<BigDecimal>(deserializedValue);
            var deserializedDecimal = (BigDecimal) deserializedValue;
            Assert.AreEqual("0.00001", deserializedDecimal.ToString());
            Assert.AreEqual(value.Scale, deserializedDecimal.Scale);
            Assert.AreEqual(value.UnscaledValue, deserializedDecimal.UnscaledValue);
            //Check that other serializers are still working
            CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 10 }, serializer.Serialize(10));
            CollectionAssert.AreEqual(new byte[] { 0x61 }, serializer.Serialize("a"));
        }

        [Test]
        public void Should_Allow_Custom_Cql_Type_Serializers()
        {
            var typeSerializer = new DummyCustomTypeSerializer();
            var serializer = new Serializer(ProtocolVersion.MaxSupported, new ITypeSerializer[] { typeSerializer });
            var value = new DummyCustomType(new byte[] { 1, 2 });
            var buffer = serializer.Serialize(value);
            CollectionAssert.AreEqual(new byte[] { 1, 2 }, buffer);
            var deserializedValue = serializer.Deserialize(buffer, ColumnTypeCode.Custom, typeSerializer.TypeInfo);
            Assert.IsInstanceOf<DummyCustomType>(deserializedValue);
            var deserializedCustom = (DummyCustomType)deserializedValue;
            CollectionAssert.AreEqual(value.Buffer, deserializedCustom.Buffer);
            //Check that other serializers are still working
            CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 10 }, serializer.Serialize(10));
            CollectionAssert.AreEqual(new byte[] { 0x61 }, serializer.Serialize("a"));
        }

        [Test]
        public void Should_Allow_Custom_Udt_Serializers()
        {
            var typeSerializer = new UdtSerializerWrapper();
            var serializer = new Serializer(ProtocolVersion.MaxSupported, new ITypeSerializer[] { typeSerializer });
            var buffer = serializer.Serialize(new object());
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("DUMMY UDT SERIALIZED"), buffer);
            CollectionAssert.AreEqual(buffer, (IEnumerable)serializer.Deserialize(buffer, ColumnTypeCode.Udt, new UdtColumnInfo("ks1.udt1")));
            //Check that other serializers are still working
            CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 10 }, serializer.Serialize(10));
            CollectionAssert.AreEqual(new byte[] { 0x61, 0x62 }, serializer.Serialize("ab"));
            Assert.AreEqual(1, typeSerializer.DeserializationCounter);
            Assert.AreEqual(1, typeSerializer.SerializationCounter);
        }
    }
}
