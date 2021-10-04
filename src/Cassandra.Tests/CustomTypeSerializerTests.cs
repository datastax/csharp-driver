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
using System.Text;
using Cassandra.Serialization;
using Cassandra.Tests.Extensions.Serializers;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class CustomTypeSerializerTests
    {
        [Test]
        public void Should_Allow_Custom_Primitive_Serializers()
        {
            var serializer = new SerializerManager(ProtocolVersion.MaxSupported, new[] { new BigDecimalSerializer() })
                .GetCurrentSerializer();
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
            var serializer = new SerializerManager(ProtocolVersion.MaxSupported, new ITypeSerializer[] { typeSerializer })
                .GetCurrentSerializer();
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
            var serializer = new SerializerManager(ProtocolVersion.MaxSupported, new ITypeSerializer[] { typeSerializer })
                .GetCurrentSerializer();
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
