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
using Cassandra.DataStax.Insights.Schema;
using Cassandra.DataStax.Insights.Schema.Converters;
using Moq;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Cassandra.Tests.DataStax.Insights
{
    [TestFixture]
    public class InsightTypeConverterTests
    {
        [Test]
        public void Should_WriteNull_When_NullObjectIsProvided()
        {
            var mockWriter = Mock.Of<JsonWriter>();
            var sut = new InsightTypeInsightsConverter();
            sut.WriteJson(mockWriter, null, new JsonSerializer());
            Mock.Get(mockWriter).Verify(mock => mock.WriteNull(), Times.Once);
        }

        [Test]
        public void Should_WriteEvent_When_EventEnumIsProvided()
        {
            var mockWriter = Mock.Of<JsonWriter>();
            var sut = new InsightTypeInsightsConverter();
            sut.WriteJson(mockWriter, InsightType.Event, new JsonSerializer());
            Mock.Get(mockWriter).Verify(mock => mock.WriteValue((object)"EVENT"), Times.Once);
        }

        [Test]
        public void Should_ReturnTrue_When_InsightTypeEnumIsProvided()
        {
            var sut = new InsightTypeInsightsConverter();
            Assert.IsTrue(sut.CanConvert(typeof(InsightType)));
        }

        [Test]
        public void Should_ReturnFalse_When_EnumIsProvided()
        {
            var sut = new InsightTypeInsightsConverter();
            Assert.IsFalse(sut.CanConvert(typeof(Enum)));
        }
    }
}