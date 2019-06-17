// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using Dse.Insights.Schema;
using Dse.Insights.Schema.Converters;
using Moq;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Dse.Test.Unit.Insights
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