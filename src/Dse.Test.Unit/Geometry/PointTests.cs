using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Dse.Geometry;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Dse.Test.Unit.Geometry
{
    public class PointTests : BaseUnitTest
    {
        private static readonly Point[] Values = 
        {
            new Point(1.1, 2.2),
            new Point(5.666, 3.1),
            new Point(-1.789, -900.77888)
        };

        [Test]
        public void Should_Be_Serialized_As_GeoJson()
        {
            foreach (var point in Values)
            {
                var json = JsonConvert.SerializeObject(point);
                var expected = string.Format("{{\"type\":\"Point\",\"coordinates\":[{0},{1}]}}", point.X, point.Y);
                Assert.AreEqual(expected, json);
                var deserialized = JsonConvert.DeserializeObject<Point>(json);
                Assert.AreEqual(point.X, deserialized.X);
                Assert.AreEqual(point.Y, deserialized.Y);
                Assert.AreEqual(point, deserialized);   
            }
        }

        [Test]
        public void TypeSerializer_Test()
        {
            var typeSerializer = new PointSerializer();
            foreach (var item in Values)
            {
                var serialized = typeSerializer.Serialize(1, item);
                //start from offset
                serialized = new byte[] {1, 2, 3}.Concat(serialized).ToArray();
                var deserialized = typeSerializer.Deserialize(1, serialized, 3, serialized.Length - 3, null);
                Assert.AreEqual(item, deserialized);
            }
        }

        [Test]
        public void ToString_Returns_WKT()
        {
            Assert.AreEqual("POINT (-1 2.2345)", new Point(-1, 2.2345).ToString());
            Assert.AreEqual("POINT (0 123.0001)", new Point(0, 123.0001).ToString());
        }
    }
}
