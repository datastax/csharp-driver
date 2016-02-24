using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Geometry;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Dse.Test.Unit.Geometry
{
    public class CircleTests : BaseUnitTest
    {
        private static readonly Circle[] Values =
        {
            new Circle(new Point(1.1, 2.2), 2.1),
            new Circle(new Point(-1.101, -20.2121121221211), 1.01)
        };

        [Test]
        public void Should_Be_Serialized_As_GeoJson()
        {
            foreach (var circle in Values)
            {
                var json = JsonConvert.SerializeObject(circle);
                var expected = string.Format("{{\"type\":\"Circle\",\"coordinates\":[{0},{1}],\"radius\":{2}}}",
                    circle.Center.X, circle.Center.Y, circle.Radius);
                Assert.AreEqual(expected, json);
                var deserialized = JsonConvert.DeserializeObject<Circle>(json);
                Assert.AreEqual(circle, deserialized);
            }
        }

        [Test]
        public void TypeSerializer_Test()
        {
            var typeSerializer = new CircleSerializer();
            foreach (var item in Values)
            {
                var serialized = typeSerializer.Serialize(1, item);
                var deserialized = typeSerializer.Deserialize(1, serialized, 0, serialized.Length, null);
                Assert.AreEqual(item, deserialized);
                //starting from offset
                serialized = new byte[] { 1, 2, 3 }.Concat(serialized).ToArray();
                deserialized = typeSerializer.Deserialize(1, serialized, 3, serialized.Length - 3, null);
                Assert.AreEqual(item, deserialized);
            }
        }

        [Test]
        public void Should_Not_Allow_Null_Center()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new Circle(null, 1));
        }

        [Test]
        public void ToString_Should_Retrieve_Wkt_Representation()
        {
            Assert.AreEqual("CIRCLE ((1 2) 4.1234)", new Circle(new Point(1, 2), 4.1234).ToString());
        }
    }
}
