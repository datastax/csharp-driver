//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Linq;
using Dse.Geometry;
using Dse.Serialization.Geometry;
using Dse.Serialization.Graph.GraphSON1;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Dse.Test.Unit.Geometry
{
    public class LineStringTests : BaseUnitTest
    {
        private static readonly LineString[] Values =
        {
            new LineString(new Point(1.1, 2.2), new Point(2.01, 4.02)),
            new LineString(),
            new LineString(new Point(-1.101, -20.2121121221211), new Point(2.01, 4.02), new Point(10.010, 14.03))
        };
        
        [Test]
        public void Should_Be_Serialized_As_GeoJson()
        {
            foreach (var line in Values)
            {
                var expected = string.Format("{{\"type\":\"LineString\",\"coordinates\":[{0}]}}",
                    string.Join(",", line.Points.Select(p => "[" + p.X + "," + p.Y + "]")));
#if NET452
                // Default serialization to JSON is GeoJson
                var json = JsonConvert.SerializeObject(line);
                Assert.AreEqual(expected, json);
#endif
                Assert.AreEqual(expected, line.ToGeoJson());
            }
        }

        [Test]
        public void Should_Be_Serialized_As_WKT()
        {
            foreach (var line in Values)
            {
                var json = JsonConvert.SerializeObject(line, GraphSON1ContractResolver.Settings);
                var expected = string.Format("\"{0}\"", line);
                Assert.AreEqual(expected, json);
            }
        }

        [Test]
        public void TypeSerializer_Test()
        {
            var typeSerializer = new LineStringSerializer();
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
        public void Should_Not_Allow_Single_Point()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentOutOfRangeException>(() => new LineString(new Point(1, 2)));
        }

        [Test]
        public void ToString_Should_Retrieve_Wkt_Representation()
        {
            Assert.AreEqual("LINESTRING EMPTY", new LineString().ToString());
            Assert.AreEqual("LINESTRING (1 2, 3 4.1234)", new LineString(new Point(1, 2), new Point(3, 4.1234)).ToString());
        }
    }
}
