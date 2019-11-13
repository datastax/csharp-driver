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
using System.Collections.Generic;
using System.Linq;
using Cassandra.Geometry;
using Cassandra.Serialization.Geometry;
using Cassandra.Serialization.Graph.GraphSON1;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Cassandra.Tests.Geometry
{
    public class PolygonTests : BaseUnitTest
    {
        private static readonly Polygon[] Values =
        {
            new Polygon(new Point(1.1, 3.1), new Point(3.1, 1.1), new Point(3.1, 6.1), new Point(1.1, 3.1)),
            new Polygon(
                new[] {new Point(1.1, 3.1), new Point(3.1, 1.1), new Point(3.1, 6.1), new Point(1.1, 3.1)},
                new[] {new Point(2.1, 2.1), new Point(2.1, 1.1), new Point(1.1, 1.1), new Point(2.1, 2.1)})
        };

        [Test]
        public void Should_Be_Serialized_As_GeoJson()
        {
            foreach (var polygon in Values)
            {
                var expected = string.Format("{{\"type\":\"Polygon\",\"coordinates\":[{0}]}}",
                    string.Join(",", polygon.Rings.Select(r =>
                        "[" + string.Join(",", r.Select(p => "[" + p.X + "," + p.Y + "]")) + "]")));
#if NET452
                // Default serialization to Json is GeoJson
                var json = JsonConvert.SerializeObject(polygon);
                Assert.AreEqual(expected, json);
#endif
                Assert.AreEqual(expected, polygon.ToGeoJson());
            }
        }

        [Test]
        public void Should_Be_Serialized_As_WKT()
        {
            foreach (var polygon in Values)
            {
                var json = JsonConvert.SerializeObject(polygon, GraphSON1ContractResolver.Settings);
                var expected = string.Format("\"{0}\"", polygon);
                Assert.AreEqual(expected, json);
            }
        }

        [Test]
        public void TypeSerializer_Test()
        {
            var typeSerializer = new PolygonSerializer();
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
            Assert.Throws<ArgumentException>(() => new Polygon((Point)null));
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new Polygon((IList<IList<Point>>)null));
        }

        [Test]
        public void ToString_Should_Retrieve_Wkt_Representation()
        {
            Assert.AreEqual("POLYGON EMPTY", new Polygon().ToString());
            Assert.AreEqual(
                "POLYGON ((1 3, 3 1, 3 6, 1 3))", 
                new Polygon(new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)).ToString());
        }
    }
}
