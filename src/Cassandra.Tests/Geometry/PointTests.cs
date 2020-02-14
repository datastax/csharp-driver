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
using System.Globalization;
using System.Linq;
using Cassandra.Geometry;
using Cassandra.Serialization.Geometry;
using Cassandra.Serialization.Graph.GraphSON1;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Cassandra.Tests.Geometry
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
                var expected = string.Format("{{\"type\":\"Point\",\"coordinates\":[{0},{1}]}}", point.X.ToString(CultureInfo.InvariantCulture), point.Y.ToString(CultureInfo.InvariantCulture));
                // Default serialization to JSON is GeoJson
                var json = JsonConvert.SerializeObject(point);
                Assert.AreEqual(expected, json);
                Assert.AreEqual(expected, point.ToGeoJson());
            }
        }

        [Test]
        public void Should_Be_Serialized_As_WKT()
        {
            foreach (var point in Values)
            {
                var json = JsonConvert.SerializeObject(point, GraphSON1ContractResolver.Settings);
                var expected = string.Format("\"{0}\"", point);
                Assert.AreEqual(expected, json);
            }
        }

        /// <summary>
        /// Represents a exception that occurs while serializing.
        /// </summary>
        public class SerializationException : Exception
        {
            public SerializationException(string message) : base(message)
            {
                
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
