//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Net;

using Cassandra.DataStax.Graph;
using Cassandra.Geometry;
using Cassandra.Serialization.Graph.Dse;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    internal class CustomGraphSON2Writer : GraphSON2Writer
    {
        private static readonly IDictionary<Type, IGraphSONSerializer> CustomGraphSON2SpecificSerializers =
            new Dictionary<Type, IGraphSONSerializer>
            {
                { typeof(DateTime), new DateSerializer() },
                { typeof(JavaInstant), new InstantSerializer() },
                { typeof(JavaDuration), new JavaDurationSerializer() },
                { typeof(LocalTime), new LocalTimeSerializer() },
                { typeof(LocalDate), new LocalDateSerializer() },
                { typeof(IPAddress), new InetAddressSerializer() },
                { typeof(byte[]), new BlobSerializer() },
                { typeof(LineString), new LineStringSerializer() },
                { typeof(Point), new PointSerializer() },
                { typeof(Polygon), new PolygonSerializer() },
                { typeof(IProperty), new PropertySerializer() },
                { typeof(IPropertyWithElement), new PropertySerializer() },
                { typeof(Property), new PropertySerializer() },
                { typeof(IVertex), new VertexSerializer() },
                { typeof(Vertex), new VertexSerializer() },
                { typeof(IVertexProperty), new VertexPropertySerializer() },
                { typeof(VertexProperty), new VertexPropertySerializer() },
                { typeof(IEdge), new EdgeSerializer() },
                { typeof(Edge), new EdgeSerializer() },
                { typeof(EnumWrapper), new EnumSerializer()},
                { typeof(GraphNode), new GraphNodeSerializer() },
                { typeof(IGraphNode), new GraphNodeSerializer() },
            };

        /// <summary>
        ///     Creates a new instance of <see cref="GraphSONReader"/>.
        /// </summary>
        public CustomGraphSON2Writer()
        {
            foreach (var kv in CustomGraphSON2Writer.CustomGraphSON2SpecificSerializers)
            {
                Serializers[kv.Key] = kv.Value;
            }
        }

        public override string WriteObject(dynamic objectData)
        {
            return JsonConvert.SerializeObject(ToDict(objectData), GraphSONNode.GraphSONSerializerSettings);
        }
    }
}