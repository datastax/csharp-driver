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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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
                { typeof(DateTime), new InstantSerializer() },
                { typeof(DateTimeOffset), new InstantSerializer() },
                { typeof(TinkerpopDate), new DateSerializer()},
                { typeof(TinkerpopTimestamp), new TimestampSerializer() },
                { typeof(TinkerpopDuration), new TinkerpopDurationSerializer() },
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

        /// <summary>
        ///     Serializes an object to GraphSON.
        /// </summary>
        /// <param name="objectData">The object to serialize.</param>
        /// <returns>The serialized GraphSON.</returns>
        public override string WriteObject(dynamic objectData)
        {
            return JsonConvert.SerializeObject(ToDict(objectData), GraphSONNode.GraphSONSerializerSettings);
        }

        /// <summary>
        ///     Transforms an object into its GraphSON representation including type information.
        /// </summary>
        /// <param name="objectData">The object to transform.</param>
        /// <returns>A GraphSON representation of the object ready to be serialized.</returns>
        public override dynamic ToDict(dynamic objectData)
        {
            if (objectData is IGraphNode graphNode)
            {
                if (!(graphNode is GraphNode concreteGraphNode))
                {
                    throw new InvalidOperationException("Serialization of custom IGraphNode implementations is not supported.");
                }

                return concreteGraphNode.GetRaw();
            }

            var type = objectData.GetType();

            IGraphSONSerializer serializer = TryGetSerializerFor(type);

            if (serializer != null)
                return serializer.Dictify(objectData, this);
            if (type == typeof(string))
                return objectData;
            if (IsSet(type))
                return SetToGraphSONSet(objectData);
            if (IsDictionary(objectData))
                return DictToGraphSONDict(objectData);
            if (IsEnumerable(objectData))
                return ListToGraphSONList(objectData);
            return objectData;
        }

        private IGraphSONSerializer TryGetSerializerFor(Type type)
        {
            if (Serializers.ContainsKey(type))
            {
                return Serializers[type];
            }
            foreach (var supportedType in Serializers.Keys)
                if (supportedType.IsAssignableFrom(type))
                {
                    return Serializers[supportedType];
                }
            return null;
        }

        private bool IsDictionary(dynamic objectData)
        {
            return objectData is IDictionary;
        }


        private bool IsSet(Type type)
        {
            return type.GetInterfaces().Any(x =>
                x.IsGenericType &&
                x.GetGenericTypeDefinition() == typeof(ISet<>));
        }
        
        private bool IsEnumerable(dynamic objectData)
        {
            return objectData is IEnumerable;
        }

        protected virtual dynamic DictToGraphSONDict(dynamic dict)
        {
            var graphSONDict = new Dictionary<string, dynamic>();
            foreach (var keyValue in dict)
            {
                graphSONDict.Add(ToDict(keyValue.Key), ToDict(keyValue.Value));
            }

            return graphSONDict;
        }

        protected virtual dynamic SetToGraphSONSet(dynamic collection)
        {
            return ListToGraphSONList(collection);
        }

        protected virtual dynamic ListToGraphSONList(dynamic collection)
        {
            var list = new List<dynamic>();
            foreach (var e in collection)
            {
                list.Add(ToDict(e));
            }

            return list;
        }
    }
}