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
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Geometry;
using Cassandra.Serialization.Graph.GraphSON2.Dse;
using Cassandra.Serialization.Graph.GraphSON2.Structure;
using Cassandra.Serialization.Graph.GraphSON2.Tinkerpop;
using Newtonsoft.Json;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    /// <inheritdoc />
    internal class CustomGraphSON2Writer : ICustomGraphSONWriter
    {
        private static readonly IDictionary<Type, IGraphSONSerializer> CustomGraphSON2SpecificSerializers =
            new Dictionary<Type, IGraphSONSerializer>
            {
                { typeof(DateTime), new InstantSerializer() },
                { typeof(DateTimeOffset), new InstantSerializer() },
                { typeof(TinkerpopDate), new TinkerpopDateSerializer()},
                { typeof(TinkerpopTimestamp), new TinkerpopTimestampSerializer() },
                { typeof(Duration), new Duration2Serializer() },
                { typeof(LocalTime), new LocalTimeSerializer() },
                { typeof(LocalDate), new LocalDateSerializer() },
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
            };

        /// <summary>
        /// Serializers must return a dictionary but IPAddress serializes to a string,
        /// so this has to be handled in a different way.
        /// </summary>
        private static readonly IDictionary<Type, Func<dynamic, dynamic>> CustomSerializers =
            new Dictionary<Type, Func<dynamic, dynamic>>
            {
                { typeof(IPAddress), objectData => ((IPAddress) objectData).ToString() }
            };
        
        private static Dictionary<Type, IGraphSONSerializer> DefaultSerializers { get; } =
            new EmptyGraphSON2Writer().GetSerializers();

        private readonly IReadOnlyDictionary<Type, IGraphSONSerializer> _customSerializers;

        static CustomGraphSON2Writer()
        {
            CustomGraphSON2Writer.AddGraphSON2Serializers(CustomGraphSON2Writer.DefaultSerializers);
        }

        protected static void AddGraphSON2Serializers(IDictionary<Type, IGraphSONSerializer> dictionary)
        {
            foreach (var kv in CustomGraphSON2Writer.CustomGraphSON2SpecificSerializers)
            {
                dictionary[kv.Key] = kv.Value;
            }
        }

        public CustomGraphSON2Writer(
            IReadOnlyDictionary<Type, IGraphSONSerializer> customSerializers, IGraphSONWriter writer)
            : this(CustomGraphSON2Writer.DefaultSerializers, customSerializers, writer)
        {
        }
        
        protected CustomGraphSON2Writer(
            Dictionary<Type, IGraphSONSerializer> serializers,
            IReadOnlyDictionary<Type, IGraphSONSerializer> customSerializers, 
            IGraphSONWriter writer)
        {
            Serializers = serializers;
            _customSerializers = customSerializers;
            Writer = writer;
        }

        protected IReadOnlyDictionary<Type, IGraphSONSerializer> Serializers { get; }
        
        protected IGraphSONWriter Writer { get; }

        /// <summary>
        ///     Serializes an object to GraphSON.
        /// </summary>
        /// <param name="objectData">The object to serialize.</param>
        /// <returns>The serialized GraphSON.</returns>
        public string WriteObject(dynamic objectData)
        {
            return JsonConvert.SerializeObject(ToDict(objectData), GraphSONNode.GraphSONSerializerSettings);
        }

        /// <summary>
        ///     Transforms an object into its GraphSON representation including type information.
        /// </summary>
        /// <param name="objectData">The object to transform.</param>
        /// <returns>A GraphSON representation of the object ready to be serialized.</returns>
        public dynamic ToDict(dynamic objectData)
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

            if (_customSerializers != null)
            {
                var customSerializer = (IGraphSONSerializer) TryGetSerializerFor(_customSerializers, type);

                if (customSerializer != null)
                {
                    return customSerializer.Dictify(objectData, Writer);
                }
            }

            IGraphSONSerializer serializer = TryGetSerializerFor(Serializers, type);

            if (serializer != null)
                return serializer.Dictify(objectData, Writer);
            if (type == typeof(string))
                return objectData;
            if (IsSet(type))
                return SetToGraphSONSet(objectData);
            if (IsDictionary(objectData))
                return DictToGraphSONDict(objectData);
            if (IsEnumerable(objectData))
                return ListToGraphSONList(objectData);
            return HandleNotSupportedType(type, objectData);
        }

        protected virtual dynamic HandleNotSupportedType(Type type, dynamic objectData)
        {
            if (CustomGraphSON2Writer.CustomSerializers.ContainsKey(type))
            {
                return CustomGraphSON2Writer.CustomSerializers[type].Invoke(objectData);
            }

            foreach (var supportedType in CustomGraphSON2Writer.CustomSerializers.Keys)
            {
                if (supportedType.IsAssignableFrom(type))
                {
                    return CustomGraphSON2Writer.CustomSerializers[supportedType].Invoke(objectData);
                }
            }

            return objectData;
        }

        private IGraphSONSerializer TryGetSerializerFor(IReadOnlyDictionary<Type, IGraphSONSerializer> serializers, Type type)
        {
            if (serializers.ContainsKey(type))
            {
                return serializers[type];
            }
            foreach (var supportedType in serializers.Keys)
                if (supportedType.IsAssignableFrom(type))
                {
                    return serializers[supportedType];
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