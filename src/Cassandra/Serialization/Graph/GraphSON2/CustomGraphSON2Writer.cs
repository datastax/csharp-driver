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

namespace Cassandra.Serialization.Graph.GraphSON2
{
    /// <inheritdoc />
    internal class CustomGraphSON2Writer : ICustomGraphSONWriter
    {
        private static readonly IDictionary<Type, IGraphSONSerializer> CustomGraphSON2SpecificSerializers =
            new Dictionary<Type, IGraphSONSerializer>
            {
                { typeof(DateTime), new TimestampSerializer() },
                { typeof(DateTimeOffset), new TimestampSerializer() },
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
        ///     Transforms an object into its GraphSON representation including type information.
        /// </summary>
        /// <param name="objectData">The object to transform.</param>
        /// <param name="result">Output parameter with the GraphSON representation of the object ready to be serialized.</param>
        /// <returns>True if serialiation is successful and the output parameter was populated.</returns>
        public bool TryToDict(dynamic objectData, out dynamic result)
        {
            if (objectData == null)
            {
                result = null;
                return true;
            }

            if (objectData is IGraphNode graphNode)
            {
                if (!(graphNode is GraphNode concreteGraphNode))
                {
                    throw new InvalidOperationException("Serialization of custom IGraphNode implementations is not supported.");
                }

                result = concreteGraphNode.GetRaw();
                return true;
            }

            var type = objectData.GetType();

            if (_customSerializers != null)
            {
                var customSerializer = (IGraphSONSerializer)TryGetSerializerFor(_customSerializers, type);

                if (customSerializer != null)
                {
                    result = customSerializer.Dictify(objectData, Writer);
                    return true;
                }
            }

            IGraphSONSerializer serializer = TryGetSerializerFor(Serializers, type);

            if (serializer != null)
            {
                result = serializer.Dictify(objectData, Writer);
            }
            else if (type == typeof(string))
            {
                result = objectData;
            }
            else if (IsSet(type))
            {
                result = SetToGraphSONSet(objectData);
            }
            else if (IsDictionary(objectData))
            {
                result = DictToGraphSONDict(objectData);
            }
            else if (IsEnumerable(objectData))
            {
                result = ListToGraphSONList(objectData);
            }
            else
            {
                return TryHandleNotSupportedType(type, objectData, out result);
            }

            return true;
        }

        protected virtual bool TryHandleNotSupportedType(Type type, dynamic objectData, out dynamic result)
        {
            if (CustomGraphSON2Writer.CustomSerializers.ContainsKey(type))
            {
                result = CustomGraphSON2Writer.CustomSerializers[type].Invoke(objectData);
                return true;
            }

            foreach (var supportedType in CustomGraphSON2Writer.CustomSerializers.Keys)
            {
                if (supportedType.IsAssignableFrom(type))
                {
                    result = CustomGraphSON2Writer.CustomSerializers[supportedType].Invoke(objectData);
                    return true;
                }
            }

            result = null;
            return false;
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
                graphSONDict.Add(Writer.ToDict(keyValue.Key), Writer.ToDict(keyValue.Value));
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
                list.Add(Writer.ToDict(e));
            }

            return list;
        }
    }
}