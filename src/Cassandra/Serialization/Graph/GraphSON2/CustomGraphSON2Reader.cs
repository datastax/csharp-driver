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
using System.Linq;

using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.GraphSON2.Dse;
using Cassandra.Serialization.Graph.GraphSON2.Structure;
using Cassandra.Serialization.Graph.GraphSON2.Tinkerpop;
using Cassandra.Serialization.Graph.GraphSON3.Tinkerpop;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    /// <inheritdoc />
    internal class CustomGraphSON2Reader : ICustomGraphSONReader
    {
        private readonly Dictionary<string, IGraphSONDeserializer> _deserializers;
        private readonly Dictionary<string, IGraphSONStructureDeserializer> _structureDeserializers;
        private readonly IGraphSONReader _reader;
        private readonly IReadOnlyDictionary<string, IGraphSONDeserializer> _customDeserializers;

        private static readonly IReadOnlyDictionary<string, IGraphSONStructureDeserializer> CustomGraphSON2SpecificStructureDeserializers =
            new Dictionary<string, IGraphSONStructureDeserializer>
            {
                // custom deserializer for graph process types (Traverser type is not a direct copy from GLV)
                { TraverserDeserializer.TypeName, new TraverserDeserializer() },

                // custom deserializers for graph structure types (driver types are different from those in the GLV)
                { VertexDeserializer.TypeName, new VertexDeserializer() },
                { VertexPropertyDeserializer.TypeName, new VertexPropertyDeserializer() },
                { EdgeDeserializer.TypeName, new EdgeDeserializer() },
                { PathDeserializer.TypeName, new PathDeserializer() },
                { PropertyDeserializer.TypeName, new PropertyDeserializer() }
            };

        private static readonly IDictionary<string, IGraphSONDeserializer> CustomGraphSON2SpecificDeserializers =
            new Dictionary<string, IGraphSONDeserializer>
            {
                // custom deserializers for tinkerpop types that are mapped to CQL types in DataStax Graph
                { Duration2Serializer.TypeName, new Duration2Serializer() },
                { TimestampSerializer.TypeName, new TimestampSerializer() },
                { LocalTimeSerializer.TypeName, new LocalTimeSerializer() },
                { LocalDateSerializer.TypeName, new LocalDateSerializer() },
                { InetAddressSerializer.TypeName, new InetAddressSerializer() },
                { BlobSerializer.TypeName, new BlobSerializer() },
                { LineStringSerializer.TypeName, new LineStringSerializer() },
                { PointSerializer.TypeName, new PointSerializer() },
                { PolygonSerializer.TypeName, new PolygonSerializer() },

                // custom deserializers for standard tinkerpop types
                { ByteBufferDeserializer.TypeName, new ByteBufferDeserializer() },
                { TinkerpopDateDeserializer.TypeName, new TinkerpopDateDeserializer() },
                { TinkerpopTimestampDeserializer.TypeName, new TinkerpopTimestampDeserializer() },
            };

        static CustomGraphSON2Reader()
        {
            CustomGraphSON2Reader.AddGraphSON2Deserializers(CustomGraphSON2Reader.DefaultDeserializers);
            CustomGraphSON2Reader.AddGraphSON2StructureDeserializers(CustomGraphSON2Reader.StructureDeserializers);
        }

        protected static void AddGraphSON2Deserializers(IDictionary<string, IGraphSONDeserializer> dictionary)
        {
            foreach (var kv in CustomGraphSON2Reader.CustomGraphSON2SpecificDeserializers)
            {
                dictionary[kv.Key] = kv.Value;
            }
        }

        protected static void AddGraphSON2StructureDeserializers(IDictionary<string, IGraphSONStructureDeserializer> dictionary)
        {
            foreach (var kv in CustomGraphSON2Reader.CustomGraphSON2SpecificStructureDeserializers)
            {
                dictionary[kv.Key] = kv.Value;
            }
        }

        public CustomGraphSON2Reader(
            Func<JToken, GraphNode> graphNodeFactory,
            IReadOnlyDictionary<string, IGraphSONDeserializer> customDeserializers,
            IGraphSONReader reader)
            : this(
                CustomGraphSON2Reader.DefaultDeserializers,
                CustomGraphSON2Reader.StructureDeserializers,
                graphNodeFactory,
                customDeserializers,
                reader)
        {
        }

        protected CustomGraphSON2Reader(
            Dictionary<string, IGraphSONDeserializer> deserializers,
            Dictionary<string, IGraphSONStructureDeserializer> structureDeserializers,
            Func<JToken, GraphNode> graphNodeFactory,
            IReadOnlyDictionary<string, IGraphSONDeserializer> customDeserializers,
            IGraphSONReader reader)
        {
            _deserializers = deserializers;
            _structureDeserializers = structureDeserializers;
            _reader = reader;
            _customDeserializers = customDeserializers;
            GraphNodeFactory = graphNodeFactory;
        }

        private static Dictionary<string, IGraphSONDeserializer> DefaultDeserializers { get; } =
            new EmptyGraphSON2Reader().GetDeserializers();

        private static Dictionary<string, IGraphSONStructureDeserializer> StructureDeserializers { get; } =
            new Dictionary<string, IGraphSONStructureDeserializer>();

        protected Func<JToken, GraphNode> GraphNodeFactory { get; }

        /// <summary>
        ///     Deserializes GraphSON to an object.
        /// </summary>
        /// <param name="jToken">The GraphSON to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public dynamic ToObject(JToken jToken)
        {
            if (IsNullOrUndefined(jToken))
            {
                return null;
            }

            if (jToken is JArray)
            {
                return jToken.Select(t => ToObject(t));
            }
            if (jToken is JValue jValue)
            {
                return jValue.Value;
            }
            if (!HasTypeKey(jToken))
            {
                return ReadDictionary(jToken);
            }
            return ReadTypedValue(jToken, _reader);
        }

        private bool HasTypeKey(JToken jToken)
        {
            var graphSONType = (string)jToken[GraphSONTokens.TypeKey];
            return graphSONType != null;
        }

        private dynamic ReadTypedValue(JToken typedValue, IGraphSONReader reader)
        {
            var value = typedValue[GraphSONTokens.ValueKey];
            if (IsNullOrUndefined(value))
            {
                return null;
            }

            var graphSONType = (string)typedValue[GraphSONTokens.TypeKey];

            if (_customDeserializers.TryGetValue(graphSONType, out var deserializer))
            {
                return deserializer.Objectify(typedValue[GraphSONTokens.ValueKey], reader);
            }

            if (_structureDeserializers.TryGetValue(graphSONType, out var structureDeserializer))
            {
                return structureDeserializer.Objectify(typedValue[GraphSONTokens.ValueKey], GraphNodeFactory, reader);
            }

            if (_deserializers.TryGetValue(graphSONType, out deserializer))
            {
                return deserializer.Objectify(typedValue[GraphSONTokens.ValueKey], reader);
            }

            throw new InvalidOperationException($"Deserializer for \"{graphSONType}\" not found");
        }

        private dynamic ReadDictionary(JToken jtokenDict)
        {
            var dict = new Dictionary<string, dynamic>();
            foreach (var e in jtokenDict)
            {
                var property = e as JProperty;
                if (property == null)
                    throw new InvalidOperationException($"Cannot read graphson: {jtokenDict}");
                dict.Add(property.Name, ToObject(property.Value));
            }
            return dict;
        }

        private bool IsNullOrUndefined(JToken jToken)
        {
            return jToken.Type == JTokenType.Null || jToken.Type == JTokenType.Undefined;
        }
    }
}