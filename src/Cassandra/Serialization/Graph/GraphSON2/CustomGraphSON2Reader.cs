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
using Cassandra.Serialization.Graph.Dse;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    internal class CustomGraphSON2Reader : GraphSON2Reader
    {
        public CustomGraphSON2Reader(Func<JToken, GraphNode> graphNodeFactory)
        {
            GraphNodeFactory = graphNodeFactory;

            var customGraphSon2SpecificDeserializers =
                new Dictionary<string, IGraphSONDeserializer>
                {
                    {"g:Date", new DateDeserializer()},
                    {"g:Timestamp", new TimestampDeserializer()},
                    { TinkerpopDurationSerializer.TypeName, new TinkerpopDurationSerializer() },
                    { InstantSerializer.TypeName, new InstantSerializer() },
                    { LocalTimeSerializer.TypeName, new LocalTimeSerializer() },
                    { LocalDateSerializer.TypeName, new LocalDateSerializer() },
                    { InetAddressSerializer.TypeName, new InetAddressSerializer() },
                    { BlobSerializer.TypeName, new BlobSerializer() },
                    { LineStringSerializer.TypeName, new LineStringSerializer() },
                    { PointSerializer.TypeName, new PointSerializer() },
                    { PolygonSerializer.TypeName, new PolygonSerializer() },
                    { VertexDeserializer.TypeName, new VertexDeserializer(GraphNodeFactory) },
                    { VertexPropertyDeserializer.TypeName, new VertexPropertyDeserializer(GraphNodeFactory) },
                    { EdgeDeserializer.TypeName, new EdgeDeserializer(GraphNodeFactory) },
                    { PathDeserializer.TypeName, new PathDeserializer(GraphNodeFactory) },
                    { PropertyDeserializer.TypeName, new PropertyDeserializer(GraphNodeFactory) },
                    { TraverserDeserializer.TypeName, new TraverserDeserializer(GraphNodeFactory) },
                };

            foreach (var kv in customGraphSon2SpecificDeserializers)
            {
                Deserializers[kv.Key] = kv.Value;
            }
        }

        protected Func<JToken, GraphNode> GraphNodeFactory { get; }

        /// <summary>
        ///     Deserializes GraphSON to an object.
        /// </summary>
        /// <param name="jToken">The GraphSON to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public override dynamic ToObject(JToken jToken)
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
            return ReadTypedValue(jToken);
        }

        private bool HasTypeKey(JToken jToken)
        {
            var graphSONType = (string)jToken[GraphSONTokens.TypeKey];
            return graphSONType != null;
        }

        private dynamic ReadTypedValue(JToken typedValue)
        {
            var graphSONType = (string)typedValue[GraphSONTokens.TypeKey];
            if (!Deserializers.TryGetValue(graphSONType, out var deserializer))
            {
                throw new InvalidOperationException($"Deserializer for \"{graphSONType}\" not found");
            }

            var value = typedValue[GraphSONTokens.ValueKey];
            if (IsNullOrUndefined(value))
            {
                return null;
            }

            return deserializer.Objectify(typedValue[GraphSONTokens.ValueKey], this);
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