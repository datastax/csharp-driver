﻿#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion License

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.DataStax.Graph.Internal;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON
{
    /// <summary>
    ///     Allows to deserialize GraphSON to objects.
    /// </summary>
    internal abstract class GraphSONReader : IGraphSONReader
    {
        /// <summary>
        /// Contains the <see cref="IGraphSONDeserializer" /> instances by their type identifier.
        /// </summary>
        protected readonly Dictionary<string, IGraphSONDeserializer> Deserializers = new Dictionary
            <string, IGraphSONDeserializer>
            {
                //{"g:Traverser", new TraverserReader()}, // added in custom reader
                {"g:Int32", new Int32Converter()},
                {"g:Int64", new Int64Converter()},
                {"g:Float", new FloatConverter()},
                {"g:Double", new DoubleConverter()},
                {"g:Direction", new DirectionDeserializer()},
                {"g:UUID", new UuidDeserializer()},
                //{"g:Date", new DateDeserializer()}, // added in custom reader
                //{"g:Timestamp", new DateDeserializer()}, // added in custom reader
                //{"g:Vertex", new VertexDeserializer()}, // added in custom reader
                //{"g:Edge", new EdgeDeserializer()}, // added in custom reader
                //{"g:Property", new PropertyDeserializer()}, // added in custom reader
                //{"g:VertexProperty", new VertexPropertyDeserializer()}, // added in custom reader
                //{"g:Path", new PathDeserializer()}, // added in custom reader
                {"g:T", new TDeserializer()},

                //Extended
                {"gx:BigDecimal", new DecimalConverter()},
                //{"gx:Duration", new DurationDeserializer()}, // added in custom reader
                {"gx:BigInteger", new BigIntegerDeserializer()},
                {"gx:Byte", new ByteConverter()},
                {"gx:Char", new CharConverter()},
                {"gx:Int16", new Int16Converter() }
            };

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONReader" /> class.
        /// </summary>
        protected GraphSONReader()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONReader" /> class.
        /// </summary>
        /// <param name="deserializerByGraphSONType">
        ///     <see cref="IGraphSONDeserializer" /> deserializers identified by their
        ///     GraphSON type.
        /// </param>
        protected GraphSONReader(IReadOnlyDictionary<string, IGraphSONDeserializer> deserializerByGraphSONType)
        {
            foreach (var deserializerAndGraphSONType in deserializerByGraphSONType)
                Deserializers[deserializerAndGraphSONType.Key] = deserializerAndGraphSONType.Value;
        }

        /// <summary>
        ///     Deserializes a GraphSON collection to an object.
        /// </summary>
        /// <param name="graphSonData">The GraphSON collection to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public virtual dynamic ToObject(IEnumerable<JToken> graphSonData)
        {
            return graphSonData.Select(graphson => ToObject(graphson));
        }

        /// <summary>
        ///     Deserializes GraphSON to an object.
        /// </summary>
        /// <param name="jToken">The GraphSON to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public virtual dynamic ToObject(JToken jToken)
        {
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
    }
}