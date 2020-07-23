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
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;

using Cassandra.DataStax.Graph;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    internal class GraphSONNode : INode
    {
        private readonly IGraphSONTypeConverter _graphSONConverter;

        private static readonly JTokenEqualityComparer Comparer = new JTokenEqualityComparer();

        private readonly JToken _token;

        private readonly string _graphsonType;

        internal static readonly JsonSerializerSettings GraphSONSerializerSettings = new JsonSerializerSettings
        {
            Culture = CultureInfo.InvariantCulture,
            DateParseHandling = DateParseHandling.None
        };

        public bool IsArray => _token is JArray;

        public bool IsObjectTree => !IsScalar && !IsArray;

        public bool IsScalar => _token is JValue
                                || (_token is JObject jobj && jobj[GraphSONTypeConverter.ValueKey] is JValue);

        public long Bulk { get; }

        internal GraphSONNode(IGraphSONTypeConverter graphTypeConverter, string json)
        {
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }

            _graphSONConverter = graphTypeConverter ?? throw new ArgumentNullException(nameof(graphTypeConverter));
            var parsedJson = (JObject)JsonConvert.DeserializeObject(json, GraphSONNode.GraphSONSerializerSettings);
            _token = parsedJson["result"];
            if (_token is JObject jobj)
            {
                _graphsonType = jobj[GraphSONTypeConverter.TypeKey]?.ToString();
            }

            var bulkToken = parsedJson["bulk"];
            Bulk = bulkToken != null ? GetTokenValue<long>(bulkToken) : 1L;
        }

        internal GraphSONNode(IGraphSONTypeConverter graphTypeConverter, JToken parsedGraphItem)
        {
            _graphSONConverter = graphTypeConverter ?? throw new ArgumentNullException(nameof(graphTypeConverter));
            _token = parsedGraphItem ?? throw new ArgumentNullException(nameof(parsedGraphItem));
            if (_token is JObject jobj)
            {
                _graphsonType = jobj[GraphSONTypeConverter.TypeKey]?.ToString();
            }
        }

        public T Get<T>(string propertyName, bool throwIfNotFound)
        {
            var value = GetPropertyValue(propertyName, throwIfNotFound);
            if (value == null)
            {
                if (default(T) != null)
                {
                    throw new NullReferenceException(string.Format(
                        "Cannot convert null to {0} because it is a value type, try using Nullable<{0}>",
                        typeof(T).Name));
                }
                return (T)(object)null;
            }
            return GetTokenValue<T>(value);
        }

        private JObject GetTypedValue()
        {
            var value = _token[GraphSONTypeConverter.ValueKey];
            if (value != null && GetGraphSONType() != null)
            {
                // The token represents a GraphSON2/GraphSON3 object {"@type": "g:...", "@value": {}}
                return value as JObject;
            }

            return null;
        }

        private JProperty GetProperty(string name)
        {
            var graphObject = _token as JObject;
            var typedValue = GetTypedValue();
            if (typedValue != null)
            {
                graphObject = typedValue;
            }
            if (graphObject == null)
            {
                throw new KeyNotFoundException($"Cannot retrieve property '{name}' of instance: '{_token}'");
            }
            return graphObject.Property(name);
        }

        private JToken GetPropertyValue(string name, bool throwIfNotFound)
        {
            var property = GetProperty(name);
            if (property == null)
            {
                if (throwIfNotFound)
                {
                    throw new KeyNotFoundException($"Graph result has no top-level property '{name}'");
                }
                return null;
            }
            return property.Value;
        }

        public dynamic GetRaw()
        {
            return _token;
        }

        /// <summary>
        /// Returns either a scalar value or an array representing the token value, performing conversions when required.
        /// </summary>
        private T GetTokenValue<T>(JToken token)
        {
            return _graphSONConverter.FromDb<T>(token);
        }

        private object GetTokenValue(JToken token, Type type)
        {
            return _graphSONConverter.FromDb(token, type);
        }

        /// <summary>
        /// Returns true if the property is defined in this instance.
        /// </summary>
        /// <exception cref="InvalidOperationException">When the underlying value is not an object tree</exception>
        public bool HasProperty(string name)
        {
            return GetProperty(name) != null;
        }

        /// <summary>
        /// Provides the implementation for operations that get member values.
        /// </summary>
        public bool TryGetMember(GetMemberBinder binder, out object result)
        {
            var token = GetPropertyValue(binder.Name, false);
            if (token == null)
            {
                result = null;
                return false;
            }
            var node = new GraphNode(new GraphSONNode(_graphSONConverter, token));
            result = node.To(binder.ReturnType);
            return true;
        }

        /// <summary>
        /// Gets the hash code for this instance, based on its value.
        /// </summary>
        public override int GetHashCode()
        {
            return Comparer.GetHashCode(_token);
        }

        /// <inheritdoc />
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            throw new NotSupportedException("Serializing GraphNodes in GraphSON2/GraphSON3 to JSON is not supported.");
        }

        /// <summary>
        /// Gets the a dictionary of properties of this node.
        /// </summary>
        public IDictionary<string, GraphNode> GetProperties()
        {
            return GetProperties<GraphNode>(_token);
        }

        /// <summary>
        /// Gets the a dictionary of properties of this node.
        /// </summary>
        public IDictionary<string, IGraphNode> GetIProperties()
        {
            return GetProperties<IGraphNode>(_token);
        }

        private IDictionary<string, T> GetProperties<T>(JToken item) where T : class, IGraphNode
        {
            var graphObject = GetTypedValue() ?? item as JObject;
            if (graphObject == null)
            {
                throw new InvalidOperationException($"Can not get properties from '{item}'");
            }
            return graphObject
                .Properties()
                .ToDictionary(prop => prop.Name, prop => new GraphNode(new GraphSONNode(_graphSONConverter, prop.Value)) as T);
        }

        /// <summary>
        /// Returns the representation of the <see cref="GraphNode"/> as an instance of the type provided.
        /// </summary>
        /// <exception cref="NotSupportedException">
        /// Throws NotSupportedException when the target type is not supported
        /// </exception>
        public object To(Type type)
        {
            return GetTokenValue(_token, type);
        }

        /// <summary>
        /// Returns the representation of the <see cref="GraphNode"/> as an instance of the type provided.
        /// </summary>
        /// <exception cref="NotSupportedException">
        /// Throws NotSupportedException when the target type is not supported
        /// </exception>
        public T To<T>()
        {
            return GetTokenValue<T>(_token);
        }

        /// <summary>
        /// Converts the instance into an array when the internal representation is a json array or a graphson list/set.
        /// </summary>
        public GraphNode[] ToArray()
        {
            return _graphSONConverter.FromDb<GraphNode[]>(_token);
        }

        /// <summary>
        /// Returns the json representation of the result.
        /// </summary>
        public override string ToString()
        {
            if (_token is JValue val)
            {
                return val.ToString(CultureInfo.InvariantCulture);
            }

            var tokenValue = _token[GraphSONTypeConverter.ValueKey];

            switch (tokenValue)
            {
                case null:
                    return string.Empty;

                case JValue tokenValueVal:
                    return tokenValueVal.ToString(CultureInfo.InvariantCulture);

                default:
                    return tokenValue.ToString();
            }
        }

        public void WriteJson(JsonWriter writer, JsonSerializer serializer)
        {
            throw new NotSupportedException("Serializing GraphNodes in GraphSON2/GraphSON3 to JSON is not supported.");
        }

        /// <inheritdoc />
        public string GetGraphSONType()
        {
            return _graphsonType;
        }
    }
}