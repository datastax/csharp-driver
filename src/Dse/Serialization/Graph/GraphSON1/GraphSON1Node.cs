// 
// Copyright (C) 2017 DataStax, Inc.
// 
// Please see the license for details:
// http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Dse.Graph;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Serialization.Graph.GraphSON1
{
    internal class GraphSON1Node : INode
    {
        private static readonly JsonSerializer Serializer =
            JsonSerializer.CreateDefault(GraphSON1ContractResolver.Settings);
        
        private static readonly JTokenEqualityComparer Comparer = new JTokenEqualityComparer();
        
        private readonly JToken _token;

        public bool IsArray => _token is JArray;

        public bool IsObjectTree => _token is JObject;

        public bool IsScalar => _token is JValue;

        internal GraphSON1Node(string json)
        {
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }
            var parsedJson = (JObject)JsonConvert.DeserializeObject(json);
            _token = parsedJson["result"];
        }

        internal GraphSON1Node(JToken parsedGraphItem)
        {
            _token = parsedGraphItem ?? throw new ArgumentNullException(nameof(parsedGraphItem));
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

        private JToken GetPropertyValue(string name, bool throwIfNotFound)
        {
            if (!(_token is JObject))
            {
                if (_token is JValue)
                {
                    throw new KeyNotFoundException("Cannot retrieve properties of scalar value of type '{0}'" + ((JValue)_token).Type);
                }
                throw new KeyNotFoundException("Cannot retrieve properties of scalar value");
            }
            var graphObject = (JObject)_token;
            var property = graphObject.Property(name);
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

        /// <summary>
        /// Gets the raw data represented by this instance.
        /// <para>
        /// Raw internal representation might be different depending on the graph serialization format and
        /// it is subject to change without any prior notice.
        /// </para>
        /// </summary>
        public dynamic GetRaw()
        {
            return _token;
        }

        /// <summary>
        /// Returns either a scalar value or an array representing the token value, performing conversions when required.
        /// </summary>
        private T GetTokenValue<T>(JToken token)
        {
            return (T)GetTokenValue(token, typeof(T));
        }

        private object GetTokenValue(JToken token, Type type)
        {
            try
            {
                if (type == typeof(object) || type == typeof(GraphNode))
                {
                    return new GraphNode(new GraphSON1Node(token));
                }
                if (token is JValue || token is JObject)
                {
                    if (type == typeof(TimeUuid))
                    {
                        // TimeUuid is not Serializable but convertible from Uuid
                        return (TimeUuid) token.ToObject<Guid>();
                    }
                    return token.ToObject(type, Serializer);
                }
                if (token is JArray)
                {
                    Type elementType = null;
                    if (type.IsArray)
                    {
                        elementType = type.GetElementType();
                    }
                    else if (type.GetTypeInfo().IsGenericType &&
                             type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    {
                        elementType = type.GetTypeInfo().GetGenericArguments()[0];
                    }
                    return ToArray((JArray) token, elementType);
                }
            }
            catch (JsonSerializationException ex)
            {
                throw new NotSupportedException($"Type {type} is not supported", ex);
            }
            catch (JsonReaderException ex)
            {
                throw new InvalidOperationException($"Could not convert to {type}: {token}", ex);
            }
            throw new NotSupportedException($"Token of type {token.GetType()} is not supported");
        }

        /// <summary>
        /// Returns either a JSON supported scalar value, a GraphNode or an Array of GraphNodes.
        /// </summary>
        private object GetTokenValue(JToken token)
        {
            if (token is JValue)
            {
                return ((JValue)token).Value;
            }
            if (token is JObject)
            {
                return new GraphNode(new GraphSON1Node(token));
            }
            if (token is JArray)
            {
                return ToArray((JArray)token);
            }
            throw new NotSupportedException($"Token of type {token.GetType()} is not supported");
        }

        /// <summary>
        /// Returns true if the property is defined in this instance.
        /// </summary>
        /// <exception cref="InvalidOperationException">When the underlying value is not an object tree</exception>
        public bool HasProperty(string name)
        {
            if (!(_token is JObject))
            {
                return false;
            }
            return ((JObject) _token).Property(name) != null;
        }

        /// <summary>
        /// Provides the implementation for operations that get member values.
        /// </summary>
        public bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = GetTokenValue(GetPropertyValue(binder.Name, false));
            return true;
        }

        /// <summary>
        /// Gets the hash code for this instance, based on its value.
        /// </summary>
        public override int GetHashCode()
        {
            return Comparer.GetHashCode(_token);
        }

#if !NETCORE
        /// <inheritdoc />
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (!IsObjectTree)
            {
                throw new NotSupportedException("Deserialization of GraphNodes that don't represent object trees is not supported");
            }
            foreach (var prop in ((JObject) _token).Properties())
            {
                info.AddValue(prop.Name, prop.Value);
            }
        }
#endif

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
            if (!(item is JObject))
            {
                throw new InvalidOperationException($"Can not get properties from '{item}'");
            }
            return ((JObject) item)
                .Properties()
                .ToDictionary(prop => prop.Name, prop => new GraphNode(new GraphSON1Node(prop.Value)) as T);
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

        public Array ToArray(JArray jArray, Type elementType = null)
        {
            if (elementType == null)
            {
                elementType = typeof (GraphNode);
            }
            var arr = Array.CreateInstance(elementType, jArray.Count);
            var isGraphNode = elementType == typeof (GraphNode);
            for (var i = 0; i < arr.Length; i++)
            {
                var value = isGraphNode
                    ? new GraphNode(new GraphSON1Node(jArray[i]))
                    : jArray[i].ToObject(elementType, Serializer);
                arr.SetValue(value, i);
            }
            return arr;
        }

        /// <summary>
        /// Converts the instance into an array when the internal representation is a json array.
        /// </summary>
        public GraphNode[] ToArray()
        {
            if (!(_token is JArray))
            {
                throw new InvalidOperationException($"Cannot convert to array from {_token}");
            }
            return (GraphNode[])ToArray((JArray) _token);
        }

        /// <summary>
        /// Returns the json representation of the result.
        /// </summary>
        public override string ToString()
        {
            return _token.ToString();
        }

        public void WriteJson(JsonWriter writer, JsonSerializer serializer)
        {
            if (!IsObjectTree)
            {
                throw new NotSupportedException(
                    "Deserialization of GraphNodes that don't represent object trees is not supported");
            }
            serializer.Serialize(writer, _token);
        }
    }
}