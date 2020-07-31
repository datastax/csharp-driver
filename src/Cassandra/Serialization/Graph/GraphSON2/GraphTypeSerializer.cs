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
using System.Linq.Expressions;
using System.Reflection;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Mapping.TypeConversion;
using Cassandra.Serialization.Graph.GraphSON3;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    internal class GraphTypeSerializer : IGraphTypeSerializer, IGraphSONWriter, IGraphSONReader
    {
        private static readonly IReadOnlyDictionary<string, IGraphSONDeserializer> EmptyDeserializersDict = 
            new Dictionary<string, IGraphSONDeserializer>(0);
        
        private static readonly IReadOnlyDictionary<Type, IGraphSONSerializer> EmptySerializersDict = 
            new Dictionary<Type, IGraphSONSerializer>(0);

        private readonly TypeConverter _typeConverter;
        private readonly ICustomGraphSONReader _reader;
        private readonly ICustomGraphSONWriter _writer;
        private readonly Func<Row, GraphNode> _rowParser;

        public const string TypeKey = "@type";
        public const string ValueKey = "@value";

        public GraphTypeSerializer(
            TypeConverter typeConverter,
            GraphProtocol protocol,
            IReadOnlyDictionary<string, IGraphSONDeserializer> customDeserializers,
            IReadOnlyDictionary<Type, IGraphSONSerializer> customSerializers,
            bool deserializeGraphNodes)
        {
            _typeConverter = typeConverter;
            DeserializeGraphNodes = deserializeGraphNodes;
            GraphProtocol = protocol;

            customDeserializers = customDeserializers ?? GraphTypeSerializer.EmptyDeserializersDict;
            customSerializers = customSerializers ?? GraphTypeSerializer.EmptySerializersDict;

            switch (protocol)
            {
                case GraphProtocol.GraphSON2:
                    _reader = new CustomGraphSON2Reader(token => new GraphNode(new GraphSONNode(this, token)), customDeserializers, this);
                    _writer = new CustomGraphSON2Writer(customSerializers, this);
                    break;

                case GraphProtocol.GraphSON3:
                    _reader = new CustomGraphSON3Reader(token => new GraphNode(new GraphSONNode(this, token)), customDeserializers, this);
                    _writer = new CustomGraphSON3Writer(customSerializers, this);
                    break;

                default:
                    throw new ArgumentException($"Can not create graph type serializer for {protocol.GetInternalRepresentation()}");
            }

            _rowParser = row => new GraphNode(new GraphSONNode(this, row.GetValue<string>("gremlin")));
        }

        public bool DeserializeGraphNodes { get; }

        public GraphProtocol GraphProtocol { get; }

        /// <inheritdoc />
        public Func<Row, GraphNode> GetGraphRowParser()
        {
            return _rowParser;
        }

        public string ToDb(object obj)
        {
            return _writer.WriteObject(obj);
        }

        public T FromDb<T>(JToken token)
        {
            var type = typeof(T);
            if (TryDeserialize(token, type, DeserializeGraphNodes, out var result))
            {
                return (T)result;
            }

            // No converter is available but the types don't match, so attempt to cast
            try
            {
                return (T)result;
            }
            catch (Exception ex)
            {
                var message = result == null
                    ? $"It is not possible to convert NULL to target type {type.FullName}"
                    : $"It is not possible to convert type {result.GetType().FullName} to target type {type.FullName}";

                throw new InvalidOperationException(message, ex);
            }
        }

        public object FromDb(JToken token, Type type)
        {
            if (TryDeserialize(token, type, DeserializeGraphNodes, out var result))
            {
                return result;
            }

            // No converter is available but the types don't match, so attempt to do:
            //     (TFieldOrProp) row.GetValue<T>(columnIndex);
            try
            {
                var expr = (ConstantExpression)Expression.Constant(result);
                var convert = Expression.Convert(expr, type);
                return Expression.Lambda(convert).Compile().DynamicInvoke();
            }
            catch (Exception ex)
            {
                var message = result == null
                    ? $"It is not possible to convert NULL to target type {type.FullName}"
                    : $"It is not possible to convert type {result.GetType().FullName} to target type {type.FullName}";
                throw new InvalidOperationException(message, ex);
            }
        }

        private bool TryDeserialize(JToken token, Type type, bool useGraphNodes, out dynamic result)
        {
            if ((type == typeof(object) && useGraphNodes) || type == typeof(GraphNode) || type == typeof(IGraphNode))
            {
                result = new GraphNode(new GraphSONNode(this, token));
                return true;
            }

            if (token is JValue)
            {
                return ConvertFromDb(_reader.ToObject(token), type, out result);
            }

            var typeName = string.Empty;
            if (token is JObject)
            {
                typeName = ((string) token[GraphSONTokens.TypeKey]) ?? string.Empty;
            }

            if (TryConvertFromListOrSet(token, type, typeName, out result))
            {
                return true;
            }

            if (TryConvertFromMap(token, type, typeName, out result))
            {
                return true;
            }

            if (TryConvertFromBulkSet(token, type, typeName, out result))
            {
                return true;
            }

            return ConvertFromDb(_reader.ToObject(token), type, out result);
        }

        private bool TryConvertFromListOrSet(JToken token, Type type, string typeName, out dynamic result)
        {
            if (token is JArray || typeName.Equals("g:List") || typeName.Equals("g:Set"))
            {
                Type elementType = null;
                if (type.IsArray)
                {
                    elementType = type.GetElementType();
                }
                else if (type.GetTypeInfo().IsGenericType
                         && (TypeConverter.ListGenericInterfaces.Contains(type.GetGenericTypeDefinition())
                             || type.GetGenericTypeDefinition() == typeof(ISet<>)
                             || type.GetGenericTypeDefinition() == typeof(IList<>)
                             || type.GetGenericTypeDefinition() == typeof(HashSet<>)
                             || type.GetGenericTypeDefinition() == typeof(SortedSet<>)
                             || type.GetGenericTypeDefinition() == typeof(List<>)))
                {
                    elementType = type.GetTypeInfo().GetGenericArguments()[0];
                }
                else
                {
                    throw new InvalidOperationException($"Can not deserialize a collection to type {type.FullName}");
                }

                if (!(token is JArray))
                {
                    return ConvertFromDb(FromListOrSetToEnumerable((JArray)token[GraphSONTokens.ValueKey], elementType), type, out result);
                }

                return ConvertFromDb(FromListOrSetToEnumerable((JArray)token, elementType), type, out result);
            }

            result = null;
            return false;
        }

        private bool TryConvertFromMap(JToken token, Type type, string typeName, out dynamic result)
        {
            if (typeName.Equals("g:Map"))
            {
                Type keyType;
                Type elementType;
                if (type.GetTypeInfo().IsGenericType
                    && (type.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)
                        || type.GetGenericTypeDefinition() == typeof(Dictionary<,>)
                        || type.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
                {
                    var genericArgs = type.GetTypeInfo().GetGenericArguments();
                    keyType = genericArgs[0];
                    elementType = genericArgs[1];
                }
                else
                {
                    throw new InvalidOperationException($"Can not deserialize a collection to type {type.FullName}");
                }

                return ConvertFromDb(FromMapToDictionary((JArray)token[GraphSONTokens.ValueKey], keyType, elementType), type, out result);
            }

            result = null;
            return false;
        }

        private bool TryConvertFromBulkSet(JToken token, Type type, string typeName, out dynamic result)
        {
            if (typeName.Equals("g:BulkSet"))
            {
                Type keyType;
                Type elementType;

                if (type.GetTypeInfo().IsGenericType
                    && (type.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)
                        || type.GetGenericTypeDefinition() == typeof(Dictionary<,>)
                        || type.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
                {
                    var genericArgs = type.GetTypeInfo().GetGenericArguments();
                    keyType = genericArgs[0];
                    elementType = genericArgs[1];
                    return ConvertFromDb(FromMapToDictionary((JArray)token[GraphSONTokens.ValueKey], keyType, elementType), type, out result);
                }

                if (type.GetTypeInfo().IsGenericType
                    && (TypeConverter.ListGenericInterfaces.Contains(type.GetGenericTypeDefinition())
                        || type.GetGenericTypeDefinition() == typeof(IList<>)
                        || type.GetGenericTypeDefinition() == typeof(List<>)))
                {
                    elementType = type.GetTypeInfo().GetGenericArguments()[0];
                    var map = FromMapToDictionary((JArray)token[GraphSONTokens.ValueKey], elementType, typeof(int));
                    var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType));
                    foreach (var key in map.Keys)
                    {
                        for (var i = 0; i < (int)map[key]; i++)
                        {
                            list.Add(key);
                        }
                    }
                    return ConvertFromDb(list, type, out result);
                }

                throw new InvalidOperationException($"Can not deserialize a collection to type {type.FullName}");
            }

            result = null;
            return false;
        }

        private bool ConvertFromDb(object obj, Type targetType, out dynamic result)
        {
            if (obj == null)
            {
                result = null;

                // return true if type supports null
                return !targetType.IsValueType || (Nullable.GetUnderlyingType(targetType) != null);
            }

            var objType = obj.GetType();

            if (targetType == objType || targetType.IsAssignableFrom(objType))
            {
                // No casting/conversion needed
                result = obj;
                return true;
            }

            // Check for a converter
            Delegate converter = _typeConverter.TryGetFromDbConverter(objType, targetType);
            if (converter == null)
            {
                result = obj;
                return false;
            }

            // Invoke the converter function on getValueT (taking into account whether it's a static method):
            //     converter(row.GetValue<T>(columnIndex));
            result = converter.DynamicInvoke(obj);
            return true;
        }

        private IEnumerable FromListOrSetToEnumerable(JArray jArray, Type elementType)
        {
            var arr = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType));

            var isGraphNode = elementType == typeof(GraphNode) || elementType == typeof(IGraphNode);
            for (var i = 0; i < jArray.Count; i++)
            {
                var value = isGraphNode
                    ? new GraphNode(new GraphSONNode(this, jArray[i]))
                    : FromDb(jArray[i], elementType);
                arr.Add(value);
            }
            return arr;
        }

        private IDictionary FromMapToDictionary(JArray jArray, Type keyType, Type elementType)
        {
            var newDictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(keyType, elementType));
            var keyIsGraphNode = keyType == typeof(GraphNode) || keyType == typeof(IGraphNode);
            var elementIsGraphNode = elementType == typeof(GraphNode) || elementType == typeof(IGraphNode);

            for (var i = 0; i < jArray.Count; i += 2)
            {
                var value = elementIsGraphNode
                    ? new GraphNode(new GraphSONNode(this, jArray[i + 1]))
                    : FromDb(jArray[i + 1], elementType);

                var key = keyIsGraphNode
                    ? new GraphNode(new GraphSONNode(this, jArray[i]))
                    : FromDb(jArray[i], keyType);

                newDictionary.Add(key, value);
            }

            return newDictionary;
        }

        public dynamic ToDict(dynamic objectData)
        {
            return _writer.ToDict(objectData);
        }

        public string WriteObject(dynamic objectData)
        {
            return _writer.WriteObject(objectData);
        }

        public dynamic ToObject(JToken token)
        {
            if (TryDeserialize(token, typeof(object), false, out var result))
            {
                return result;
            }

            throw new InvalidOperationException($"It is not possible to deserialize {token.ToString()}");
        }
    }
}