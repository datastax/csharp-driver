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
using Cassandra.Mapping.TypeConversion;
using Cassandra.Serialization.Graph.GraphSON2;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph
{
    internal class GraphSONTypeConverter : IGraphSONTypeConverter
    {
        private readonly TypeConverter _typeConverter;
        private readonly GraphSONReader _reader;
        private readonly GraphSONWriter _writer;

        public const string TypeKey = "@type";
        public const string ValueKey = "@value";
        
        public static IGraphSONTypeConverter NewGraphSON2Converter(TypeConverter typeConverter)
        {
            return new GraphSONTypeConverter(typeConverter, GraphProtocol.GraphSON2);
        }
        
        public static IGraphSONTypeConverter NewGraphSON3Converter(TypeConverter typeConverter)
        {
            return new GraphSONTypeConverter(typeConverter, GraphProtocol.GraphSON3);
        }

        public GraphSONTypeConverter(
            TypeConverter typeConverter, GraphProtocol protocol)
        {
            _typeConverter = typeConverter;

            switch (protocol)
            {
                case GraphProtocol.GraphSON2:
                    _reader = new CustomGraphSON2Reader(token => new GraphNode(new GraphSONNode(this, token)));
                    _writer = new CustomGraphSON2Writer();
                    break;
                case GraphProtocol.GraphSON3:
                    _reader = new CustomGraphSON3Reader(token => new GraphNode(new GraphSONNode(this, token)));
                    _writer = new CustomGraphSON3Writer();
                    break;
                default:
                    throw new ArgumentException($"Can not create graph type converter for {protocol.GetInternalRepresentation()}");
            }
        }

        public string ToDb(object obj)
        {
            return _writer.WriteObject(obj);
        }

        public T FromDb<T>(JToken token)
        {
            var type = typeof(T);
            if (TryConvert(token, type, out var result))
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
            if (TryConvert(token, type, out var result))
            {
                return result;
            }

            // No converter is available but the types don't match, so attempt to do:
            //     (TFieldOrProp) row.GetValue<T>(columnIndex);
            try
            {
                var expr = (ConstantExpression) Expression.Constant(result);
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

        private bool TryConvert(JToken token, Type type, out dynamic result)
        {
            if (type == typeof(object) || type == typeof(GraphNode) || type == typeof(IGraphNode))
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
                typeName = (string)token[GraphSONTokens.TypeKey];
            }

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

            return ConvertFromDb(_reader.ToObject(token), type, out result);
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
            if (elementType == null)
            {
                elementType = typeof(GraphNode);
            }

            var arr = (IList) Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType));

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
            if (elementType == null)
            {
                elementType = typeof(GraphNode);
            }
            if (keyType == null)
            {
                elementType = typeof(GraphNode);
            }
            
            var newDictionary = (IDictionary) Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(keyType, elementType));
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
    }
}