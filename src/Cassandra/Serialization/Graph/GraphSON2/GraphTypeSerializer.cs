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
using System.Reflection;

using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Mapping.TypeConversion;
using Cassandra.Serialization.Graph.GraphSON3;
using Cassandra.Serialization.Graph.GraphSON3.Dse;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Cassandra.SessionManagement;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    /// <summary>
    /// <para>
    /// See xml docs on the <see cref="IGraphTypeSerializer"/> interface first.
    /// </para>
    /// <para>
    /// The <see cref="IGraphSONWriter"/> and <see cref="IGraphSONReader"/> interfaces are implemented by this class
    /// (<see cref="GraphTypeSerializer"/>) which is the point of entry for serialization and deserialization logic.
    /// </para>
    /// <para>
    /// The individual serializer and deserializer instances call the <see cref="GraphTypeSerializer"/> instance to
    /// serialize and deserialize inner properties.
    /// </para>
    /// </summary>
    internal class GraphTypeSerializer : IGraphTypeSerializer, IGraphSONWriter, IGraphSONReader
    {
        private static readonly TypeConverter DefaultTypeConverter = new DefaultTypeConverter();

        private static readonly IReadOnlyDictionary<string, IGraphSONDeserializer> EmptyDeserializersDict =
            new Dictionary<string, IGraphSONDeserializer>(0);

        private static readonly IReadOnlyDictionary<Type, IGraphSONSerializer> EmptySerializersDict =
            new Dictionary<Type, IGraphSONSerializer>(0);

        private static readonly IComplexTypeGraphSONDeserializer UdtDeserializer = new UdtGraphSONDeserializer();
        private static readonly IComplexTypeGraphSONSerializer UdtSerializer = new UdtGraphSONSerializer();
        private static readonly IComplexTypeGraphSONDeserializer TupleDeserializer = new TupleGraphSONDeserializer();
        private static readonly IComplexTypeGraphSONSerializer TupleSerializer = new TupleGraphSONSerializer();

        private readonly TypeConverter _typeConverter;
        private readonly ICustomGraphSONReader _reader;
        private readonly ICustomGraphSONWriter _writer;
        private readonly Func<Row, GraphNode> _rowParser;
        private readonly IInternalSession _session;

        public const string TypeKey = "@type";
        public const string ValueKey = "@value";

        public GraphTypeSerializer(
            IInternalSession session,
            GraphProtocol protocol,
            IReadOnlyDictionary<string, IGraphSONDeserializer> customDeserializers,
            IReadOnlyDictionary<Type, IGraphSONSerializer> customSerializers,
            bool deserializeGraphNodes)
        {
            _session = session;
            _typeConverter = GraphTypeSerializer.DefaultTypeConverter;
            DefaultDeserializeGraphNodes = deserializeGraphNodes;
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

        /// <inheritdoc />
        public bool DefaultDeserializeGraphNodes { get; }

        public GraphProtocol GraphProtocol { get; }

        /// <inheritdoc />
        public Func<Row, GraphNode> GetGraphRowParser()
        {
            return _rowParser;
        }

        /// <inheritdoc />
        public string ToDb(object obj)
        {
            return WriteObject(obj);
        }

        /// <inheritdoc />
        public T FromDb<T>(JToken token)
        {
            var type = typeof(T);
            if (TryDeserialize(token, type, DefaultDeserializeGraphNodes, out var result))
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

        /// <inheritdoc />
        public object FromDb(JToken token, Type type)
        {
            return FromDb(token, type, DefaultDeserializeGraphNodes);
        }

        /// <inheritdoc />
        public object FromDb(JToken token, Type type, bool deserializeGraphNodes)
        {
            if (TryDeserialize(token, type, deserializeGraphNodes, out var result))
            {
                return result;
            }

            try
            {
                return Convert.ChangeType(result, type);
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
                typeName = ((string)token[GraphSONTokens.TypeKey]) ?? string.Empty;
            }

            if (TryConvertFromListOrSet(token, type, typeName, useGraphNodes, out result))
            {
                return true;
            }

            if (TryConvertFromMap(token, type, typeName, useGraphNodes, out result))
            {
                return true;
            }

            if (TryConvertFromBulkSet(token, type, typeName, useGraphNodes, out result))
            {
                return true;
            }

            if (TryConvertFromUdt(token, type, typeName, out result))
            {
                return true;
            }

            if (TryConvertFromTuple(token, type, typeName, out result))
            {
                return true;
            }

            return ConvertFromDb(_reader.ToObject(token), type, out result);
        }

        private bool TryConvertFromListOrSet(JToken token, Type type, string typeName, bool deserializeGraphNodes, out dynamic result)
        {
            if (!(token is JArray) && !typeName.Equals("g:List") && !typeName.Equals("g:Set"))
            {
                result = null;
                return false;
            }

            Type elementType;
            var createSet = false;
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
                elementType = typeof(object);
                createSet = typeName.Equals("g:Set");
            }

            if (!(token is JArray))
            {
                return createSet
                    ? ConvertFromDb(FromSetToEnumerable((JArray)token[GraphSONTokens.ValueKey], deserializeGraphNodes), type, out result)
                    : ConvertFromDb(FromListOrSetToEnumerable((JArray)token[GraphSONTokens.ValueKey], elementType, deserializeGraphNodes), type, out result);
            }

            return ConvertFromDb(FromListOrSetToEnumerable((JArray)token, elementType, deserializeGraphNodes), type, out result);
        }

        private bool TryConvertFromMap(JToken token, Type type, string typeName, bool deserializeGraphNodes, out dynamic result)
        {
            if (!typeName.Equals("g:Map"))
            {
                result = null;
                return false;
            }

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
                keyType = typeof(object);
                elementType = typeof(object);
            }

            return ConvertFromDb(FromMapToDictionary((JArray)token[GraphSONTokens.ValueKey], keyType, elementType, deserializeGraphNodes), type, out result);
        }

        private bool TryConvertFromBulkSet(JToken token, Type type, string typeName, bool deserializeGraphNodes, out dynamic result)
        {
            if (!typeName.Equals("g:BulkSet"))
            {
                result = null;
                return false;
            }

            Type elementType;

            if (type.GetTypeInfo().IsGenericType
                && (type.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)
                    || type.GetGenericTypeDefinition() == typeof(Dictionary<,>)
                    || type.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
            {
                var genericArgs = type.GetTypeInfo().GetGenericArguments();
                var keyType = genericArgs[0];
                elementType = genericArgs[1];
                return ConvertFromDb(FromMapToDictionary((JArray)token[GraphSONTokens.ValueKey], keyType, elementType, deserializeGraphNodes), type, out result);
            }
            else if (type.GetTypeInfo().IsGenericType
                     && (TypeConverter.ListGenericInterfaces.Contains(type.GetGenericTypeDefinition())
                         || type.GetGenericTypeDefinition() == typeof(IList<>)
                         || type.GetGenericTypeDefinition() == typeof(List<>)))
            {
                elementType = type.GetTypeInfo().GetGenericArguments()[0];
            }
            else
            {
                elementType = typeof(object);
            }

            var map = FromMapToDictionary((JArray)token[GraphSONTokens.ValueKey], elementType, typeof(int), deserializeGraphNodes);
            var length = map.Values.Cast<int>().Sum();
            var arr = Array.CreateInstance(elementType, length);
            var idx = 0;
            foreach (var key in map.Keys)
            {
                for (var i = 0; i < (int)map[key]; i++)
                {
                    arr.SetValue(key, idx);
                    idx++;
                }
            }
            return ConvertFromDb(arr, type, out result);
        }

        private bool TryConvertFromUdt(JToken token, Type type, string typeName, out dynamic result)
        {
            if (!typeName.Equals("dse:UDT"))
            {
                result = null;
                return false;
            }

            result = GraphTypeSerializer.UdtDeserializer.Objectify(
                token[GraphSONTokens.ValueKey],
                type,
                this,
                _session.InternalCluster.Metadata.ControlConnection.Serializer.GetGenericSerializer());
            return true;
        }

        private bool TryConvertFromTuple(JToken token, Type type, string typeName, out dynamic result)
        {
            if (!typeName.Equals("dse:Tuple"))
            {
                result = null;
                return false;
            }

            result = GraphTypeSerializer.TupleDeserializer.Objectify(
                token[GraphSONTokens.ValueKey],
                type,
                this,
                _session.InternalCluster.Metadata.ControlConnection.Serializer.GetGenericSerializer());
            return true;
        }

        public bool ConvertFromDb(object obj, Type targetType, out dynamic result)
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
            var converter = _typeConverter.TryGetFromDbConverter(objType, targetType);
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

        private IEnumerable FromListOrSetToEnumerable(JArray jArray, Type elementType, bool deserializeGraphNodes)
        {
            var arr = Array.CreateInstance(elementType, jArray.Count);
            var isGraphNode = elementType == typeof(GraphNode) || elementType == typeof(IGraphNode);
            for (var i = 0; i < jArray.Count; i++)
            {
                var value = isGraphNode
                    ? new GraphNode(new GraphSONNode(this, jArray[i]))
                    : FromDb(jArray[i], elementType, deserializeGraphNodes);
                arr.SetValue(value, i);
            }
            return arr;
        }

        private IEnumerable FromSetToEnumerable(JArray jArray, bool deserializeGraphNodes)
        {
            return new HashSet<object>(jArray.Select(e => FromDb(e, typeof(object), deserializeGraphNodes)));
        }

        private IDictionary FromMapToDictionary(JArray jArray, Type keyType, Type elementType, bool deserializeGraphNodes)
        {
            var newDictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(keyType, elementType));
            var keyIsGraphNode = keyType == typeof(GraphNode) || keyType == typeof(IGraphNode);
            var elementIsGraphNode = elementType == typeof(GraphNode) || elementType == typeof(IGraphNode);

            for (var i = 0; i < jArray.Count; i += 2)
            {
                var value = elementIsGraphNode
                    ? new GraphNode(new GraphSONNode(this, jArray[i + 1]))
                    : FromDb(jArray[i + 1], elementType, deserializeGraphNodes);

                var key = keyIsGraphNode
                    ? new GraphNode(new GraphSONNode(this, jArray[i]))
                    : FromDb(jArray[i], keyType, deserializeGraphNodes);

                newDictionary.Add(key, value);
            }

            return newDictionary;
        }

        public dynamic ToDict(dynamic objectData)
        {
            if (_writer.TryToDict(objectData, out dynamic result))
            {
                return result;
            }

            if (GraphProtocol != GraphProtocol.GraphSON3)
            {
                return objectData;
            }

            var genericSerializer = _session.InternalCluster.Metadata.ControlConnection.Serializer.GetGenericSerializer();
            if (GraphTypeSerializer.UdtSerializer.TryDictify(objectData, this, genericSerializer, out result))
            {
                return result;
            }

            if (GraphTypeSerializer.TupleSerializer.TryDictify(objectData, this, genericSerializer, out result))
            {
                return result;
            }

            return objectData;
        }

        public string WriteObject(dynamic objectData)
        {
            return JsonConvert.SerializeObject(ToDict(objectData), GraphSONNode.GraphSONSerializerSettings);
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