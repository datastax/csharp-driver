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
using Cassandra.Serialization.Graph.GraphSON2;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON3.Dse
{
    /// <inheritdoc />
    internal class UdtGraphSONDeserializer : IComplexTypeGraphSONDeserializer
    {
        /// <inheritdoc />
        public dynamic Objectify(
            JToken graphsonObject, Type type, IGraphTypeSerializer serializer, IGenericSerializer genericSerializer)
        {
            var keyspace = serializer.FromDb<string>(graphsonObject["keyspace"]);
            var name = serializer.FromDb<string>(graphsonObject["name"]);
            var values = (JArray) graphsonObject["value"];

            var targetTypeIsDictionary = false;
            Type elementType = null;
            if (type.GetTypeInfo().IsGenericType
                && (type.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)
                    || type.GetGenericTypeDefinition() == typeof(Dictionary<,>)
                    || type.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
            {
                targetTypeIsDictionary = true;
                var genericArgs = type.GetTypeInfo().GetGenericArguments();
                if (genericArgs[0] != typeof(string))
                {
                    throw new InvalidOperationException(
                        "Deserializing UDT to Dictionary is only supported when the dictionary key is of type \"string\".");
                }
                elementType = genericArgs[1];
            }

            UdtMap udtMap = null;
            bool readToDictionary;
            if (targetTypeIsDictionary)
            {
                readToDictionary = true;
            }
            else
            {
                udtMap = genericSerializer.GetUdtMapByName($"{keyspace}.{name}");
                if (udtMap != null)
                {
                    readToDictionary = false;
                }
                else
                {
                    readToDictionary = true;
                    elementType = typeof(object);
                }
            }

            var obj = readToDictionary 
                ? ToDictionary(serializer, elementType, (JArray) graphsonObject["definition"], values) 
                : ToObject(serializer, udtMap, values);
            
            if (!serializer.ConvertFromDb(obj, type, out var result))
            {
                throw new InvalidOperationException($"Could not convert UDT from type {obj.GetType().FullName} to {type.FullName}");
            }

            return result;
        }
        
        internal object ToObject(IGraphTypeSerializer serializer, UdtMap map, IEnumerable<JToken> valuesArr)
        {
            var obj = Activator.CreateInstance(map.NetType);
            var i = 0;
            foreach (var value in valuesArr)
            {
                if (i >= map.Definition.Fields.Count)
                {
                    break;
                }
                
                var field = map.Definition.Fields[i];
                i++;

                var prop = map.GetPropertyForUdtField(field.Name);

                if (prop == null)
                {
                    continue;
                }
                
                var convertedValue = serializer.FromDb(value, prop.PropertyType, false);
                prop.SetValue(obj, convertedValue, null);
            }

            return obj;
        }
        
        internal object ToDictionary(
            IGraphTypeSerializer serializer, Type elementType, IEnumerable<JToken> definitions, IEnumerable<JToken> valuesArr)
        {
            var fieldNames = definitions.Select(def => (string) def["fieldName"]).ToArray();
            var newDictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(typeof(string), elementType));
            var elementIsGraphNode = elementType == typeof(GraphNode) || elementType == typeof(IGraphNode);

            var i = 0;
            foreach (var value in valuesArr)
            {
                var newValue = elementIsGraphNode
                    ? new GraphNode(new GraphSONNode(serializer, value))
                    : serializer.FromDb(value, elementType, false);
                var key = fieldNames[i];
                i++;

                newDictionary.Add(key, newValue);
            }

            return newDictionary;
        }
    }
}