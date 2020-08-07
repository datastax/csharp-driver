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

namespace Cassandra.Serialization.Graph.GraphSON3.Dse
{
    internal static class ComplexTypeDefinitionHelper
    {
        public static Dictionary<string, dynamic> GetUdtTypeDefinition(
            Dictionary<string, dynamic> dictionary, UdtMap map, IGenericSerializer genericSerializer)
        {
            dictionary.Add("cqlType", "udt");
            dictionary.Add("keyspace", map.Keyspace);
            dictionary.Add("name", map.IgnoreCase ? map.UdtName.ToLowerInvariant() : map.UdtName);
            dictionary.Add("definition", map.Definition.Fields.Select(
                c => GetDefinitionByType(
                    new Dictionary<string, dynamic> { { "fieldName", c.Name } },
                    genericSerializer,
                    c.TypeCode,
                    c.TypeInfo)));
            return dictionary;
        }

        public static Dictionary<string, dynamic> GetDefinitionByValue(
            IGenericSerializer genericSerializer, dynamic obj)
        {
            var objType = (Type) obj.GetType();
            var typeCode = genericSerializer.GetCqlType(objType, out var typeInfo);
            return GetDefinitionByType(new Dictionary<string, dynamic>(), genericSerializer, typeCode, typeInfo);
        }
        
        private static Dictionary<string, dynamic> GetDefinitionByType(
            IGenericSerializer genericSerializer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return GetDefinitionByType(new Dictionary<string, dynamic>(), genericSerializer, typeCode, typeInfo);
        }

        private static Dictionary<string, dynamic> GetDefinitionByType(
            Dictionary<string, dynamic> dictionary, IGenericSerializer genericSerializer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (typeInfo is UdtColumnInfo udtTypeInfo)
            {
                var udtMap = genericSerializer.GetUdtMapByName(udtTypeInfo.Name);
                if (udtMap == null)
                {
                    throw new InvalidOperationException($"Could not find UDT mapping for {udtTypeInfo.Name}");
                }
                return GetUdtTypeDefinition(dictionary, genericSerializer.GetUdtMapByName(udtTypeInfo.Name), genericSerializer);
            }

            dictionary.Add("cqlType", typeCode.ToString().ToLower());

            if (typeInfo is TupleColumnInfo tupleColumnInfo)
            {
                dictionary.Add(
                    "definition",
                    tupleColumnInfo.Elements.Select(c => GetDefinitionByType(genericSerializer, c.TypeCode, c.TypeInfo)));
            }

            if (typeInfo is MapColumnInfo mapColumnInfo)
            {
                dictionary.Add(
                    "definition",
                    new[] {
                        GetDefinitionByType(genericSerializer, mapColumnInfo.KeyTypeCode, mapColumnInfo.KeyTypeInfo),
                        GetDefinitionByType(genericSerializer, mapColumnInfo.ValueTypeCode, mapColumnInfo.ValueTypeInfo)
                    });
            }

            if (typeInfo is ListColumnInfo listColumnInfo)
            {
                dictionary.Add(
                    "definition",
                    new[] { GetDefinitionByType(
                        genericSerializer, listColumnInfo.ValueTypeCode, listColumnInfo.ValueTypeInfo) });
            }

            if (typeInfo is SetColumnInfo setColumnInfo)
            {
                dictionary.Add(
                    "definition",
                    new[] { GetDefinitionByType(
                        genericSerializer, setColumnInfo.KeyTypeCode, setColumnInfo.KeyTypeInfo) });
            }

            return dictionary;
        }
    }
}