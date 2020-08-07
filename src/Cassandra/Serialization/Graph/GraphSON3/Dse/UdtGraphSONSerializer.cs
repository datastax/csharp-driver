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
using System.Reflection;

using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.GraphSON3.Dse
{
    /// <inheritdoc />
    internal class UdtGraphSONSerializer : IUdtGraphSONSerializer
    {
        /// <inheritdoc />
        public bool TryDictify(
            dynamic objectData,
            IGraphSONWriter serializer,
            IGenericSerializer genericSerializer,
            out Dictionary<string, dynamic> result)
        {
            if (objectData == null)
            {
                result = null;
                return false;
            }

            var type = (Type)objectData.GetType();
            var map = genericSerializer.GetUdtMapByType(type);

            if (map == null)
            {
                result = null;
                return false;
            }

            var dict = GetUdtTypeDefinition(map, genericSerializer);

            var value = (object)objectData;
            var values = new List<object>();

            foreach (var field in map.Definition.Fields)
            {
                object fieldValue = null;
                var prop = map.GetPropertyForUdtField(field.Name);
                var fieldTargetType = genericSerializer.GetClrTypeForGraph(field.TypeCode, field.TypeInfo);
                if (prop != null)
                {
                    fieldValue = prop.GetValue(value, null);
                    if (!fieldTargetType.GetTypeInfo().IsAssignableFrom(prop.PropertyType.GetTypeInfo()))
                    {
                        fieldValue = UdtMap.TypeConverter.ConvertToDbFromUdtFieldValue(prop.PropertyType,
                            fieldTargetType,
                            fieldValue);
                    }
                }

                values.Add(fieldValue);
            }

            dict.Add("value", values.Select(serializer.ToDict));
            result = GraphSONUtil.ToTypedValue("UDT", dict, "dse");
            return true;
        }

        private Dictionary<string, dynamic> GetUdtTypeDefinition(UdtMap map, IGenericSerializer genericSerializer)
        {
            return GetUdtTypeDefinition(new Dictionary<string, dynamic> { { "cqlType", "udt" } }, map, genericSerializer);
        }

        private Dictionary<string, dynamic> GetUdtTypeDefinition(
            Dictionary<string, dynamic> dictionary, UdtMap map, IGenericSerializer genericSerializer)
        {
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

        private Dictionary<string, dynamic> GetDefinitionByType(
            IGenericSerializer genericSerializer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return GetDefinitionByType(new Dictionary<string, dynamic>(), genericSerializer, typeCode, typeInfo);
        }

        private Dictionary<string, dynamic> GetDefinitionByType(
            Dictionary<string, dynamic> dictionary, IGenericSerializer genericSerializer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            dictionary.Add("cqlType", typeCode.ToString().ToLower());

            if (typeInfo is UdtColumnInfo udtTypeInfo)
            {
                var udtMap = genericSerializer.GetUdtMapByName(udtTypeInfo.Name);
                if (udtMap == null)
                {
                    throw new InvalidOperationException($"Could not find UDT mapping for {udtTypeInfo.Name}");
                }
                return GetUdtTypeDefinition(dictionary, genericSerializer.GetUdtMapByName(udtTypeInfo.Name), genericSerializer);
            }

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