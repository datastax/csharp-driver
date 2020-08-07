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
    internal class UdtGraphSONSerializer : IComplexTypeGraphSONSerializer
    {
        /// <inheritdoc />
        public bool TryDictify(
            dynamic objectData,
            IGraphSONWriter serializer,
            IGenericSerializer genericSerializer,
            out dynamic result)
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
            return ComplexTypeDefinitionHelper.GetUdtTypeDefinition(
                new Dictionary<string, dynamic>(), map, genericSerializer);
        }
    }
}