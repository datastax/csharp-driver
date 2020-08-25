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
    internal class TupleGraphSONSerializer : IComplexTypeGraphSONSerializer
    {
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

            var tupleType = (Type)objectData.GetType();
            if (!Utils.IsTuple(tupleType))
            {
                result = null;
                return false;
            }

            var tupleTypeInfo = tupleType.GetTypeInfo();
            var subtypes = tupleTypeInfo.GetGenericArguments();
            var data = new List<object>();
            for (var i = 1; i <= subtypes.Length; i++)
            {
                var prop = tupleTypeInfo.GetProperty("Item" + i);
                if (prop != null)
                {
                    data.Add(prop.GetValue(objectData, null));
                }
            }

            var dict = new Dictionary<string, dynamic>
            {
                { "cqlType", "tuple" },
                { 
                    "definition", 
                    data.Select(elem => ComplexTypeDefinitionHelper.GetDefinitionByValue(genericSerializer, elem)) },
                { "value", data.Select(d => serializer.ToDict(d)) }
            };

            result = GraphSONUtil.ToTypedValue("Tuple", dict, "dse");
            return true;
        }
    }
}