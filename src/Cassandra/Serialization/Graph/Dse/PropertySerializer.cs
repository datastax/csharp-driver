﻿//
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
using Cassandra.DataStax.Graph;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.Dse
{
    internal class PropertySerializer : IGraphSONSerializer
    {
        public Dictionary<string, dynamic> Dictify(dynamic objectData, GraphSONWriter writer)
        {
            if (objectData is IPropertyWithElement propertyWithElement)
            {
                var elementDict = CreateElementDict(propertyWithElement.Element, writer);
                var valueDict = new Dictionary<string, dynamic>
                {
                    {"key", propertyWithElement.Name},
                    {"value", writer.ToDict(propertyWithElement.Value)},
                    {"element", elementDict}
                };
                return GraphSONUtil.ToTypedValue(nameof(Property), valueDict);
            }
            else
            {
                IProperty property = objectData;
                var valueDict = new Dictionary<string, dynamic>
                {
                    {"key", property.Name},
                    {"value", writer.ToDict(property.Value)}
                };
                return GraphSONUtil.ToTypedValue(nameof(Property), valueDict);
            }
        }

        private dynamic CreateElementDict(IGraphNode graphNode, GraphSONWriter writer)
        {
            if (graphNode == null)
            {
                return null;
            }
            
            var serializedElement = writer.ToDict(graphNode);
            Dictionary<string, dynamic> elementDict = serializedElement;
            if (elementDict.ContainsKey(GraphSONTokens.ValueKey))
            {
                var elementValueSerialized = elementDict[GraphSONTokens.ValueKey];
                Dictionary<string, dynamic> elementValueDict = elementValueSerialized;
                if (elementValueDict != null)
                {
                    elementValueDict.Remove("outVLabel");
                    elementValueDict.Remove("inVLabel");
                    elementValueDict.Remove("properties");
                    elementValueDict.Remove("value");
                }
            }
            return serializedElement;
        }
    }
}