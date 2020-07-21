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
using Cassandra.DataStax.Graph;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.Dse
{
    internal class EdgeDeserializer : BaseDeserializer, IGraphSONDeserializer
    {
        private const string Prefix = "g";
        private const string TypeKey = "Edge";
        
        public EdgeDeserializer(Func<JToken, GraphNode> graphNodeFactory) : base(graphNodeFactory)
        {
        }
        
        public static string TypeName => 
            GraphSONUtil.FormatTypeName(EdgeDeserializer.Prefix, EdgeDeserializer.TypeKey);

        public dynamic Objectify(JToken token, GraphSONReader reader)
        {
            IDictionary<string, GraphNode> properties = null;
            var tokenProperties = !(token is JObject jobj) ? null : jobj["properties"];
            if (tokenProperties != null && tokenProperties is JObject propertiesJsonProp)
            {
                properties = propertiesJsonProp
                             .Properties()
                             .ToDictionary(prop => prop.Name, prop => ToGraphNode(prop.Value));
            }

            return new Edge(
                ToGraphNode(token, "id", true),
                ToString(token, "label") ?? "edge",
                properties ?? new Dictionary<string, GraphNode>(0),
                ToGraphNode(token, "inV"),
                ToString(token, "inVLabel") ?? VertexDeserializer.DefaultLabel,
                ToGraphNode(token, "outV"),
                ToString(token, "outVLabel") ?? VertexDeserializer.DefaultLabel);
        }
    }
}