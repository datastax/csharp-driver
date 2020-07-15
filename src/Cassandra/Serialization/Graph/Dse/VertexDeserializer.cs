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
using System.Linq;

using Cassandra.DataStax.Graph;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.Dse
{
    internal class VertexDeserializer : BaseDeserializer, IGraphSONDeserializer
    {
        private const string Prefix = "g";
        private const string TypeKey = "Vertex";

        public const string DefaultLabel = "vertex";

        public VertexDeserializer(Func<JToken, GraphNode> graphNodeFactory) : base(graphNodeFactory)
        {
        }

        public static string TypeName => GraphSONUtil.FormatTypeName(VertexDeserializer.Prefix, VertexDeserializer.TypeKey);

        public dynamic Objectify(JToken token, GraphSONReader reader)
        {
            var properties = BaseDeserializer.EmptyProperties;
            var tokenProperties = !(token is JObject jobj) ? null : jobj["properties"];
            if (tokenProperties != null && tokenProperties is JObject propertiesJsonProp)
            {
                properties = propertiesJsonProp
                             .Properties()
                             .ToDictionary(prop => prop.Name, prop => ToGraphNode(prop.Value));
            }

            var label = ToString(token, "label", false) ?? VertexDeserializer.DefaultLabel;
            return new Vertex(
                ToGraphNode(token, "id", true),
                label,
                properties);
        }
    }
}