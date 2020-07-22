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

using Cassandra.DataStax.Graph;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.Dse
{
    internal class ListDeserializer : BaseDeserializer, IGraphSONDeserializer
    {
        private const string Prefix = "g";
        private const string TypeKey = "List";

        public ListDeserializer(Func<JToken, GraphNode> graphNodeFactory) : base(graphNodeFactory)
        {
        }

        public static string TypeName =>
            GraphSONUtil.FormatTypeName(ListDeserializer.Prefix, ListDeserializer.TypeKey);

        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            if (!(graphsonObject is JArray jArray))
            {
                return new GraphNode[0];
            }

            var result = new GraphNode[jArray.Count];
            for (var i = 0; i < result.Length; i++)
            {
                result[i] = ToGraphNode(jArray[i]);
            }

            return result;
        }
    }
}