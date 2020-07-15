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
    internal class PathDeserializer : BaseDeserializer, IGraphSONDeserializer
    {
        private const string Prefix = "g";
        private const string TypeKey = "Path";
        
        public PathDeserializer(Func<JToken, GraphNode> graphNodeFactory) : base(graphNodeFactory)
        {
        }
        
        public static string TypeName => 
            GraphSONUtil.FormatTypeName(PathDeserializer.Prefix, PathDeserializer.TypeKey);

        public dynamic Objectify(JToken token, GraphSONReader reader)
        {
            ICollection<ICollection<string>> labels = null;
            ICollection<GraphNode> objects = null;
            if (token["labels"] is JArray labelsProp)
            {
                // labels prop is a js Array<Array<string>>
                labels = labelsProp
                         .Select(node =>
                         {
                             var arrayNode = node as JArray;
                             if (arrayNode == null)
                             {
                                 throw new InvalidOperationException($"Cannot create an Path from {token}");
                             }
                             return new HashSet<string>(arrayNode.Select(n => n.ToString()));
                         })
                         .ToArray();
            }

            if (token["objects"] is JArray objectsProp)
            {
                // labels prop is a js Array<object>
                objects = objectsProp.Select(ToGraphNode).ToArray();
            }
            return new Path(labels, objects);
        }
    }
}