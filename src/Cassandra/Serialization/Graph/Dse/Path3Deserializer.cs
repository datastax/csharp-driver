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
    internal class Path3Deserializer : BaseDeserializer, IGraphSONDeserializer
    {
        private const string Prefix = "g";
        private const string TypeKey = "Path";
        
        public Path3Deserializer(Func<JToken, GraphNode> graphNodeFactory) : base(graphNodeFactory)
        {
        }
        
        public static string TypeName => 
            GraphSONUtil.FormatTypeName(Path3Deserializer.Prefix, Path3Deserializer.TypeKey);

        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            ICollection<ICollection<string>> labels = null;
            ICollection<GraphNode> objects = null;

            if (graphsonObject is JObject jObj)
            {
                labels = ParseLabels(jObj);
                objects = ParseObjects(jObj);
            }
            
            return new Path(labels, objects);
        }

        private ICollection<ICollection<string>> ParseLabels(JObject tokenObj)
        {
            if (tokenObj["labels"] is JObject labelsObj 
                && labelsObj[GraphSONTypeConverter.ValueKey] is JArray labelsArray)
            {
                return labelsArray
                       .Select(node =>
                       {
                          if (node is JObject nodeObj
                              && nodeObj[GraphSONTypeConverter.ValueKey] is JArray nodeArray)
                          {
                              return new HashSet<string>(nodeArray.Select(n => n.ToString()));
                          }

                          throw new InvalidOperationException($"Cannot create a Path from {tokenObj}");
                      })
                      .ToArray();
            }

            return null;
        }

        private ICollection<GraphNode> ParseObjects(JObject tokenObj)
        {
            if (tokenObj["objects"] is JObject objectsObj 
                && objectsObj[GraphSONTypeConverter.ValueKey] is JArray objectsArray)
            {
                return objectsArray.Select(ToGraphNode).ToArray();
            }

            return null;
        }
    }
}