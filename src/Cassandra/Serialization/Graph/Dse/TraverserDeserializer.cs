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
    internal class TraverserDeserializer : BaseDeserializer, IGraphSONDeserializer
    {
        private const string Prefix = "g";
        private const string TypeKey = "Traverser";
        
        public TraverserDeserializer(Func<JToken, GraphNode> graphNodeFactory) : base(graphNodeFactory)
        {
        }
        
        public static string TypeName => 
            GraphSONUtil.FormatTypeName(TraverserDeserializer.Prefix, TraverserDeserializer.TypeKey);

        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            long bulkObj = reader.ToObject(graphsonObject["bulk"]);
            var valueObj = ToGraphNode(graphsonObject, "value");
            return new Traverser(valueObj, bulkObj);
        }
    }
}