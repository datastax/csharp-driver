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
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.GraphSON2.Structure;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2.Tinkerpop
{
    internal class TraverserDeserializer : BaseStructureDeserializer, IGraphSONStructureDeserializer
    {
        private const string Prefix = "g";
        private const string TypeKey = "Traverser";
        
        public static string TypeName => 
            GraphSONUtil.FormatTypeName(TraverserDeserializer.Prefix, TraverserDeserializer.TypeKey);

        public dynamic Objectify(JToken graphsonObject, Func<JToken, GraphNode> factory, IGraphSONReader reader)
        {
            long bulkObj = reader.ToObject(graphsonObject["bulk"]);
            var valueObj = ToGraphNode(factory, graphsonObject, "value");
            return new Traverser(valueObj, bulkObj);
        }
    }
}