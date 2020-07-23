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

using Cassandra.DataStax.Graph;
using Cassandra.Serialization.Graph.Dse;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    internal class CustomGraphSON3Reader : CustomGraphSON2Reader
    {
        public CustomGraphSON3Reader(Func<JToken, GraphNode> graphNodeFactory) : base(graphNodeFactory)
        {
            var customGraphSon3SpecificDeserializers =
                new Dictionary<string, IGraphSONDeserializer>
                {
                    { Path3Deserializer.TypeName, new Path3Deserializer(GraphNodeFactory)},
                    { ListDeserializer.TypeName, new ListDeserializer(GraphNodeFactory) },
                    { SetDeserializer.TypeName, new SetDeserializer(GraphNodeFactory) },
                    { MapDeserializer.TypeName, new MapDeserializer(GraphNodeFactory) },
                    { BulkSetSerializer.TypeName, new BulkSetSerializer(GraphNodeFactory) }
                };

            foreach (var kv in customGraphSon3SpecificDeserializers)
            {
                Deserializers[kv.Key] = kv.Value;
            }
        }
    }
}