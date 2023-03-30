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
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON1
{
    internal class GraphSON1TypeSerializer : IGraphTypeSerializer
    {
        private static readonly Func<Row, GraphNode> RowParser = 
            row => new GraphNode(new GraphSON1Node(row.GetValue<string>("gremlin"), false));

        public bool DefaultDeserializeGraphNodes => true;

        public GraphProtocol GraphProtocol => GraphProtocol.GraphSON1;

        /// <inheritdoc />
        public Func<Row, GraphNode> GetGraphRowParser()
        {
            return GraphSON1TypeSerializer.RowParser;
        }

        public object FromDb(JToken token, Type type)
        {
            throw new InvalidOperationException("Not supported.");
        }

        public object FromDb(JToken token, Type type, bool deserializeGraphNodes)
        {
            throw new InvalidOperationException("Not supported.");
        }

        public T FromDb<T>(JToken token)
        {
            throw new InvalidOperationException("Not supported.");
        }

        public string ToDb(object obj)
        {
            return JsonConvert.SerializeObject(obj, GraphSON1ContractResolver.Settings);
        }

        public bool ConvertFromDb(object obj, Type targetType, out dynamic result)
        {
            throw new InvalidOperationException("Not supported.");
        }
    }
}