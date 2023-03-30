#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System.Collections.Generic;
using System.Linq;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.GraphSON2.Structure
{
    internal class VertexSerializer : IGraphSONSerializer
    {
        public Dictionary<string, dynamic> Dictify(dynamic objectData, IGraphSONWriter writer)
        {
            Vertex vertex = objectData;
            var vertexDict = new Dictionary<string, dynamic>
            {
                {"id", writer.ToDict(vertex.Id)},
                {"label", writer.ToDict(vertex.Label)}
            };

            if (vertex.Properties != null && vertex.Properties.Count > 0)
            {
                vertexDict["properties"] = vertex.Properties.ToDictionary(kvp => kvp.Key, kvp => writer.ToDict(kvp.Value));
            }

            return GraphSONUtil.ToTypedValue(nameof(Vertex), vertexDict);
        }
    }
}