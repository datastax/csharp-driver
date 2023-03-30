//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.Collections.Generic;

namespace Cassandra.DataStax.Graph
{
    /// <summary>
    /// Represents an edge in DSE graph.
    /// </summary>
    public class Edge : Element, IEdge
    {
        /// <summary>
        /// Gets the incoming/head vertex.
        /// </summary>
        public GraphNode InV { get; }

        IGraphNode IEdge.InV => InV;

        /// <summary>
        /// Gets the label of the incoming/head vertex.
        /// </summary>
        public string InVLabel { get; }

        /// <summary>
        /// Gets the outgoing/tail vertex.
        /// </summary>
        public GraphNode OutV { get; }

        IGraphNode IEdge.OutV => OutV;

        /// <summary>
        /// Gets the label of the outgoing/tail vertex.
        /// </summary>
        public string OutVLabel { get; }

        /// <summary>
        /// Creates a new instance of <see cref="Edge"/>.
        /// </summary>
        public Edge(GraphNode id, string label, IDictionary<string, GraphNode> properties, 
            GraphNode inV, string inVLabel, GraphNode outV, string outVLabel)
            : base(id, label, properties)
        {
            InV = inV;
            InVLabel = inVLabel;
            OutV = outV;
            OutVLabel = outVLabel;
        }
    }
}
