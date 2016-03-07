using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse.Graph
{
    /// <summary>
    /// Represents an edge in DSE graph.
    /// </summary>
    public class Edge : Element
    {
        /// <summary>
        /// Gets the incoming/head vertex.
        /// </summary>
        public GraphResult InV { get; private set; }

        /// <summary>
        /// Gets the label of the incoming/head vertex.
        /// </summary>
        public string InVLabel { get; private set; }

        /// <summary>
        /// Gets the outgoing/tail vertex.
        /// </summary>
        public GraphResult OutV { get; private set; }

        /// <summary>
        /// Gets the label of the outgoing/tail vertex.
        /// </summary>
        public string OutVLabel { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="Edge"/>.
        /// </summary>
        public Edge(GraphResult id, string label, IDictionary<string, GraphResult> properties, 
            GraphResult inV, string inVLabel, GraphResult outV, string outVLabel)
            : base(id, label, properties)
        {
            InV = inV;
            InVLabel = inVLabel;
            OutV = outV;
            OutVLabel = outVLabel;
        }
    }
}
