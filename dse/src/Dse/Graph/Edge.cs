//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
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
        public GraphNode InV { get; private set; }

        /// <summary>
        /// Gets the label of the incoming/head vertex.
        /// </summary>
        public string InVLabel { get; private set; }

        /// <summary>
        /// Gets the outgoing/tail vertex.
        /// </summary>
        public GraphNode OutV { get; private set; }

        /// <summary>
        /// Gets the label of the outgoing/tail vertex.
        /// </summary>
        public string OutVLabel { get; private set; }

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
