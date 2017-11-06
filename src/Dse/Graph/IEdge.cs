//
// Copyright (C) 2017 DataStax, Inc.
//
// Please see the license for details:
// http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Graph
{
    /// <summary>
    /// Represents an edge in DSE graph.
    /// </summary>
    public interface IEdge : IElement
    {
        /// <summary>
        /// Gets the incoming/head vertex.
        /// </summary>
        IGraphNode InV { get; }

        /// <summary>
        /// Gets the label of the incoming/head vertex.
        /// </summary>
        string InVLabel { get; }

        /// <summary>
        /// Gets the outgoing/tail vertex.
        /// </summary>
        IGraphNode OutV { get; }

        /// <summary>
        /// Gets the label of the outgoing/tail vertex.
        /// </summary>
        string OutVLabel { get; }
    }
}