//
//  Copyright (C) 2017 DataStax, Inc.
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
    /// Represents a vertex in DSE graph.
    /// </summary>
    public class Vertex : Element
    {
        /// <summary>
        /// Creates a new <see cref="Vertex"/> instance.
        /// </summary>
        public Vertex(GraphNode id, string label, IDictionary<string, GraphNode> properties) 
            : base(id, label, properties)
        {
        }
    }
}
