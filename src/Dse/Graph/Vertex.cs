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
