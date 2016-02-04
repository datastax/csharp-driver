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
        public Vertex(GraphResult id, string label, IDictionary<string, GraphResult> properties) 
            : base(id, label, properties)
        {
        }
    }
}
