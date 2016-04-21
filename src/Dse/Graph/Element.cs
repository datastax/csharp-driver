using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse.Graph
{
    /// <summary>
    /// Base class for vertices and edges
    /// </summary>
    public abstract class Element
    {
        /// <summary>
        /// Creates a new instance of a Graph <see cref="Element"/>.
        /// </summary>
        protected Element(GraphNode id, string label, IDictionary<string, GraphNode> properties)
        {
            Id = id;
            Label = label;
            Properties = properties;
        }

        /// <summary>
        /// Gets the identifier
        /// </summary>
        public GraphNode Id { get; private set; }

        /// <summary>
        /// Gets the label of the element
        /// </summary>
        public string Label { get; private set; }

        /// <summary>
        /// Gets the properties
        /// </summary>
        public IDictionary<string, GraphNode> Properties { get; private set; }
    }
}
