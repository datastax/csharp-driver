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
        protected Element(GraphResult id, string label, IDictionary<string, GraphResult> properties)
        {
            Id = id;
            Label = label;
            Properties = properties;
        }

        /// <summary>
        /// Gets the identifier
        /// </summary>
        public GraphResult Id { get; private set; }

        /// <summary>
        /// Gets the label of the element
        /// </summary>
        public string Label { get; private set; }

        /// <summary>
        /// Gets the properties
        /// </summary>
        public IDictionary<string, GraphResult> Properties { get; private set; }
    }
}
