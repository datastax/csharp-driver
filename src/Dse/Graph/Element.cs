//
//  Copyright (C) 2016-2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Linq;

namespace Dse.Graph
{
    /// <summary>
    /// Base class for vertices and edges
    /// </summary>
    public abstract class Element : IElement
    {
        public bool Equals(IElement other)
        {
            return Equals(Id, other?.Id);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((IElement) obj);
        }

        public override int GetHashCode()
        {
            return (Id != null ? Id.GetHashCode() : 0);
        }

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
        public GraphNode Id { get; }

        /// <summary>
        /// Gets the identifier
        /// </summary>
        IGraphNode IElement.Id => Id;

        /// <summary>
        /// Gets the label of the element
        /// </summary>
        public string Label { get; }

        /// <summary>
        /// Gets the properties
        /// </summary>
        public IDictionary<string, GraphNode> Properties { get; }

        /// <summary>
        /// Gets a property by name.
        /// </summary>
        public IProperty GetProperty(string name)
        {
            return Properties.TryGetValue(name, out var result) ? new Property(name, result) : null;
        }

        /// <summary>
        /// Gets all properties of an element.
        /// </summary>
        public IEnumerable<IProperty> GetProperties()
        {
            return Properties.Select(item => (IProperty)new Property(item.Key, item.Value));
        }
    }
}
