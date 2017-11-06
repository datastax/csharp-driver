// 
// Copyright (C) 2017 DataStax, Inc.
// 
// Please see the license for details:
// http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;

namespace Dse.Graph
{
    /// <summary>
    /// Internal default implementation of <see cref="IVertexProperty"/>.
    /// </summary>
    internal class VertexProperty : Element, IVertexProperty
    {
        internal VertexProperty(GraphNode id, string name, IGraphNode value, IDictionary<string, GraphNode> properties)
            : base(id, name, properties)
        {
            Name = name;
            Value = value;
        }

        public string Name { get; }

        public IGraphNode Value { get; }

        public bool Equals(IProperty other)
        {
            return Equals((object) other);
        }

        protected bool Equals(VertexProperty other)
        {
            return string.Equals(Name, other.Name) && Equals(Value, other.Value) && string.Equals(Label, other.Label);
        }

        public bool Equals(IVertexProperty other)
        {
            return Equals((object) other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != GetType())
            {
                return false;
            }
            return Equals((VertexProperty) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Value != null ? Value.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Label != null ? Label.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}