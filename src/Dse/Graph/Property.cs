//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
namespace Dse.Graph
{
    /// <summary>
    /// Internal default implementation of a property.
    /// </summary>
    internal class Property : IProperty
    {
        public string Name { get; }

        public IGraphNode Value { get; }

        internal Property(string name, IGraphNode value)
        {
            Name = name;
            Value = value;
        }

        protected bool Equals(Property other)
        {
            return string.Equals(Name, other.Name) && Equals(Value, other.Value);
        }

        public bool Equals(IProperty other)
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
            return Equals((Property) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (Value != null ? Value.GetHashCode() : 0);
            }
        }
    }
}