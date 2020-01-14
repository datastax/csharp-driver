//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
namespace Cassandra.DataStax.Graph
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
            return string.Equals(Name, other.Name) && object.Equals(Value, other.Value);
        }

        public bool Equals(IProperty other)
        {
            return Equals((object) other);
        }

        public override bool Equals(object obj)
        {
            if (object.ReferenceEquals(null, obj))
            {
                return false;
            }
            if (object.ReferenceEquals(this, obj))
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