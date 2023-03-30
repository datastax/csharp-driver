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

using System.Collections.Generic;

namespace Cassandra.DataStax.Graph
{
    /// <summary>
    /// Internal default implementation of <see cref="IVertexProperty"/>.
    /// </summary>
    internal class VertexProperty : Element, IVertexProperty
    {
        internal VertexProperty(
            GraphNode id, 
            string name, 
            IGraphNode value, 
            IGraphNode vertex, 
            IDictionary<string, GraphNode> properties)
            : base(id, name, properties)
        {
            Name = name;
            Value = value;
            Vertex = vertex;
        }

        public string Name { get; }

        public IGraphNode Value { get; }

        public IGraphNode Vertex { get; }

        public bool Equals(IProperty other)
        {
            return Equals((object) other);
        }

        protected bool Equals(VertexProperty other)
        {
            return string.Equals(Name, other.Name) 
                   && object.Equals(Value, other.Value) 
                   && object.Equals(Vertex, other.Vertex)
                   && string.Equals(Label, other.Label);
        }

        public bool Equals(IVertexProperty other)
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
            return Equals((VertexProperty) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Value != null ? Value.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Label != null ? Label.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Vertex != null ? Vertex.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}