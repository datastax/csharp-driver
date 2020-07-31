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

using System;
using System.Collections.Generic;
using System.Dynamic;
using Cassandra.DataStax.Graph;
using Newtonsoft.Json;

namespace Cassandra.Serialization.Graph
{
    /// <summary>
    /// Represents a xml node
    /// </summary>
    internal interface INode
    {
        /// <summary>
        /// Whether to deserialize nodes to GraphNodes when the requested type is object.
        /// </summary>
        bool DeserializeGraphNodes { get; }

        /// <summary>
        /// Returns true if the underlying value is an array.
        /// </summary>
        bool IsArray { get; }

        /// <summary>
        /// Returns true if the underlying value is an object tree.
        /// </summary>
        bool IsObjectTree { get; }

        /// <summary>
        /// Returns true if the underlying value is a scalar value (string, double, boolean, ...).
        /// </summary>
        bool IsScalar { get; }

        /// <summary>
        /// Gets the number of identical results represented by this instance.
        /// It represents the number of times it should be repeated. Defaults to 1.
        /// </summary>
        long Bulk { get; }

        /// <summary>
        /// Gets the typed value of a property of the result.
        /// </summary>
        T Get<T>(string propertyName, bool throwIfNotFound);

        int GetHashCode();

        /// <summary>
        /// Gets the a dictionary of properties of this node.
        /// </summary>
        IDictionary<string, GraphNode> GetProperties();

        /// <summary>
        /// Gets the a dictionary of properties of this node.
        /// </summary>
        IDictionary<string, IGraphNode> GetIProperties();

        /// <summary>
        /// Gets the raw data represented by this instance.
        /// </summary>
        dynamic GetRaw();

        /// <summary>
        /// Returns true if the property is defined in this instance.
        /// </summary>
        /// <exception cref="InvalidOperationException">When the underlying value is not an object tree</exception>
        bool HasProperty(string name);

        /// <summary>
        /// Returns the GraphSON type (@type property). Returns null if there isn't one.
        /// </summary>
        string GetGraphSONType();

        /// <summary>
        /// Returns the representation of the node as an instance of the type provided.
        /// </summary>
        object To(Type type);

        /// <summary>
        /// Returns the representation of the node as an instance of the type provided.
        /// </summary>
        T To<T>();

        /// <summary>
        /// Returns the current representation as an Array
        /// </summary>
        GraphNode[] ToArray();

        /// <summary>
        /// When the value is scalar, returns the string representation of the scalar.
        /// When the value is a object tree, it returns the string representation of the tree.
        /// </summary>
        string ToString();

        /// <summary>
        /// Provides the implementation for operations that get member values.
        /// </summary>
        bool TryGetMember(GetMemberBinder binder, out object result);

        void WriteJson(JsonWriter writer, JsonSerializer serializer);
        
        void GetObjectData(System.Runtime.Serialization.SerializationInfo info,
                           System.Runtime.Serialization.StreamingContext context);
    }
}