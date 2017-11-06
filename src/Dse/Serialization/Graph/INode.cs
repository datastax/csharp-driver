//
// Copyright (C) 2017 DataStax, Inc.
//
// Please see the license for details:
// http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Dynamic;
using Dse.Graph;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Serialization.Graph
{
    /// <summary>
    /// Represents a xml node
    /// </summary>
    internal interface INode
    {
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
        /// Returns the representation of the node as an instance of the type provided.
        /// </summary>
        object To(Type type);

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
        
#if !NETCORE
        void GetObjectData(System.Runtime.Serialization.SerializationInfo info,
                           System.Runtime.Serialization.StreamingContext context);
#endif
    }
}