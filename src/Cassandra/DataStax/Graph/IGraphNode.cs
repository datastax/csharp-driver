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

namespace Cassandra.DataStax.Graph
{
    public interface IGraphNode : IEquatable<IGraphNode>
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
        /// <typeparam name="T">Type of the property. Use dynamic for object trees.</typeparam>
        /// <param name="propertyName">Name of the property.</param>
        /// <exception cref="NotSupportedException">
        /// Throws NotSupportedException when the target type is not supported
        /// </exception>
        T Get<T>(string propertyName);

        /// <summary>
        /// Returns true if the property is defined in this instance.
        /// </summary>
        /// <exception cref="InvalidOperationException">When the underlying value is not an object tree</exception>
        bool HasProperty(string name);

        /// <summary>
        /// Returns the representation of the <see cref="GraphNode"/> as an instance of type T.
        /// </summary>
        /// <typeparam name="T">The type to which the current instance is going to be converted to.</typeparam>
        /// <exception cref="NotSupportedException">
        /// Throws NotSupportedException when the target type is not supported
        /// </exception>
        T To<T>();

        /// <summary>
        /// Returns the representation of the <see cref="GraphNode"/> as an instance of the type provided.
        /// </summary>
        /// <exception cref="NotSupportedException">
        /// Throws NotSupportedException when the target type is not supported
        /// </exception>
        object To(Type type);

        /// <summary>
        /// Gets the a dictionary of properties of this node.
        /// </summary>
        IDictionary<string, IGraphNode> GetProperties();

        /// <summary>
        /// Converts the instance into an array when the internal representation is a json array.
        /// </summary>
        IGraphNode[] ToIArray();

        /// <summary>
        /// Returns the representation of the result as a boolean.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        bool ToBoolean();

        /// <summary>
        /// Returns the representation of the result as a double.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        double ToDouble();

        /// <summary>
        /// Returns the representation of the result as a float.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        float ToFloat();

        /// <summary>
        /// Returns the representation of the result as an int.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        int ToInt32();
    }
}