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
    /// <summary>
    /// Represents an element in DSE Graph.
    /// </summary>
    public interface IElement : IEquatable<IElement>
    {
        /// <summary>
        /// Gets the label of the element.
        /// </summary>
        string Label { get; }

        /// <summary>
        /// Gets the identifier as an instance of <see cref="IGraphNode"/>.
        /// </summary>
        IGraphNode Id { get; }

        /// <summary>
        /// Gets a property by name.
        /// </summary>
        IProperty GetProperty(string name);

        /// <summary>
        /// Gets all properties of an element.
        /// </summary>
        IEnumerable<IProperty> GetProperties();
    }
}