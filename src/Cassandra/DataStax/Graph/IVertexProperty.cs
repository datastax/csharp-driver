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

namespace Cassandra.DataStax.Graph
{
    /// <summary>
    /// Represents a vertex property in DSE Graph.
    /// <para>
    /// Vertex properties are special because they are also elements, and thus have an identifier; they can also
    /// contain properties of their own (usually referred to as "meta properties").
    /// </para>
    /// </summary>
    public interface IVertexProperty : IProperty, IElement, IEquatable<IVertexProperty>
    {
        /// <summary>
        ///     The <see cref="IVertex" /> that owns this <see cref="IVertexProperty" />.
        /// </summary>
        IGraphNode Vertex { get; }
    }
}