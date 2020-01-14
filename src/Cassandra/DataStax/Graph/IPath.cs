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
    /// Represents a walk through a graph as defined by a traversal.
    /// </summary>
    public interface IPath
    {
        /// <summary>
        /// Gets the sets of labels of the steps traversed by this path, or an empty list, if this path is empty.
        /// </summary>
        ICollection<ICollection<string>> Labels { get; }
        
        /// <summary>
        /// Gets the objects traversed by this path, or an empty list, if this path is empty.
        /// </summary>
        ICollection<IGraphNode> Objects { get; }
    }
}