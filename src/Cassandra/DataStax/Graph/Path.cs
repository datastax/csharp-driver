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
    public class Path : IPath
    {
        /// <summary>
        /// Returns the sets of labels of the steps traversed by this path, or an empty list, if this path is empty.
        /// </summary>
        public ICollection<ICollection<string>> Labels { get; protected set; }

        /// <summary>
        /// Returns the objects traversed by this path, or an empty list, if this path is empty.
        /// </summary>
        public ICollection<GraphNode> Objects { get; protected set; }

        ICollection<IGraphNode> IPath.Objects => (ICollection<IGraphNode>) Objects;

        /// <summary>
        /// Creates a new instance of <see cref="Path"/>.
        /// </summary>
        /// <param name="labels">The sets of labels of the steps traversed by this path.</param>
        /// <param name="objects">The objects traversed by this path</param>
        public Path(ICollection<ICollection<string>> labels, ICollection<GraphNode> objects)
        {
            Labels = labels;
            Objects = objects;
        }
    }
}
