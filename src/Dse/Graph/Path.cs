//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse.Graph
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
