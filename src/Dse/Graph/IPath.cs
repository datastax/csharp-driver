//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Dse.Graph
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