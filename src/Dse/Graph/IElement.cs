//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;

namespace Dse.Graph
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