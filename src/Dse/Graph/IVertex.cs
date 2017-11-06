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
    /// Represents a Vertex in DSE Graph.
    /// </summary>
    public interface IVertex : IElement
    {
        /// <summary>
        /// Gets the first property of this element that has the given name, or null if the property 
        /// does not exist.
        /// <para>
        /// If more than one property of this element has the given name, it will return one of them 
        /// (unspecified order).
        /// </para>
        /// </summary>
        new IVertexProperty GetProperty(string name);

        /// <summary>
        /// Gets the properties of this element that has the given name or an empty iterator if not found.
        /// </summary>
        IEnumerable<IVertexProperty> GetProperties(string name);

        /// <summary>
        /// Gets the properties of this element.
        /// </summary>
        new IEnumerable<IVertexProperty> GetProperties();
    }
}