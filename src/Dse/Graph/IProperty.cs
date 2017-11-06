//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Graph
{
    /// <summary>
    /// Represents a property in DSE Graph.
    /// </summary>
    public interface IProperty : IEquatable<IProperty>
    {
        /// <summary>
        /// Gets the property name.
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// Gets the property value.
        /// </summary>
        IGraphNode Value { get; }
    }
}