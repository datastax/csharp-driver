// 
// Copyright (C) 2017 DataStax, Inc.
// 
// Please see the license for details:
// http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Graph
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
        
    }
}