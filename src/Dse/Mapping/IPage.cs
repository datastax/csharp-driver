//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Dse.Mapping
{
    /// <summary>
    /// Represents the result of a paged query, returned by manually paged query executions.
    /// </summary>
    public interface IPage<T> : ICollection<T>
    {
        /// <summary>
        /// Returns a token representing the state used to retrieve this results.
        /// </summary>
        byte[] CurrentPagingState { get; }
        /// <summary>
        /// Returns a token representing the state to retrieve the next page of results.
        /// </summary>
        byte[] PagingState { get; }
    }
}
