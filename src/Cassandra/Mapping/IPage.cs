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

namespace Cassandra.Mapping
{
    /// <summary>
    /// Represents the result of a paged query, returned by manually paged query executions.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Naming", 
        "CA1710:Identifiers should have correct suffix", 
        Justification = "Public API")]
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
