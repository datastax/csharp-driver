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

using System.Threading.Tasks;

namespace Cassandra.Connections
{
    /// <summary>
    /// Builds instances of <see cref="IConnectionEndPoint"/> from <see cref="Host"/> instances.
    /// The endpoints are used to create connections.
    /// </summary>
    internal interface IEndPointResolver
    {
        bool CanBeResolved { get; }

        /// <summary>
        /// Gets an instance of <see cref="IConnectionEndPoint"/> to the provided host from the internal cache (if caching is supported by the implementation).
        /// </summary>
        /// <param name="host">Host related to the new endpoint.</param>
        /// <param name="refreshCache">Whether to refresh the internal cache. If it is false and the cache is populated then
        /// no round trip will occur.</param>
        /// <returns>Endpoint.</returns>
        Task<IConnectionEndPoint> GetConnectionEndPointAsync(Host host, bool refreshCache);
    }
}