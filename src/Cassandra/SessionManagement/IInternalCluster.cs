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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.Requests;
using Cassandra.Serialization;

namespace Cassandra.SessionManagement
{
    /// <inheritdoc />
    internal interface IInternalCluster : ICluster
    {
        bool AnyOpenConnections(Host host);

        /// <summary>
        /// Gets the control connection used by the cluster
        /// </summary>
        IControlConnection GetControlConnection();

        /// <summary>
        /// Gets the the prepared statements cache
        /// </summary>
        ConcurrentDictionary<byte[], PreparedStatement> PreparedQueries { get; }

        /// <summary>
        /// Executes the prepare request on the first host selected by the load balancing policy.
        /// When <see cref="QueryOptions.IsPrepareOnAllHosts"/> is enabled, it prepares on the rest of the hosts in
        /// parallel.
        /// In case the statement was already in the prepared statements cache, logs an warning but prepares it anyway.
        /// </summary>
        Task<PreparedStatement> Prepare(IInternalSession session, ISerializerManager serializerManager, PrepareRequest request);
        
        IReadOnlyDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>> GetResolvedEndpoints();

        /// <summary>
        /// Helper method to retrieve the aggregate distance from all configured LoadBalancingPolicies and set it at Host level.
        /// </summary>
        HostDistance RetrieveAndSetDistance(Host host);
    }
}