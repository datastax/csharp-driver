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

namespace Cassandra
{
    /// <summary>
    /// Represents a DSE cluster client that contains information and known state of a DSE cluster.
    /// </summary>
    public interface IDseCluster : ICluster
    {
        /// <summary>
        /// Gets the DSE cluster client configuration.
        /// </summary>
        new DseConfiguration Configuration { get; }

        /// <summary>
        /// Creates a new DSE session on this cluster and initializes it.
        /// </summary>
        /// <returns>A new <see cref="IDseSession"/> instance.</returns>
        new IDseSession Connect();

        /// <summary>
        /// Creates a new DSE session on this cluster, initializes it and sets the keyspace to the provided one.
        /// </summary>
        /// <param name="keyspace">The keyspace to connect to</param>
        /// <returns>A new <see cref="IDseSession"/> instance.</returns>
        new IDseSession Connect(string keyspace);

        /// <summary>
        /// Creates a new DSE session on this cluster.
        /// </summary>
        new Task<IDseSession> ConnectAsync();

        /// <summary>
        /// Creates a new DSE session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        new Task<IDseSession> ConnectAsync(string keyspace);
    }
}