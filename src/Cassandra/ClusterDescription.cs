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

namespace Cassandra
{
    /// <summary>
    /// Provides a snapshot of cluster metadata properties.
    /// </summary>
    public class ClusterDescription
    {
        internal ClusterDescription(
            string clusterName, bool isDbaas, ProtocolVersion protocolVersion)
        {
            ClusterName = clusterName;
            IsDbaas = isDbaas;
            ProtocolVersion = protocolVersion;
        }

        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public string ClusterName { get; }

        /// <summary>
        /// Determines whether the cluster is provided as a service (DataStax Astra).
        /// </summary>
        public bool IsDbaas { get; }

        /// <summary>
        /// Gets the Cassandra native binary protocol version that was
        /// negotiated between the driver and the Cassandra node.
        /// </summary>
        public ProtocolVersion ProtocolVersion { get; }
    }
}