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

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// <para>
    /// Information and known state of a Cassandra cluster.
    /// </para>
    /// <para>
    /// This is the main entry point of the driver. A simple example of access to a
    /// Cassandra cluster would be:
    /// </para>
    /// <code>
    /// Cluster cluster = Cluster.Builder().AddContactPoint("192.168.0.1").Build(); 
    /// Session session = cluster.Connect("db1"); 
    /// foreach (var row in session.Execute("SELECT * FROM table1")) 
    ///     // do something ...
    /// </code>
    /// <para>
    /// A cluster object maintains a permanent connection to one of the cluster node
    /// that it uses solely to maintain information on the state and current topology of the cluster.
    /// Using the connection, the driver will discover all the nodes composing the cluster as well as
    /// new nodes joining the cluster.
    /// </para>
    /// </summary>
    public interface ICluster : IDisposable
    {
        /// <summary>
        ///  Gets read-only metadata on the connected cluster. 
        /// <para>This includes the
        ///  know nodes (with their status as seen by the driver) as well as the schema
        ///  definitions.
        /// </para>
        /// <para>This method may trigger the creation of a connection if none has been established yet.
        /// </para>
        /// </summary>
        Metadata Metadata { get; }

        /// <summary>
        /// Cluster client configuration
        /// </summary>
        Configuration Configuration { get; }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        ICollection<Host> AllHosts();

        /// <summary>
        /// Event that gets triggered when a new host is added to the cluster
        /// </summary>
        event Action<Host> HostAdded;

        /// <summary>
        /// Event that gets triggered when a host has been removed from the cluster
        /// </summary>
        event Action<Host> HostRemoved;

        /// <summary>
        ///  Creates a new session on this cluster.
        /// </summary>
        /// <returns>a new session on this cluster set to no keyspace.</returns>
        ISession Connect();

        /// <summary>
        ///  Creates a new session on this cluster and sets a keyspace to use.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use.</param>
        /// <returns>a new session on this cluster set to keyspace: <c>keyspaceName</c>. </returns>
        ISession Connect(string keyspace);

        /// <summary>
        /// Creates a new session on this cluster.
        /// </summary>
        Task<ISession> ConnectAsync();

        /// <summary>
        /// Creates a new session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        Task<ISession> ConnectAsync(string keyspace);

        /// <summary>
        /// Get the host instance for a given Ip address.
        /// </summary>
        /// <param name="address">Ip address of the host</param>
        /// <returns>The host or null if not found</returns>
        Host GetHost(IPEndPoint address);

        /// <summary>
        /// Gets a collection of replicas for a given partitionKey. Backward-compatibility only, use GetReplicas(keyspace, partitionKey) instead.
        /// </summary>
        /// <param name="partitionKey">Byte array representing the partition key</param>
        /// <returns></returns>
        ICollection<Host> GetReplicas(byte[] partitionKey);

        /// <summary>
        /// Gets a collection of replicas for a given partitionKey on a given keyspace
        /// </summary>
        /// <param name="keyspace">Byte array representing the partition key</param>
        /// <param name="partitionKey">Byte array representing the partition key</param>
        /// <returns></returns>
        ICollection<Host> GetReplicas(string keyspace, byte[] partitionKey);

        /// <summary>
        ///  Shutdown this cluster instance. This closes all connections from all the
        ///  sessions of this <c>* Cluster</c> instance and reclaim all resources
        ///  used by it. <p> This method has no effect if the cluster was already shutdown.</p>
        /// </summary>
        void Shutdown(int timeoutMs = Timeout.Infinite);
        
        /// <summary>
        ///  Shutdown this cluster instance asynchronously. This closes all connections from all the
        ///  sessions of this <c>* Cluster</c> instance and reclaim all resources
        ///  used by it. <p> This method has no effect if the cluster was already shutdown.</p>
        /// </summary>
        Task ShutdownAsync(int timeoutMs = Timeout.Infinite);
        
        /// <summary>
        /// Updates keyspace metadata (including token metadata for token aware routing) for a given keyspace or a specific keyspace table.
        /// If no keyspace is provided then this method will update the metadata and token map for all the keyspaces of the cluster.
        /// </summary>
        Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null);
        
        /// <summary>
        /// Updates keyspace metadata (including token metadata for token aware routing) for a given keyspace or a specific keyspace table.
        /// If no keyspace is provided then this method will update the metadata and token map for all the keyspaces of the cluster.
        /// </summary>
        bool RefreshSchema(string keyspace = null, string table = null);
    }
}
