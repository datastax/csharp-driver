// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Allows for fetching of metadata about the connected cluster, including known nodes and schema definitions.
    /// </summary>
    public interface IMetadata : IMetadataSnapshotProvider
    {
        Task<ClusterDescription> GetClusterDescriptionAsync();

        ClusterDescription GetClusterDescription();
        
        Host GetHost(IPEndPoint address);

        Task<Host> GetHostAsync(IPEndPoint address);

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        ICollection<Host> AllHosts();

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        Task<ICollection<Host>> AllHostsAsync();

        IEnumerable<IPEndPoint> AllReplicas();

        Task<IEnumerable<IPEndPoint>> AllReplicasAsync();

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        ICollection<Host> GetReplicas(string keyspaceName, byte[] partitionKey);

        /// <summary>
        /// Get the replicas for a given partition key
        /// </summary>
        ICollection<Host> GetReplicas(byte[] partitionKey);

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        Task<ICollection<Host>> GetReplicasAsync(string keyspaceName, byte[] partitionKey);

        /// <summary>
        /// Get the replicas for a given partition key
        /// </summary>
        Task<ICollection<Host>> GetReplicasAsync(byte[] partitionKey);

        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// <returns>the metadata of the requested keyspace or <c>null</c> if
        ///  <c>* keyspace</c> is not a known keyspace.</returns>
        KeyspaceMetadata GetKeyspace(string keyspace);

        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// <returns>the metadata of the requested keyspace or <c>null</c> if
        ///  <c>* keyspace</c> is not a known keyspace.</returns>
        Task<KeyspaceMetadata> GetKeyspaceAsync(string keyspace);

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        ICollection<string> GetKeyspaces();

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        Task<ICollection<string>> GetKeyspacesAsync();

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        ICollection<string> GetTables(string keyspace);

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        Task<ICollection<string>> GetTablesAsync(string keyspace);

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        TableMetadata GetTable(string keyspace, string tableName);

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        Task<TableMetadata> GetTableAsync(string keyspace, string tableName);

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        MaterializedViewMetadata GetMaterializedView(string keyspace, string name);

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        Task<MaterializedViewMetadata> GetMaterializedViewAsync(string keyspace, string name);

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        UdtColumnInfo GetUdtDefinition(string keyspace, string typeName);

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName);

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        FunctionMetadata GetFunction(string keyspace, string name, string[] signature);

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        Task<FunctionMetadata> GetFunctionAsync(string keyspace, string name, string[] signature);

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        AggregateMetadata GetAggregate(string keyspace, string name, string[] signature);

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        Task<AggregateMetadata> GetAggregateAsync(string keyspace, string name, string[] signature);

        /// <summary>
        /// Updates keyspace metadata (including token metadata for token aware routing) for a given keyspace or a specific keyspace table.
        /// If no keyspace is provided then this method will update the metadata and token map for all the keyspaces of the cluster.
        /// </summary>
        bool RefreshSchema(string keyspace = null, string table = null);

        /// <summary>
        /// Updates keyspace metadata (including token metadata for token aware routing) for a given keyspace or a specific keyspace table.
        /// If no keyspace is provided then this method will update the metadata and token map for all the keyspaces of the cluster.
        /// </summary>
        Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null);

        /// <summary>
        /// Initiates a schema agreement check.
        /// <para/>
        /// Schema changes need to be propagated to all nodes in the cluster.
        /// Once they have settled on a common version, we say that they are in agreement.
        /// <para/>
        /// This method does not perform retries so
        /// <see cref="ProtocolOptions.MaxSchemaAgreementWaitSeconds"/> does not apply.
        /// </summary>
        /// <returns>True if schema agreement was successful and false if it was not successful.</returns>
        Task<bool> CheckSchemaAgreementAsync();
    }
}