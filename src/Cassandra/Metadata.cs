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

using Cassandra.Connections.Control;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    ///  Keeps metadata on the connected cluster, including known nodes and schema
    ///  definitions.
    /// </summary>
    public class Metadata
    {
        private const int Disposed = 10;
        private const int Initialized = 5;
        private const int Initializing = 1;
        private const int NotInitialized = 0;

        private readonly InternalMetadata _internalMetadata;
        private readonly int _queryAbortTimeout;
        private volatile Task _initTask;

        private long _state = Metadata.NotInitialized;

        internal InternalMetadata InternalMetadata => _internalMetadata;

        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;

        /// <summary>
        /// Event that gets triggered when a new host is added to the cluster
        /// </summary>
        public event Action<Host> HostAdded;

        /// <summary>
        /// Event that gets triggered when a host has been removed from the cluster
        /// </summary>
        public event Action<Host> HostRemoved;

        internal Metadata(
            IInternalCluster cluster,
            Configuration configuration,
            ISerializerManager serializerManager,
            IEnumerable<IContactPoint> parsedContactPoints)
        {
            _queryAbortTimeout = configuration.DefaultRequestOptions.QueryAbortTimeout;
            _internalMetadata = new InternalMetadata(cluster, this, configuration, serializerManager, parsedContactPoints);
        }

        internal Metadata(
            IInternalCluster cluster,
            Configuration configuration,
            ISerializerManager serializerManager,
            IEnumerable<IContactPoint> parsedContactPoints,
            SchemaParser schemaParser)
        {
            _queryAbortTimeout = configuration.DefaultRequestOptions.QueryAbortTimeout;
            _internalMetadata = new InternalMetadata(
                cluster, this, configuration, serializerManager, parsedContactPoints, schemaParser);
        }

        internal Task TryInitializeAsync()
        {
            var currentState = Interlocked.Read(ref _state);
            if (currentState == Metadata.Initialized)
            {
                //It was already initialized
                return TaskHelper.Completed;
            }

            return InitializeAsync(currentState);
        }
        
        private Task InitializeAsync(long currentState)
        {
            if (currentState == Metadata.Disposed)
            {
                throw new ObjectDisposedException("This metadata object has been disposed.");
            }

            if (Interlocked.CompareExchange(ref _state, Metadata.Initializing, Metadata.NotInitialized)
                == Metadata.NotInitialized)
            {
                _initTask = Task.Run(InitializeInternalAsync);
            }

            return _initTask;
        }

        private async Task InitializeInternalAsync()
        {
            await _internalMetadata.InitAsync().ConfigureAwait(false);
            var previousState = Interlocked.CompareExchange(ref _state, Metadata.Initialized, Metadata.Initializing);
            if (previousState == Metadata.Disposed)
            {
                await _internalMetadata.ShutdownAsync().ConfigureAwait(false);
                throw new ObjectDisposedException("Metadata instance was disposed before initialization finished.");
            }
        }

        internal async Task ShutdownAsync()
        {
            var previousState = Interlocked.Exchange(ref _state, Metadata.Disposed);

            if (previousState != Metadata.Initialized)
            {
                return;
            }

            await _internalMetadata.ShutdownAsync().ConfigureAwait(false);
        }

        internal void OnHostRemoved(Host h)
        {
            HostRemoved?.Invoke(h);
        }

        internal void OnHostAdded(Host h)
        {
            HostAdded?.Invoke(h);
        }

        public async Task<ClusterDescription> GetClusterDescriptionAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return new ClusterDescription(
                _internalMetadata.ClusterName, _internalMetadata.IsDbaas, _internalMetadata.ProtocolVersion);
        }

        public ClusterDescription GetClusterDescription()
        {
            return TaskHelper.WaitToComplete(GetClusterDescriptionAsync());
        }

        public Host GetHost(IPEndPoint address)
        {
            return TaskHelper.WaitToComplete(GetHostAsync(address), _queryAbortTimeout);
        }

        public async Task<Host> GetHostAsync(IPEndPoint address)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return _internalMetadata.GetHost(address);
        }

        internal void FireSchemaChangedEvent(SchemaChangedEventArgs.Kind what, string keyspace, string table, object sender = null)
        {
            SchemaChangedEvent?.Invoke(sender ?? this, new SchemaChangedEventArgs { Keyspace = keyspace, What = what, Table = table });
        }

        internal void OnHostDown(Host h)
        {
            HostsEvent?.Invoke(this, new HostsEventArgs { Address = h.Address, What = HostsEventArgs.Kind.Down });
        }

        internal void OnHostUp(Host h)
        {
            HostsEvent?.Invoke(h, new HostsEventArgs { Address = h.Address, What = HostsEventArgs.Kind.Up });
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        public ICollection<Host> AllHosts()
        {
            return TaskHelper.WaitToComplete(AllHostsAsync(), _queryAbortTimeout);
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        public async Task<ICollection<Host>> AllHostsAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return _internalMetadata.AllHosts();
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            return TaskHelper.WaitToComplete(AllReplicasAsync(), _queryAbortTimeout);
        }

        public async Task<IEnumerable<IPEndPoint>> AllReplicasAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return _internalMetadata.AllReplicas();
        }

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        public ICollection<Host> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            return TaskHelper.WaitToComplete(GetReplicasAsync(keyspaceName, partitionKey));
        }

        /// <summary>
        /// Get the replicas for a given partition key
        /// </summary>
        public ICollection<Host> GetReplicas(byte[] partitionKey)
        {
            return GetReplicas(null, partitionKey);
        }

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        public async Task<ICollection<Host>> GetReplicasAsync(string keyspaceName, byte[] partitionKey)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return _internalMetadata.GetReplicas(keyspaceName, partitionKey);
        }

        /// <summary>
        /// Get the replicas for a given partition key
        /// </summary>
        public Task<ICollection<Host>> GetReplicasAsync(byte[] partitionKey)
        {
            return GetReplicasAsync(null, partitionKey);
        }

        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// <returns>the metadata of the requested keyspace or <c>null</c> if
        ///  <c>* keyspace</c> is not a known keyspace.</returns>
        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            return TaskHelper.WaitToComplete(GetKeyspaceAsync(keyspace), _queryAbortTimeout);
        }

        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// <returns>the metadata of the requested keyspace or <c>null</c> if
        ///  <c>* keyspace</c> is not a known keyspace.</returns>
        public async Task<KeyspaceMetadata> GetKeyspaceAsync(string keyspace)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetKeyspaceAsync(keyspace).ConfigureAwait(false);
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            return TaskHelper.WaitToComplete(GetKeyspacesAsync(), _queryAbortTimeout);
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public async Task<ICollection<string>> GetKeyspacesAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetKeyspacesAsync().ConfigureAwait(false);
        }

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        public ICollection<string> GetTables(string keyspace)
        {
            return TaskHelper.WaitToComplete(GetTablesAsync(keyspace), _queryAbortTimeout);
        }

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        public async Task<ICollection<string>> GetTablesAsync(string keyspace)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetTablesAsync(keyspace).ConfigureAwait(false);
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            return TaskHelper.WaitToComplete(GetTableAsync(keyspace, tableName), _queryAbortTimeout * 2);
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public async Task<TableMetadata> GetTableAsync(string keyspace, string tableName)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetTableAsync(keyspace, tableName).ConfigureAwait(false);
        }

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        public MaterializedViewMetadata GetMaterializedView(string keyspace, string name)
        {
            return TaskHelper.WaitToComplete(GetMaterializedViewAsync(keyspace, name), _queryAbortTimeout * 2);
        }

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        public async Task<MaterializedViewMetadata> GetMaterializedViewAsync(string keyspace, string name)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetMaterializedViewAsync(keyspace, name).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            return TaskHelper.WaitToComplete(GetUdtDefinitionAsync(keyspace, typeName), _queryAbortTimeout);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public async Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetUdtDefinitionAsync(keyspace, typeName).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public FunctionMetadata GetFunction(string keyspace, string name, string[] signature)
        {
            return TaskHelper.WaitToComplete(GetFunctionAsync(keyspace, name, signature), _queryAbortTimeout);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public async Task<FunctionMetadata> GetFunctionAsync(string keyspace, string name, string[] signature)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetFunctionAsync(keyspace, name, signature).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public AggregateMetadata GetAggregate(string keyspace, string name, string[] signature)
        {
            return TaskHelper.WaitToComplete(GetAggregateAsync(keyspace, name, signature), _queryAbortTimeout);
        }

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public async Task<AggregateMetadata> GetAggregateAsync(string keyspace, string name, string[] signature)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetAggregateAsync(keyspace, name, signature).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the query trace.
        /// </summary>
        /// <param name="trace">The query trace that contains the id, which properties are going to be populated.</param>
        /// <returns></returns>
        internal async Task<QueryTrace> GetQueryTraceAsync(QueryTrace trace)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.GetQueryTraceAsync(trace).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates keyspace metadata (including token metadata for token aware routing) for a given keyspace or a specific keyspace table.
        /// If no keyspace is provided then this method will update the metadata and token map for all the keyspaces of the cluster.
        /// </summary>
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            return TaskHelper.WaitToComplete(RefreshSchemaAsync(keyspace, table), _queryAbortTimeout * 2);
        }

        /// <summary>
        /// Updates keyspace metadata (including token metadata for token aware routing) for a given keyspace or a specific keyspace table.
        /// If no keyspace is provided then this method will update the metadata and token map for all the keyspaces of the cluster.
        /// </summary>
        public async Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.RefreshSchemaAsync(keyspace, table).ConfigureAwait(false);
        }

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
        public async Task<bool> CheckSchemaAgreementAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await _internalMetadata.CheckSchemaAgreementAsync().ConfigureAwait(false);
        }
    }
}