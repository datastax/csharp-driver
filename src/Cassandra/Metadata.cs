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
using Cassandra.Tasks;

namespace Cassandra
{
    /// <inheritdoc />
    internal class Metadata : IMetadata
    {
        private const int Disposed = 10;
        private const int Initialized = 5;
        private const int Initializing = 1;
        private const int NotInitialized = 0;

        private readonly int _queryAbortTimeout;
        private readonly CancellationToken _cancellationToken;

        private volatile Task _initTask;
        private volatile bool _initialized = false;

        private long _state = Metadata.NotInitialized;

        internal IInternalMetadata InternalMetadata { get; }

        internal Metadata(IInternalMetadata internalMetadata, CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            Configuration = internalMetadata.Configuration;
            _queryAbortTimeout = Configuration.DefaultRequestOptions.QueryAbortTimeout;
            InternalMetadata = internalMetadata;
        }

        public Configuration Configuration { get; }

        /// <inheritdoc />
        public event HostsEventHandler HostsEvent;

        /// <inheritdoc />
        public event SchemaChangedEventHandler SchemaChangedEvent;

        /// <inheritdoc />
        public event Action<Host> HostAdded;

        /// <inheritdoc />
        public event Action<Host> HostRemoved;

        /// <inheritdoc />
        public async Task<ClusterDescription> GetClusterDescriptionAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return GetClusterDescriptionInternal();
        }

        /// <inheritdoc />
        public ClusterDescription GetClusterDescription()
        {
            TryInitialize();
            return GetClusterDescriptionInternal();
        }

        /// <inheritdoc />
        public ICollection<Host> AllHostsSnapshot()
        {
            return InternalMetadata.AllHosts();
        }

        /// <inheritdoc />
        public IEnumerable<IPEndPoint> AllReplicasSnapshot()
        {
            return InternalMetadata.AllReplicas();
        }

        /// <inheritdoc />
        public ICollection<Host> GetReplicasSnapshot(string keyspaceName, byte[] partitionKey)
        {
            return InternalMetadata.GetReplicas(keyspaceName, partitionKey);
        }

        /// <inheritdoc />
        public ICollection<Host> GetReplicasSnapshot(byte[] partitionKey)
        {
            return GetReplicasSnapshot(null, partitionKey);
        }

        private ClusterDescription GetClusterDescriptionInternal()
        {
            return new ClusterDescription(InternalMetadata);
        }

        /// <inheritdoc />
        public Host GetHost(IPEndPoint address)
        {
            TryInitialize();
            return InternalMetadata.GetHost(address);
        }

        /// <inheritdoc />
        public async Task<Host> GetHostAsync(IPEndPoint address)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return InternalMetadata.GetHost(address);
        }

        /// <inheritdoc />
        public ICollection<Host> AllHosts()
        {
            TryInitialize();
            return InternalMetadata.AllHosts();
        }

        /// <inheritdoc />
        public async Task<ICollection<Host>> AllHostsAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return InternalMetadata.AllHosts();
        }

        /// <inheritdoc />
        public IEnumerable<IPEndPoint> AllReplicas()
        {
            TryInitialize();
            return InternalMetadata.AllReplicas();
        }

        /// <inheritdoc />
        public async Task<IEnumerable<IPEndPoint>> AllReplicasAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return InternalMetadata.AllReplicas();
        }

        /// <inheritdoc />
        public ICollection<Host> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            TryInitialize();
            return InternalMetadata.GetReplicas(keyspaceName, partitionKey);
        }

        /// <inheritdoc />
        public ICollection<Host> GetReplicas(byte[] partitionKey)
        {
            return GetReplicas(null, partitionKey);
        }

        /// <inheritdoc />
        public async Task<ICollection<Host>> GetReplicasAsync(string keyspaceName, byte[] partitionKey)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return InternalMetadata.GetReplicas(keyspaceName, partitionKey);
        }

        /// <inheritdoc />
        public Task<ICollection<Host>> GetReplicasAsync(byte[] partitionKey)
        {
            return GetReplicasAsync(null, partitionKey);
        }

        /// <inheritdoc />
        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.GetKeyspaceAsync(keyspace), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public async Task<KeyspaceMetadata> GetKeyspaceAsync(string keyspace)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.GetKeyspaceAsync(keyspace).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public ICollection<string> GetKeyspaces()
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.GetKeyspacesAsync(), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public async Task<ICollection<string>> GetKeyspacesAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.GetKeyspacesAsync().ConfigureAwait(false);
        }

        /// <inheritdoc />
        public ICollection<string> GetTables(string keyspace)
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.GetTablesAsync(keyspace), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public async Task<ICollection<string>> GetTablesAsync(string keyspace)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.GetTablesAsync(keyspace).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.GetTableAsync(keyspace, tableName), _queryAbortTimeout * 2);
        }

        /// <inheritdoc />
        public async Task<TableMetadata> GetTableAsync(string keyspace, string tableName)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.GetTableAsync(keyspace, tableName).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public MaterializedViewMetadata GetMaterializedView(string keyspace, string name)
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.GetMaterializedViewAsync(keyspace, name), _queryAbortTimeout * 2);
        }

        /// <inheritdoc />
        public async Task<MaterializedViewMetadata> GetMaterializedViewAsync(string keyspace, string name)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.GetMaterializedViewAsync(keyspace, name).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.GetUdtDefinitionAsync(keyspace, typeName), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public async Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.GetUdtDefinitionAsync(keyspace, typeName).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public FunctionMetadata GetFunction(string keyspace, string name, string[] signature)
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.GetFunctionAsync(keyspace, name, signature), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public async Task<FunctionMetadata> GetFunctionAsync(string keyspace, string name, string[] signature)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.GetFunctionAsync(keyspace, name, signature).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public AggregateMetadata GetAggregate(string keyspace, string name, string[] signature)
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.GetAggregateAsync(keyspace, name, signature), _queryAbortTimeout);
        }

        /// <inheritdoc />
        public async Task<AggregateMetadata> GetAggregateAsync(string keyspace, string name, string[] signature)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.GetAggregateAsync(keyspace, name, signature).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            TryInitialize();
            return TaskHelper.WaitToComplete(InternalMetadata.RefreshSchemaAsync(keyspace, table), _queryAbortTimeout * 2);
        }

        /// <inheritdoc />
        public async Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.RefreshSchemaAsync(keyspace, table).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<bool> CheckSchemaAgreementAsync()
        {
            await TryInitializeAsync().ConfigureAwait(false);
            return await InternalMetadata.CheckSchemaAgreementAsync().ConfigureAwait(false);
        }

        internal Task TryInitializeAsync()
        {
            if (_initialized)
            {
                //It was already initialized
                return TaskHelper.Completed;
            }

            TryLaunchInitTask();
            return _initTask;
        }

        internal void TryInitialize()
        {
            if (_initialized)
            {
                //It was already initialized
                return;
            }

            TryLaunchInitTask();
            _initTask.GetAwaiter().GetResult();
        }

        private void TryLaunchInitTask()
        {
            var currentState = Interlocked.Read(ref _state);
            if (currentState == Metadata.Disposed)
            {
                throw new ObjectDisposedException("This metadata object has been disposed.");
            }

            if (Interlocked.CompareExchange(ref _state, Metadata.Initializing, Metadata.NotInitialized)
                == Metadata.NotInitialized)
            {
                _initTask = Task.Run(() => InitializeInternalAsync(_cancellationToken));
            }
        }

        private async Task InitializeInternalAsync(CancellationToken token = default(CancellationToken))
        {
            await InternalMetadata.InitAsync(token).ConfigureAwait(false);
            SetupEventForwarding(InternalMetadata);

            var previousState = Interlocked.CompareExchange(ref _state, Metadata.Initialized, Metadata.Initializing);
            if (previousState == Metadata.Disposed)
            {
                await InternalMetadata.ShutdownAsync().ConfigureAwait(false);
                throw new ObjectDisposedException("Metadata instance was disposed before initialization finished.");
            }
            
            // Set metadata dependent options
            SetMetadataDependentOptions();

            _initialized = true;
        }

        internal async Task ShutdownAsync()
        {
            var previousState = Interlocked.Exchange(ref _state, Metadata.Disposed);
            _initialized = false;
            if (previousState != Metadata.Initialized)
            {
                return;
            }

            await InternalMetadata.ShutdownAsync().ConfigureAwait(false);
        }

        private void OnInternalHostRemoved(Host h)
        {
            HostRemoved?.Invoke(h);
        }

        private void OnInternalHostAdded(Host h)
        {
            HostAdded?.Invoke(h);
        }

        private void OnInternalHostsEvent(object sender, HostsEventArgs args)
        {
            HostsEvent?.Invoke(sender, args);
        }

        private void OnInternalSchemaChangedEvent(object sender, SchemaChangedEventArgs args)
        {
            SchemaChangedEvent?.Invoke(sender, args);
        }

        private void SetupEventForwarding(IInternalMetadata internalMetadata)
        {
            internalMetadata.Hosts.Added += internalMetadata.OnHostAdded;
            internalMetadata.Hosts.Removed += internalMetadata.OnHostRemoved;
            internalMetadata.HostAdded += OnInternalHostAdded;
            internalMetadata.HostRemoved += OnInternalHostRemoved;
            internalMetadata.HostsEvent += OnInternalHostsEvent;
            internalMetadata.SchemaChangedEvent += OnInternalSchemaChangedEvent;
        }

        private void SetMetadataDependentOptions()
        {
            if (InternalMetadata.IsDbaas)
            {
                Configuration.SetDefaultConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }
        }
    }
}