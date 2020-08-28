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
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Collections;
using Cassandra.Connections;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Insights;
using Cassandra.ExecutionProfiles;
using Cassandra.Metrics;
using Cassandra.Metrics.Internal;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <inheritdoc cref="ISession" />
    public class Session : IInternalSession
    {
        private readonly ISerializerManager _serializerManager;
        private static readonly Logger Logger = new Logger(typeof(Session));
        private readonly IThreadSafeDictionary<IPEndPoint, IHostConnectionPool> _connectionPool;
        private readonly IInternalCluster _cluster;
        private int _disposed;
        private volatile string _keyspace;
        private readonly IMetricsManager _metricsManager;
        private readonly IObserverFactory _observerFactory;
        private readonly IInsightsClient _insightsClient;
        private readonly IGraphTypeSerializerFactory _graphTypeSerializerFactory = new GraphTypeSerializerFactory();

        internal IInternalSession InternalRef => this;

        public int BinaryProtocolVersion => (int)_serializerManager.GetCurrentSerializer().ProtocolVersion;

        /// <inheritdoc />
        public ICluster Cluster => _cluster;

        IInternalCluster IInternalSession.InternalCluster => _cluster;

        IMetricsManager IInternalSession.MetricsManager => _metricsManager;

        IObserverFactory IInternalSession.ObserverFactory => _observerFactory;

        /// <summary>
        /// Gets the cluster configuration
        /// </summary>
        public Configuration Configuration { get; protected set; }

        /// <summary>
        /// Determines if the session is already disposed
        /// </summary>
        public bool IsDisposed => Volatile.Read(ref _disposed) > 0;

        /// <summary>
        /// Gets or sets the keyspace
        /// </summary>
        public string Keyspace
        {
            get => InternalRef.Keyspace;
            private set => InternalRef.Keyspace = value;
        }

        /// <summary>
        /// Gets or sets the keyspace
        /// </summary>
        string IInternalSession.Keyspace
        {
            get => _keyspace;
            set => _keyspace = value;
        }

        /// <inheritdoc />
        public UdtMappingDefinitions UserDefinedTypes { get; private set; }

        public string SessionName { get; }

        public Policies Policies => Configuration.Policies;

        /// <inheritdoc />
        Guid IInternalSession.InternalSessionId { get; } = Guid.NewGuid();

        internal Session(
            IInternalCluster cluster,
            Configuration configuration,
            string keyspace,
            ISerializerManager serializerManager,
            string sessionName)
        {
            _serializerManager = serializerManager;
            _cluster = cluster;
            Configuration = configuration;
            Keyspace = keyspace;
            SessionName = sessionName;
            UserDefinedTypes = new UdtMappingDefinitions(this, serializerManager);
            _connectionPool = new CopyOnWriteDictionary<IPEndPoint, IHostConnectionPool>();
            _cluster.HostRemoved += OnHostRemoved;
            _metricsManager = new MetricsManager(configuration.MetricsProvider, Configuration.MetricsOptions, Configuration.MetricsEnabled, SessionName);
            _observerFactory = configuration.ObserverFactoryBuilder.Build(_metricsManager);
            _insightsClient = configuration.InsightsClientFactory.Create(cluster, this);
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            return ExecuteAsync(statement).ToApm(callback, state);
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), callback, state);
        }

        /// <inheritdoc />
        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            return PrepareAsync(cqlQuery).ToApm(callback, state);
        }

        /// <inheritdoc />
        public void ChangeKeyspace(string keyspace)
        {
            if (Keyspace != keyspace)
            {
                Execute(new SimpleStatement(CqlQueryTools.GetUseKeyspaceCql(keyspace)));
                Keyspace = keyspace;
            }
        }

        /// <inheritdoc />
        public void CreateKeyspace(string keyspace, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            WaitForSchemaAgreement(Execute(CqlQueryTools.GetCreateKeyspaceCql(keyspace, replication, durableWrites, false)));
            Session.Logger.Info("Keyspace [" + keyspace + "] has been successfully CREATED.");
        }

        /// <inheritdoc />
        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            try
            {
                CreateKeyspace(keyspaceName, replication, durableWrites);
            }
            catch (AlreadyExistsException)
            {
                Session.Logger.Info(string.Format("Cannot CREATE keyspace:  {0}  because it already exists.", keyspaceName));
            }
        }

        /// <inheritdoc />
        public void DeleteKeyspace(string keyspaceName)
        {
            Execute(CqlQueryTools.GetDropKeyspaceCql(keyspaceName, false));
        }

        /// <inheritdoc />
        public void DeleteKeyspaceIfExists(string keyspaceName)
        {
            try
            {
                DeleteKeyspace(keyspaceName);
            }
            catch (InvalidQueryException)
            {
                Session.Logger.Info(string.Format("Cannot DELETE keyspace:  {0}  because it not exists.", keyspaceName));
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            ShutdownAsync().GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task ShutdownAsync()
        {
            //Only dispose once
            if (Interlocked.Increment(ref _disposed) != 1)
            {
                return;
            }

            if (_insightsClient != null)
            {
                await _insightsClient.ShutdownAsync().ConfigureAwait(false);
            }

            _metricsManager?.Dispose();

            _cluster.HostRemoved -= OnHostRemoved;

            var pools = _connectionPool.ToArray();
            foreach (var pool in pools)
            {
                pool.Value.Dispose();
            }
        }

        /// <inheritdoc />
        async Task IInternalSession.Init()
        {
            _metricsManager.InitializeMetrics(this);

            if (Configuration.GetOrCreatePoolingOptions(_serializerManager.CurrentProtocolVersion).GetWarmup())
            {
                await Warmup().ConfigureAwait(false);
            }

            if (Keyspace != null)
            {
                // Borrow a connection, trying to fail fast
                var handler = Configuration.RequestHandlerFactory.Create(this, _serializerManager.GetCurrentSerializer());
                await handler.GetNextConnectionAsync(new Dictionary<IPEndPoint, Exception>()).ConfigureAwait(false);
            }

            _insightsClient.Init();
        }

        /// <summary>
        /// Creates the required connections on all hosts in the local DC.
        /// Returns a Task that is marked as completed after all pools were warmed up.
        /// In case, all the host pool warmup fail, it logs an error.
        /// </summary>
        private async Task Warmup()
        {
            var hosts = _cluster.AllHosts().Where(h => _cluster.RetrieveAndSetDistance(h) == HostDistance.Local).ToArray();
            var tasks = new Task[hosts.Length];
            for (var i = 0; i < hosts.Length; i++)
            {
                var host = hosts[i];
                var pool = InternalRef.GetOrCreateConnectionPool(host, HostDistance.Local);
                tasks[i] = pool.Warmup();
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch
            {
                if (tasks.Any(t => t.Status == TaskStatus.RanToCompletion))
                {
                    // At least 1 of the warmup tasks completed
                    return;
                }

                // Log and continue as the ControlConnection is connected
                Session.Logger.Error($"Connection pools for {hosts.Length} host(s) failed to be warmed up");
            }
        }

        /// <inheritdoc />
        public RowSet EndExecute(IAsyncResult ar)
        {
            var task = (Task<RowSet>)ar;
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            var task = (Task<PreparedStatement>)ar;
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement, string executionProfileName)
        {
            var task = ExecuteAsync(statement, executionProfileName);
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement)
        {
            return Execute(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery)
        {
            return Execute(GetDefaultStatement(cqlQuery));
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, string executionProfileName)
        {
            return Execute(GetDefaultStatement(cqlQuery), executionProfileName);
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            return Execute(GetDefaultStatement(cqlQuery).SetConsistencyLevel(consistency));
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, int pageSize)
        {
            return Execute(GetDefaultStatement(cqlQuery).SetPageSize(pageSize));
        }

        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            return ExecuteAsync(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement statement, string executionProfileName)
        {
            return InternalRef.ExecuteAsync(statement, InternalRef.GetRequestOptions(executionProfileName));
        }

        Task<RowSet> IInternalSession.ExecuteAsync(IStatement statement, IRequestOptions requestOptions)
        {
            return Configuration.RequestHandlerFactory
                                .Create(this, _serializerManager.GetCurrentSerializer(), statement, requestOptions)
                                .SendAsync();
        }

        /// <inheritdoc />
        IHostConnectionPool IInternalSession.GetOrCreateConnectionPool(Host host, HostDistance distance)
        {
            var hostPool = _connectionPool.GetOrAdd(host.Address, address =>
            {
                var newPool = Configuration.HostConnectionPoolFactory.Create(
                    host, Configuration, _serializerManager, _observerFactory);
                newPool.AllConnectionClosed += InternalRef.OnAllConnectionClosed;
                newPool.SetDistance(distance);
                _metricsManager.GetOrCreateNodeMetrics(host).InitializePoolGauges(newPool);
                return newPool;
            });
            return hostPool;
        }

        /// <inheritdoc />
        IEnumerable<KeyValuePair<IPEndPoint, IHostConnectionPool>> IInternalSession.GetPools()
        {
            return _connectionPool.Select(kvp => new KeyValuePair<IPEndPoint, IHostConnectionPool>(kvp.Key, kvp.Value));
        }

        void IInternalSession.OnAllConnectionClosed(Host host, IHostConnectionPool pool)
        {
            if (_cluster.AnyOpenConnections(host))
            {
                pool.ScheduleReconnection();
                return;
            }

            // There isn't any open connection to this host in any of the pools
            pool.MarkAsDownAndScheduleReconnection();
        }

        /// <inheritdoc/>
        int IInternalSession.ConnectedNodes => _connectionPool.Count(kvp => kvp.Value.HasConnections);

        public IDriverMetrics GetMetrics()
        {
            return _metricsManager;
        }

        bool IInternalSession.HasConnections(Host host)
        {
            if (_connectionPool.TryGetValue(host.Address, out var pool))
            {
                return pool.HasConnections;
            }
            return false;
        }

        /// <inheritdoc />
        IHostConnectionPool IInternalSession.GetExistingPool(IPEndPoint address)
        {
            _connectionPool.TryGetValue(address, out var pool);
            return pool;
        }

        void IInternalSession.CheckHealth(Host host, IConnection connection)
        {
            if (!_connectionPool.TryGetValue(host.Address, out var pool))
            {
                Session.Logger.Error("Internal error: No host connection pool found");
                return;
            }
            pool.CheckHealth(connection);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery)
        {
            return Prepare(cqlQuery, null, null);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return Prepare(cqlQuery, null, customPayload);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, string keyspace)
        {
            return Prepare(cqlQuery, keyspace, null);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            var task = PrepareAsync(cqlQuery, keyspace, customPayload);
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, Configuration.ClientOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query)
        {
            return PrepareAsync(query, null, null);
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query, IDictionary<string, byte[]> customPayload)
        {
            return PrepareAsync(query, null, customPayload);
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace)
        {
            return PrepareAsync(cqlQuery, keyspace, null);
        }

        /// <inheritdoc />
        public async Task<PreparedStatement> PrepareAsync(
            string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            var serializer = _serializerManager.GetCurrentSerializer();
            var currentVersion = serializer.ProtocolVersion;
            if (!currentVersion.SupportsKeyspaceInRequest() && keyspace != null)
            {
                // Validate protocol version here and not at PrepareRequest level, as PrepareRequest can be issued
                // in the background (prepare and retry, prepare on up, ...)
                throw new NotSupportedException($"Protocol version {currentVersion} does not support" +
                                                " setting the keyspace as part of the PREPARE request");
            }
            var request = new PrepareRequest(serializer, cqlQuery, keyspace, customPayload);
            return await _cluster.Prepare(this, _serializerManager, request).ConfigureAwait(false);
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
        }

        public bool WaitForSchemaAgreement(IPEndPoint hostAddress)
        {
            return false;
        }

        private IStatement GetDefaultStatement(string cqlQuery)
        {
            return new SimpleStatement(cqlQuery);
        }

        /// <inheritdoc />
        IRequestOptions IInternalSession.GetRequestOptions(string executionProfileName)
        {
            if (!Configuration.RequestOptions.TryGetValue(executionProfileName, out var profile))
            {
                throw new ArgumentException("The provided execution profile name does not exist. It must be added through the Cluster Builder.");
            }

            return profile;
        }

        private void OnHostRemoved(Host host)
        {
            _metricsManager.RemoveNodeMetrics(host);
            if (_connectionPool.TryRemove(host.Address, out var pool))
            {
                pool.OnHostRemoved();
                pool.Dispose();
            }
        }

        /// <inheritdoc />
        public GraphResultSet ExecuteGraph(IGraphStatement statement)
        {
            return ExecuteGraph(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement)
        {
            return ExecuteGraphAsync(graphStatement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public GraphResultSet ExecuteGraph(IGraphStatement statement, string executionProfileName)
        {
            return TaskHelper.WaitToCompleteWithMetrics(_metricsManager, ExecuteGraphAsync(statement, executionProfileName));
        }

        /// <inheritdoc />
        public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement, string executionProfileName)
        {
            return Configuration.RequestHandlerFactory
                                .CreateGraphRequestHandler(this, _graphTypeSerializerFactory)
                                .SendAsync(graphStatement, InternalRef.GetRequestOptions(executionProfileName));
        }
    }
}