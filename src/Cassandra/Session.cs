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
using Cassandra.Connections.Control;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Insights;
using Cassandra.ExecutionProfiles;
using Cassandra.Metrics;
using Cassandra.Metrics.Internal;
using Cassandra.Observers.Abstractions;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <inheritdoc cref="ISession" />
    public class Session : IInternalSession
    {
        private const int Disposed = 10;
        private const int Initialized = 5;
        private const int Initializing = 1;

        private static readonly Logger Logger = new Logger(typeof(Session));
        private readonly IThreadSafeDictionary<IPEndPoint, IHostConnectionPool> _connectionPool;
        private readonly IInternalCluster _cluster;
        private readonly IMetricsManager _metricsManager;
        private readonly IObserverFactory _observerFactory;
        private readonly IInsightsClient _insightsClient;

        private readonly Task<IInternalMetadata> _initTask;
        private long _state = Session.Initializing;

        private volatile string _keyspace;

        internal IInternalSession InternalRef => this;

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
        public bool IsDisposed => Interlocked.Read(ref _state) == Session.Disposed;

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

        /// <inheritdoc />
        public void Connect()
        {
            InternalRef.TryInit();
        }

        /// <inheritdoc />
        public Task ConnectAsync()
        {
            return InternalRef.TryInitAndGetMetadataAsync();
        }

        public Policies Policies => Configuration.Policies;

        /// <inheritdoc />
        Guid IInternalSession.InternalSessionId { get; } = Guid.NewGuid();

        internal Session(
            IInternalCluster cluster,
            Configuration configuration,
            string keyspace,
            string sessionName)
        {
            _cluster = cluster;
            Configuration = configuration;
            Keyspace = keyspace;
            SessionName = sessionName;
            _connectionPool = new CopyOnWriteDictionary<IPEndPoint, IHostConnectionPool>();
            _metricsManager = new MetricsManager(configuration.MetricsProvider, Configuration.MetricsOptions, Configuration.MetricsEnabled, SessionName);
            _observerFactory = configuration.ObserverFactoryBuilder.Build(_metricsManager);
            _insightsClient = configuration.InsightsClientFactory.Create(cluster, this);
            UserDefinedTypes = new UdtMappingDefinitions(this);
            _initTask = Task.Run(InitInternalAsync);
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
            Execute(CqlQueryTools.GetCreateKeyspaceCql(keyspace, replication, durableWrites, false));
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
            var previousState = Interlocked.Exchange(ref _state, Session.Disposed);
            if (previousState != Session.Initialized)
            {
                return;
            }

            await ShutdownInternalAsync().ConfigureAwait(false);
        }

        private async Task ShutdownInternalAsync()
        {
            if (_insightsClient != null)
            {
                await _insightsClient.ShutdownAsync().ConfigureAwait(false);
            }

            _metricsManager?.Dispose();

            _cluster.Metadata.HostRemoved -= OnHostRemoved;

            var pools = _connectionPool.ToArray();
            foreach (var pool in pools)
            {
                pool.Value.Dispose();
            }
        }

        Task<IInternalMetadata> IInternalSession.TryInitAndGetMetadataAsync()
        {
            ValidateState();
            return _initTask;
        }

        void IInternalSession.TryInit()
        {
            if (ValidateState())
            {
                return;
            }

            TaskHelper.WaitToComplete(_initTask);
        }

        /// <summary>
        /// Validates if the session is not disposed and checks whether it is already initialized.
        /// Throws the initialization exception if the initialization failed.
        /// </summary>
        /// <returns>true if session is initialized, false otherwise</returns>
        private bool ValidateState()
        {
            var currentState = Interlocked.Read(ref _state);
            
            if (currentState == Session.Initialized)
            {
                //It was already initialized
                return true;
            }

            if (currentState == Session.Disposed)
            {
                throw new ObjectDisposedException("This session object has been disposed.");
            }
            
            if (_cluster.InitException != null)
            {
                //There was an exception that is not possible to recover from
                throw _cluster.InitException;
            }

            return false;
        }
        
        private async Task<IInternalMetadata> InitInternalAsync()
        {
            var internalMetadata = await InternalRef.InternalCluster.TryInitAndGetMetadataAsync().ConfigureAwait(false);

            InternalRef.InternalCluster.Metadata.HostRemoved += OnHostRemoved;

            _metricsManager.InitializeMetrics(this);

            var serializerManager = _cluster.Configuration.SerializerManager;
            if (Configuration.GetOrCreatePoolingOptions(serializerManager.CurrentProtocolVersion).GetWarmup())
            {
                await WarmupAsync(internalMetadata).ConfigureAwait(false);
            }

            if (Keyspace != null)
            {
                // Borrow a connection, trying to fail fast
                var handler = Configuration.RequestHandlerFactory.Create(this, internalMetadata, serializerManager.GetCurrentSerializer());
                await handler.GetNextConnectionAsync(new Dictionary<IPEndPoint, Exception>()).ConfigureAwait(false);
            }

            _insightsClient.Initialize(internalMetadata);

            var previousState = Interlocked.CompareExchange(ref _state, Session.Initialized, Session.Initializing);
            if (previousState == Session.Disposed)
            {
                await ShutdownInternalAsync().ConfigureAwait(false);
                throw new ObjectDisposedException("Session instance was disposed before initialization finished.");
            }

            return internalMetadata;
        }

        /// <summary>
        /// Creates the required connections on all hosts in the local DC.
        /// Returns a Task that is marked as completed after all pools were warmed up.
        /// In case, all the host pool warmup fail, it logs an error.
        /// </summary>
        private async Task WarmupAsync(IInternalMetadata metadata)
        {
            var hosts = metadata.AllHosts().Where(h => _cluster.RetrieveAndSetDistance(h) == HostDistance.Local).ToArray();
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
            return ExecuteAsync(statement, InternalRef.GetRequestOptions(executionProfileName));
        }

        private async Task<RowSet> ExecuteAsync(IStatement statement, IRequestOptions requestOptions)
        {
            var metadata = await InternalRef.TryInitAndGetMetadataAsync().ConfigureAwait(false);
            return await ExecuteAsync(metadata, statement, requestOptions).ConfigureAwait(false);
        }

        private Task<RowSet> ExecuteAsync(IInternalMetadata metadata, IStatement statement, IRequestOptions requestOptions)
        {
            return Configuration.RequestHandlerFactory
                                .Create(this, metadata, metadata.SerializerManager.GetCurrentSerializer(), statement, requestOptions)
                                .SendAsync();
        }

        /// <inheritdoc />
        IHostConnectionPool IInternalSession.GetOrCreateConnectionPool(Host host, HostDistance distance)
        {
            var hostPool = _connectionPool.GetOrAdd(host.Address, address =>
            {
                var newPool = Configuration.HostConnectionPoolFactory.Create(
                    host, Configuration, _cluster.Configuration.SerializerManager, _observerFactory);
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
            return await _cluster.PrepareAsync(this, cqlQuery, keyspace, customPayload).ConfigureAwait(false);
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
        public async Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement, string executionProfileName)
        {
            var metadata = await InternalRef.TryInitAndGetMetadataAsync().ConfigureAwait(false);
            var requestOptions = InternalRef.GetRequestOptions(executionProfileName);
            var stmt = graphStatement.ToIStatement(requestOptions.GraphOptions);
            await GetAnalyticsPrimary(metadata, stmt, graphStatement, requestOptions).ConfigureAwait(false);
            var rs = await ExecuteAsync(stmt, requestOptions).ConfigureAwait(false);
            return GraphResultSet.CreateNew(rs, graphStatement, requestOptions.GraphOptions);
        }

        private async Task<IStatement> GetAnalyticsPrimary(
            IInternalMetadata internalMetadata, IStatement statement, IGraphStatement graphStatement, IRequestOptions requestOptions)
        {
            if (!(statement is TargettedSimpleStatement) || !requestOptions.GraphOptions.IsAnalyticsQuery(graphStatement))
            {
                return statement;
            }

            var targetedSimpleStatement = (TargettedSimpleStatement)statement;

            RowSet rs;
            try
            {
                rs = await ExecuteAsync(
                    new SimpleStatement("CALL DseClientTool.getAnalyticsGraphServer()"), requestOptions).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Session.Logger.Verbose("Error querying graph analytics server, query will not be routed optimally: {0}", ex);
                return statement;
            }

            return AdaptRpcPrimaryResult(internalMetadata, rs, targetedSimpleStatement);
        }

        private IStatement AdaptRpcPrimaryResult(IInternalMetadata internalMetadata, RowSet rowSet, TargettedSimpleStatement statement)
        {
            var row = rowSet.FirstOrDefault();
            if (row == null)
            {
                Session.Logger.Verbose("Empty response querying graph analytics server, query will not be routed optimally");
                return statement;
            }
            var resultField = row.GetValue<IDictionary<string, string>>("result");
            if (resultField == null || !resultField.ContainsKey("location") || resultField["location"] == null)
            {
                Session.Logger.Verbose("Could not extract graph analytics server location from RPC, query will not be routed optimally");
                return statement;
            }
            var location = resultField["location"];
            var hostName = location.Substring(0, location.LastIndexOf(':'));
            var address = Configuration.AddressTranslator.Translate(
                new IPEndPoint(IPAddress.Parse(hostName), Configuration.ProtocolOptions.Port));
            var host = internalMetadata.GetHost(address);
            statement.PreferredHost = host;
            return statement;
        }
    }
}