//
//      Copyright (C) 2012-2016 DataStax Inc.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <inheritdoc cref="Cassandra.ISession" />
    public class Session : IInternalSession
    {
        private readonly Serializer _serializer;
        private ISessionManager _sessionManager;
        private static readonly Logger Logger = new Logger(typeof(Session));
        private readonly ConcurrentDictionary<IPEndPoint, IHostConnectionPool> _connectionPool;
        private readonly IInternalCluster _cluster;
        private int _disposed;
        private volatile string _keyspace;

        internal IInternalSession InternalRef => this;

        public int BinaryProtocolVersion => (int)_serializer.ProtocolVersion;

        /// <inheritdoc />
        public ICluster Cluster => _cluster;

        IInternalCluster IInternalSession.InternalCluster => _cluster;

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

        public Policies Policies => Configuration.Policies;

        internal Session(
            IInternalCluster cluster,
            Configuration configuration,
            string keyspace,
            Serializer serializer)
        {
            _serializer = serializer;
            _cluster = cluster;
            Configuration = configuration;
            Keyspace = keyspace;
            UserDefinedTypes = new UdtMappingDefinitions(this, serializer);
            _connectionPool = new ConcurrentDictionary<IPEndPoint, IHostConnectionPool>();
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
            Logger.Info("Keyspace [" + keyspace + "] has been successfully CREATED.");
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
                Logger.Info(string.Format("Cannot CREATE keyspace:  {0}  because it already exists.", keyspaceName));
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
                Logger.Info(string.Format("Cannot DELETE keyspace:  {0}  because it not exists.", keyspaceName));
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            //Only dispose once
            if (Interlocked.Increment(ref _disposed) != 1)
            {
                return;
            }

            _sessionManager?.OnShutdownAsync().GetAwaiter().GetResult();

            var hosts = Cluster.AllHosts().ToArray();
            foreach (var host in hosts)
            {
                if (_connectionPool.TryGetValue(host.Address, out var pool))
                {
                    pool.Dispose();
                }
            }
        }
        
        /// <inheritdoc />
        Task IInternalSession.Init()
        {
            return InternalRef.Init(null);
        }

        /// <inheritdoc />
        async Task IInternalSession.Init(ISessionManager sessionManager)
        {
            _sessionManager = sessionManager;

            if (Configuration.GetPoolingOptions(_serializer.ProtocolVersion).GetWarmup())
            {
                await Warmup().ConfigureAwait(false);
            }

            if (Keyspace != null)
            {
                // Borrow a connection, trying to fail fast
                var handler = Configuration.RequestHandlerFactory.Create(this, _serializer);
                await handler.GetNextConnectionAsync(new Dictionary<IPEndPoint, Exception>()).ConfigureAwait(false);
            }

            if (_sessionManager != null)
            {
                await _sessionManager.OnInitializationAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Creates the required connections on all hosts in the local DC.
        /// Returns a Task that is marked as completed after all pools were warmed up.
        /// In case, all the host pool warmup fail, it logs an error.
        /// </summary>
        private async Task Warmup()
        {
            // Load balancing policy was initialized
            var lbp = Configuration.DefaultRequestOptions.LoadBalancingPolicy;
            var hosts = lbp.NewQueryPlan(Keyspace, null).Where(h => lbp.Distance(h) == HostDistance.Local).ToArray();
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
                Logger.Error($"Connection pools for {hosts.Length} host(s) failed to be warmed up");
            }
        }

        /// <inheritdoc />
        public RowSet EndExecute(IAsyncResult ar)
        {
            var task = (Task<RowSet>)ar;
            TaskHelper.WaitToComplete(task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            var task = (Task<PreparedStatement>)ar;
            TaskHelper.WaitToComplete(task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement, string executionProfileName)
        {
            var task = ExecuteAsync(statement, executionProfileName);
            TaskHelper.WaitToComplete(task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement)
        {
            var task = ExecuteAsync(statement);
            TaskHelper.WaitToComplete(task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
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
            return Configuration.RequestHandlerFactory
                                .Create(this, _serializer, statement, GetRequestOptions(executionProfileName))
                                .SendAsync();
        }
        
        /// <inheritdoc />
        IHostConnectionPool IInternalSession.GetOrCreateConnectionPool(Host host, HostDistance distance)
        {
            var hostPool = _connectionPool.GetOrAdd(host.Address, address =>
            {
                var newPool = Configuration.HostConnectionPoolFactory.Create(host, Configuration, _serializer);
                newPool.AllConnectionClosed += InternalRef.OnAllConnectionClosed;
                newPool.SetDistance(distance);
                return newPool;
            });
            return hostPool;
        }

        /// <inheritdoc />
        IEnumerable<KeyValuePair<IPEndPoint, IHostConnectionPool>> IInternalSession.GetPools()
        {
            return _connectionPool.ToArray().Select(kvp => new KeyValuePair<IPEndPoint, IHostConnectionPool>(kvp.Key, kvp.Value));
        }

        void IInternalSession.OnAllConnectionClosed(Host host, IHostConnectionPool pool)
        {
            if (_cluster.AnyOpenConnections(host))
            {
                pool.ScheduleReconnection();
                return;
            }
            // There isn't any open connection to this host in any of the pools
            InternalRef.MarkAsDownAndScheduleReconnection(host, pool);
        }

        void IInternalSession.MarkAsDownAndScheduleReconnection(Host host, IHostConnectionPool pool)
        {
            // By setting the host as down, all pools should cancel any outstanding reconnection attempt
            if (host.SetDown())
            {
                // Only attempt reconnection with 1 connection pool
                pool.ScheduleReconnection();
            }
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

        void IInternalSession.CheckHealth(IConnection connection)
        {
            if (!_connectionPool.TryGetValue(connection.Address, out var pool))
            {
                Logger.Error("Internal error: No host connection pool found");
                return;
            }
            pool.CheckHealth(connection);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery)
        {
            return Prepare(PrepareRequestBuilder.FromQuery(cqlQuery).Build());
        }
        
        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return Prepare(PrepareRequestBuilder.FromQuery(cqlQuery).WithCustomPayload(customPayload).Build());
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(IPrepareRequest prepareRequest)
        {
            var task = PrepareAsync(prepareRequest);
            TaskHelper.WaitToComplete(task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query)
        {
            return PrepareAsync(PrepareRequestBuilder.FromQuery(query).Build());
        }
        
        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query, IDictionary<string, byte[]> customPayload)
        {
            return PrepareAsync(PrepareRequestBuilder.FromQuery(query).WithCustomPayload(customPayload).Build());
        }

        /// <inheritdoc />
        public async Task<PreparedStatement> PrepareAsync(IPrepareRequest prepareRequest)
        {
            var request = new InternalPrepareRequest(prepareRequest.Query)
            {
                Payload = prepareRequest.CustomPayload
            };

            return await _cluster.Prepare(this, _serializer, request, GetRequestOptions(prepareRequest.ExecutionProfileName)).ConfigureAwait(false);
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

        private IRequestOptions GetRequestOptions(string executionProfileName)
        {
            if (!Configuration.RequestOptions.TryGetValue(executionProfileName, out var profile))
            {
                throw new ArgumentException("The provided execution profile name does not exist. It must be added through the Cluster Builder.");
            }

            return profile;
        }
    }
}