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
using System.Linq;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;
 using Cassandra.Tasks;
using Cassandra.Requests;
 using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ISession"/>.
    /// </summary>
    /// <inheritdoc cref="Cassandra.ISession" />
    public class Session : ISession
    {
        private readonly Serializer _serializer;
        private static readonly Logger Logger = new Logger(typeof(Session));
        private readonly ConcurrentDictionary<IPEndPoint, HostConnectionPool> _connectionPool;
        private readonly Cluster _cluster;
        private int _disposed;
        private volatile string _keyspace;

        public int BinaryProtocolVersion { get { return (int)_serializer.ProtocolVersion; } }

        /// <inheritdoc />
        public ICluster Cluster { get { return _cluster; } }

        /// <summary>
        /// Gets the cluster configuration
        /// </summary>
        public Configuration Configuration { get; protected set; }

        /// <summary>
        /// Determines if the session is already disposed
        /// </summary>
        public bool IsDisposed
        {
            get { return Volatile.Read(ref _disposed) > 0; }
        }

        /// <summary>
        /// Gets or sets the keyspace
        /// </summary>
        public string Keyspace
        {
            get { return _keyspace; }
            internal set { _keyspace = value; }
        }

        /// <inheritdoc />
        public UdtMappingDefinitions UserDefinedTypes { get; private set; }

        public Policies Policies { get { return Configuration.Policies; } }

        internal Session(Cluster cluster, Configuration configuration, string keyspace, Serializer serializer)
        {
            _serializer = serializer;
            _cluster = cluster;
            Configuration = configuration;
            Keyspace = keyspace;
            UserDefinedTypes = new UdtMappingDefinitions(this, serializer);
            _connectionPool = new ConcurrentDictionary<IPEndPoint, HostConnectionPool>();
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
            var hosts = Cluster.AllHosts().ToArray();
            foreach (var host in hosts)
            {
                HostConnectionPool pool;
                if (_connectionPool.TryGetValue(host.Address, out pool))
                {
                    pool.Dispose();
                }
            }
        }

        /// <summary>
        /// Initialize the session
        /// </summary>
        internal Task Init()
        {
            var handler = new RequestHandler<RowSet>(this, _serializer);
            //Borrow a connection, trying to fail fast
            return handler.GetNextConnection(new Dictionary<IPEndPoint, Exception>());
        }

        /// <inheritdoc />
        public RowSet EndExecute(IAsyncResult ar)
        {
            var task = (Task<RowSet>)ar;
            TaskHelper.WaitToComplete(task, Configuration.ClientOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            var task = (Task<PreparedStatement>)ar;
            TaskHelper.WaitToComplete(task, Configuration.ClientOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement)
        {
            var task = ExecuteAsync(statement);
            TaskHelper.WaitToComplete(task, Configuration.ClientOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery)
        {
            return Execute(new SimpleStatement(cqlQuery).SetConsistencyLevel(Configuration.QueryOptions.GetConsistencyLevel()).SetPageSize(Configuration.QueryOptions.GetPageSize()));
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            return Execute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency).SetPageSize(Configuration.QueryOptions.GetPageSize()));
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, int pageSize)
        {
            return Execute(new SimpleStatement(cqlQuery).SetConsistencyLevel(Configuration.QueryOptions.GetConsistencyLevel()).SetPageSize(pageSize));
        }

        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            return new RequestHandler<RowSet>(this, _serializer, statement).Send();
        }

        /// <summary>
        /// Gets or creates the connection pool for a given host
        /// </summary>
        internal HostConnectionPool GetOrCreateConnectionPool(Host host, HostDistance distance)
        {
            var hostPool = _connectionPool.GetOrAdd(host.Address, address =>
            {
                var newPool = new HostConnectionPool(host, Configuration, _serializer);
                newPool.AllConnectionClosed += OnAllConnectionClosed;
                newPool.SetDistance(distance);
                return newPool;
            });
            return hostPool;
        }

        internal void OnAllConnectionClosed(Host host, HostConnectionPool pool)
        {
            if (_cluster.AnyOpenConnections(host))
            {
                pool.ScheduleReconnection();
                return;
            }
            // There isn't any open connection to this host in any of the pools
            MarkAsDownAndScheduleReconnection(host, pool);
        }

        internal void MarkAsDownAndScheduleReconnection(Host host, HostConnectionPool pool)
        {
            // By setting the host as down, all pools should cancel any outstanding reconnection attempt
            if (host.SetDown())
            {
                // Only attempt reconnection with 1 connection pool
                pool.ScheduleReconnection();
            }
        }

        internal bool HasConnections(Host host)
        {
            HostConnectionPool pool;
            if (_connectionPool.TryGetValue(host.Address, out pool))
            {
                return pool.HasConnections;
            }
            return false;
        }

        /// <summary>
        /// Gets the existing connection pool for this host and session or null when it does not exists
        /// </summary>
        internal HostConnectionPool GetExistingPool(Connection connection)
        {
            HostConnectionPool pool;
            _connectionPool.TryGetValue(connection.Address, out pool);
            return pool;
        }

        internal void CheckHealth(Connection connection)
        {
            HostConnectionPool pool;
            if (!_connectionPool.TryGetValue(connection.Address, out pool))
            {
                Logger.Error("Internal error: No host connection pool found");
                return;
            }
            pool.CheckHealth(connection);
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            return Prepare(cqlQuery, null);
        }

        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            var task = PrepareAsync(cqlQuery, customPayload);
            TaskHelper.WaitToComplete(task, Configuration.ClientOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query)
        {
            return PrepareAsync(query, null);
        }

        /// <inheritdoc />
        public async Task<PreparedStatement> PrepareAsync(string query, IDictionary<string, byte[]> customPayload)
        {
            var request = new PrepareRequest(query)
            {
                Payload = customPayload
            };
            var ps = await PrepareHandler.Send(this, _serializer, request).ConfigureAwait(false);
            await SetPrepareTableInfo(ps).ConfigureAwait(false);
            return ps;
        }

        private async Task SetPrepareTableInfo(PreparedStatement ps)
        {
            //TODO: Move
            const string msgRoutingNotSet = "Routing information could not be set for query \"{0}\"";
            var column = ps.Metadata.Columns.FirstOrDefault();
            if (column == null || column.Keyspace == null)
            {
                // The prepared statement does not contain parameters
                return;
            }
            if (ps.Metadata.PartitionKeys != null)
            {
                //The routing indexes where parsed in the prepared response
                if (ps.Metadata.PartitionKeys.Length == 0)
                {
                    // zero-length partition keys means that none of the parameters are partition keys
                    // the partition key is hard-coded.
                    return;
                }
                ps.RoutingIndexes = ps.Metadata.PartitionKeys;
                return;
            }
            try
            {
                var table = await Cluster.Metadata.GetTableAsync(column.Keyspace, column.Table).ConfigureAwait(false);
                if (table == null)
                {
                    Logger.Info(msgRoutingNotSet, ps.Cql);
                    return;
                }
                var routingSet = ps.SetPartitionKeys(table.PartitionKeys);
                if (!routingSet)
                {
                    Logger.Info(msgRoutingNotSet, ps.Cql);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("There was an error while trying to retrieve table metadata for {0}.{1}. {2}", 
                             column.Keyspace, column.Table, ex.InnerException);
            }
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            
        }

        public bool WaitForSchemaAgreement(IPEndPoint hostAddress)
        {
            return false;
        }
    }
}
