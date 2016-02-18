//
//      Copyright (C) 2012-2014 DataStax Inc.
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
﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;
 using Cassandra.Tasks;
using Cassandra.Requests;

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ISession"/>.
    /// </summary>
    /// <inheritdoc cref="Cassandra.ISession" />
    public class Session : ISession
    {
        private static readonly Logger Logger = new Logger(typeof(Session));
        private readonly ConcurrentDictionary<IPEndPoint, HostConnectionPool> _connectionPool;
        private int _disposed;
        private volatile string _keyspace;

        public int BinaryProtocolVersion { get; internal set; }

        /// <inheritdoc />
        public ICluster Cluster { get; private set; }

        /// <summary>
        /// Gets the cluster configuration
        /// </summary>
        public Configuration Configuration { get; protected set; }

        /// <summary>
        /// Determines if the session is already disposed
        /// </summary>
        public bool IsDisposed
        {
            get { return Thread.VolatileRead(ref _disposed) > 0; }
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

        internal Session(ICluster cluster, Configuration configuration, string keyspace, int binaryProtocolVersion)
        {
            Cluster = cluster;
            Configuration = configuration;
            Keyspace = keyspace;
            BinaryProtocolVersion = binaryProtocolVersion;
            UserDefinedTypes = new UdtMappingDefinitions(this);
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
        public Task ChangeKeyspaceAsync(string keyspace)
        {
            if (Keyspace != keyspace)
            {
                var task = ExecuteAsync(new SimpleStatement(CqlQueryTools.GetUseKeyspaceCql(keyspace)));
                return task.Continue(rs => Keyspace = keyspace);
            }
            return TaskHelper.Completed;
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
        public Task CreateKeyspaceAsync(string keyspace, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            return ExecuteAsync(CqlQueryTools.GetCreateKeyspaceCql(keyspace, replication, durableWrites, false)).Continue(t => Logger.Info("Keyspace [" + keyspace + "] has been successfully CREATED."));
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
        internal void Init()
        {
            var handler = new RequestHandler<RowSet>(this);
            //Borrow a connection, trying to fail fast
            TaskHelper.WaitToComplete(handler.GetNextConnection(new Dictionary<IPEndPoint,Exception>()));
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
        public Task<RowSet> ExecuteAsync(string cqlQuery)
        {
            return ExecuteAsync(new SimpleStatement(cqlQuery).SetConsistencyLevel(Configuration.QueryOptions.GetConsistencyLevel()).SetPageSize(Configuration.QueryOptions.GetPageSize()));
        }

        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            return new RequestHandler<RowSet>(this, statement).Send();
        }

        /// <summary>
        /// Gets a list of all opened connections to all hosts
        /// </summary>
        private List<Connection> GetAllConnections()
        {
            var hosts = Cluster.AllHosts();
            var connections = new List<Connection>();
            foreach (var host in hosts)
            {
                HostConnectionPool pool;
                if (_connectionPool.TryGetValue(host.Address, out pool))
                {
                    connections.AddRange(pool.OpenConnections);
                }
            }
            return connections;
        }

        /// <summary>
        /// Gets or creates the connection pool for a given host
        /// </summary>
        internal HostConnectionPool GetOrCreateConnectionPool(Host host, HostDistance distance)
        {
            var hostPool = _connectionPool.GetOrAdd(host.Address, address => new HostConnectionPool(host, distance, Configuration));
            //It can change from the last time, when trying lower protocol versions
            hostPool.ProtocolVersion = (byte) BinaryProtocolVersion;
            return hostPool;
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

        public Task<PreparedStatement> PrepareAsync(string query, IDictionary<string, byte[]> customPayload)
        {
            var request = new PrepareRequest(BinaryProtocolVersion, query)
            {
                Payload = customPayload
            };
            return new RequestHandler<PreparedStatement>(this, request)
                .Send()
                .Continue(SetPrepareTableInfo);
        }

        /// <inheritdoc />
        private PreparedStatement SetPrepareTableInfo(Task<PreparedStatement> t)
        {
            const string msgRoutingNotSet = "Routing information could not be set for query \"{0}\"";
            var ps = t.Result;
            var column = ps.Metadata.Columns.FirstOrDefault();
            if (column == null || column.Keyspace == null)
            {
                //The prepared statement does not contain parameters
                return ps;
            }
            if (ps.Metadata.PartitionKeys != null)
            {
                //The routing indexes where parsed in the prepared response
                if (ps.Metadata.PartitionKeys.Length == 0)
                {
                    //zero-length partition keys means that none of the parameters are partition keys
                    //the partition key is hard-coded.
                    return ps;
                }
                ps.RoutingIndexes = ps.Metadata.PartitionKeys;
                return ps;
            }
            TableMetadata table = null;
            try
            {
                table = Cluster.Metadata.GetTable(column.Keyspace, column.Table);
            }
            catch (Exception ex)
            {
                Logger.Error("There was an error while trying to retrieve table metadata for {0}.{1}. {2}", column.Keyspace, column.Table, ex.ToString());
            }
            if (table == null)
            {
                Logger.Info(msgRoutingNotSet, ps.Cql);
                return ps;
            }
            var routingSet = ps.SetPartitionKeys(table.PartitionKeys);
            if (!routingSet)
            {
                Logger.Info(msgRoutingNotSet, ps.Cql);
            }
            return ps;
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            
        }

        public bool WaitForSchemaAgreement(IPEndPoint hostAddress)
        {
            return false;
        }

        /// <summary>
        /// Waits for all pending responses to be received on all open connections or until a timeout is reached
        /// </summary>
        internal bool WaitForAllPendingActions(int timeout)
        {
            if (timeout == Timeout.Infinite)
            {
                //It is generally invoked with timeout infinite
                //Do not honor that setting as it is best to cancel pending requests than waiting forever
                timeout = Configuration.ClientOptions.QueryAbortTimeout;
            }
            var connections = GetAllConnections();
            if (connections.Count == 0)
            {
                return true;
            }
            Logger.Info("Waiting for pending operations of " + connections.Count + " connections to complete.");
            var handles = connections.Select(c => c.WaitPending()).ToArray();
            //WaitHandle.WaitAll() not supported on STAThreads (thanks COM!)
            //Start new task and wait on the individual Task
            return Task.Factory.StartNew(() => WaitHandle.WaitAll(handles, timeout)).Wait(timeout);
        }
    }
}
