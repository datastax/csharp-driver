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

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ISession"/>.
    /// </summary>
    /// <inheritdoc cref="Cassandra.ISession" />
    public class Session : ISession
    {
        private static Logger _logger = new Logger(typeof(Session));
        
        private readonly ConcurrentDictionary<IPAddress, HostConnectionPool> _connectionPool;
        private int _disposed;

        public int BinaryProtocolVersion { get; internal set; }

        public ICluster Cluster { get; private set; }

        /// <summary>
        /// Gets the cluster configuration
        /// </summary>
        public Configuration Configuration { get; protected set; }

        public bool IsDisposed
        {
            get { return Thread.VolatileRead(ref _disposed) > 0; }
        }

        public string Keyspace { get; internal set; }
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
            _connectionPool = new ConcurrentDictionary<IPAddress, HostConnectionPool>();
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
            if (this.Keyspace != keyspace)
            {
                this.Execute(new SimpleStatement(CqlQueryTools.GetUseKeyspaceCql(keyspace)));
                this.Keyspace = keyspace;
            }
        }

        /// <inheritdoc />
        public void CreateKeyspace(string keyspace, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            WaitForSchemaAgreement(Execute(CqlQueryTools.GetCreateKeyspaceCql(keyspace, replication, durableWrites, false)));
            _logger.Info("Keyspace [" + keyspace + "] has been successfully CREATED.");
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
                _logger.Info(string.Format("Cannot CREATE keyspace:  {0}  because it already exists.", keyspaceName));
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
                _logger.Info(string.Format("Cannot DELETE keyspace:  {0}  because it not exists.", keyspaceName));
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
            //Cancel all pending operations and dispose every connection
            var connections = GetAllConnections();
            _logger.Info("Disposing session, closing " + connections.Count + " connections.");
            foreach (var c in connections)
            {
                c.Dispose();
            }
        }

        /// <summary>
        /// Initialize the session
        /// </summary>
        /// <param name="createConnection">Determine if a connection must be created to test the host</param>
        internal void Init(bool createConnection)
        {
            Policies.LoadBalancingPolicy.Initialize(Cluster);

            if (!createConnection)
            {
                return;
            }
            var handler = new RequestHandler<RowSet>(this, null, null);
            //Borrow a connection
            handler.GetNextConnection(null);
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
            return new RequestHandler<RowSet>(this, GetRequest(statement), statement).Send();
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
                if (!host.IsUp)
                {
                    continue;
                }
                var distance = this.Policies.LoadBalancingPolicy.Distance(host);
                var hostPool = this.GetConnectionPool(host, distance);
                foreach (var c in hostPool.OpenConnections)
                {
                    connections.Add(c);
                }
            }
            return connections;
        }

        /// <summary>
        /// Gets the connection pool for a given host
        /// </summary>
        internal HostConnectionPool GetConnectionPool(Host host, HostDistance distance)
        {
            var hostPool = _connectionPool.GetOrAdd(host.Address, new HostConnectionPool(host, distance, Configuration));
            //It can change from the last time, when trying lower protocol versions
            hostPool.ProtocolVersion = (byte) BinaryProtocolVersion;
            return hostPool;
        }

        /// <summary>
        /// Gets the Request to send to a cassandra node based on the statement type
        /// </summary>
        internal IRequest GetRequest(IStatement statement)
        {
            var defaultConsistency = Configuration.QueryOptions.GetConsistencyLevel();
            if (statement is RegularStatement)
            {
                var s = (RegularStatement)statement;
                var options = QueryProtocolOptions.CreateFromQuery(s, defaultConsistency);
                options.ValueNames = s.QueryValueNames;
                return new QueryRequest(BinaryProtocolVersion, s.QueryString, s.IsTracing, options);
            }
            if (statement is BoundStatement)
            {
                var s = (BoundStatement)statement;
                var options = QueryProtocolOptions.CreateFromQuery(s, defaultConsistency);
                return new ExecuteRequest(BinaryProtocolVersion, s.PreparedStatement.Id, null, s.IsTracing, options);
            }
            if (statement is BatchStatement)
            {
                var s = (BatchStatement)statement;
                var consistency = defaultConsistency;
                if (s.ConsistencyLevel != null)
                {
                    consistency = s.ConsistencyLevel.Value;
                }
                return new BatchRequest(BinaryProtocolVersion, s, consistency);
            }
            throw new NotSupportedException("Statement of type " + statement.GetType().FullName + " not supported");
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            var task = PrepareAsync(cqlQuery);
            TaskHelper.WaitToComplete(task, Configuration.ClientOptions.QueryAbortTimeout);
            return task.Result;
        }

        public Task<PreparedStatement> PrepareAsync(string query)
        {
            var request = new PrepareRequest(this.BinaryProtocolVersion, query);
            return new RequestHandler<PreparedStatement>(this, request, null).Send();
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            WaitForSchemaAgreement(rs.Info.QueriedHost);
        }

        public bool WaitForSchemaAgreement(IPAddress hostAddress)
        {
            //TODO: Remove method
            if (Cluster.Metadata.AllHosts().Count == 1)
            {
                return true;
            }
            //This is trivial, but there isn't a reliable way to wait for all nodes to have the same schema.
            Thread.Sleep(1000);
            return false;
        }

        /// <summary>
        /// Waits for all pending responses to be received on all open connections or until a timeout is reached
        /// </summary>
        internal bool WaitForAllPendingActions(int timeout)
        {
            if (timeout == Timeout.Infinite)
            {
                timeout = Configuration.ClientOptions.QueryAbortTimeout;
            }
            var connections = GetAllConnections();
            if (connections.Count == 0)
            {
                return true;
            }
            _logger.Info("Waiting for pending operations of " + connections.Count + " connections to complete.");
            var handles = connections.Select(c => c.WaitPending()).ToArray();
            return WaitHandle.WaitAll(handles, timeout);
        }
    }
}
