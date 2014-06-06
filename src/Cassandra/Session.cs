//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Diagnostics;
﻿using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Net.Sockets;

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ISession"/>.
    /// </summary>
    /// <inheritdoc cref="Cassandra.ISession" />
    public class Session : ISession
    {
        private static Logger _logger = new Logger(typeof(Session));
        internal static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);
        
        readonly ConcurrentDictionary<IPAddress, HostConnectionPool> _connectionPool = new ConcurrentDictionary<IPAddress, HostConnectionPool>();
        private int _disposed;

        public int BinaryProtocolVersion { get; protected set; }

        public Cluster Cluster { get; private set; }

        /// <summary>
        /// Gets the cluster configuration
        /// </summary>
        public Configuration Configuration { get; protected set; }

        public bool IsDisposed
        {
            get { return Thread.VolatileRead(ref _disposed) > 0; }
        }

        /// <summary>
        /// Gets or sets the identifier of this instance
        /// </summary>
        internal Guid Guid { get; private set; }

        public string Keyspace { get; protected set; }

        public Policies Policies { get { return Configuration.Policies; } }

        internal Session(Cluster cluster, Configuration configuration, string keyspace, int binaryProtocolVersion)
        {
            this.Cluster = cluster;
            this.Configuration = configuration;
            this.Keyspace = keyspace;
            this.BinaryProtocolVersion = binaryProtocolVersion;
            this.Guid = Guid.NewGuid();

        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            return ExecuteAsync(statement).ToApm(callback, state);
        }

        public IAsyncResult BeginExecute(IStatement statement, object tag, AsyncCallback callback, object state)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), callback, state);
        }

        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, object tag, AsyncCallback callback, object state)
        {
            //TODO: Remove method and document
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void ChangeKeyspace(string keyspace)
        {
            if (this.Keyspace != keyspace)
            {
                this.Execute(new SimpleStatement("USE " + keyspace));
                this.Keyspace = keyspace;
            }
        }

        /// <inheritdoc />
        public void CreateKeyspace(string keyspace, Dictionary<string, string> replication = null, bool durable_writes = true)
        {
            WaitForSchemaAgreement(Execute(CqlQueryTools.GetCreateKeyspaceCql(keyspace, replication, durable_writes, false)));
            _logger.Info("Keyspace [" + keyspace + "] has been successfully CREATED.");
        }

        /// <inheritdoc />
        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durable_writes = true)
        {
            try
            {
                CreateKeyspace(keyspaceName, replication, durable_writes);
            }
            catch (AlreadyExistsException)
            {
                _logger.Info(string.Format("Cannot CREATE keyspace:  {0}  because it already exists.", keyspaceName));
            }
        }

        /// <inheritdoc />
        public void DeleteKeyspace(string keyspaceName)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void DeleteKeyspaceIfExists(string keyspaceName)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
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
        /// Gets the connection pool for a given host
        /// </summary>
        internal HostConnectionPool GetConnectionPool(Host host, HostDistance distance)
        {
            return _connectionPool.GetOrAdd(host.Address, new HostConnectionPool(host, distance, (byte)BinaryProtocolVersion, Configuration));
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
                return new QueryRequest(s.QueryString, s.IsTracing, options);
            }
            if (statement is BoundStatement)
            {
                var s = (BoundStatement)statement;
                var options = QueryProtocolOptions.CreateFromQuery(s, defaultConsistency);
                return new ExecuteRequest(s.PreparedStatement.Id, null, s.IsTracing, options);
            }
            if (statement is BatchStatement)
            {
                var s = (BatchStatement)statement;
                var consistency = defaultConsistency;
                if (s.ConsistencyLevel != null)
                {
                    consistency = s.ConsistencyLevel.Value;
                }
                var subRequests = new List<IQueryRequest>();
                foreach (Statement q in s.Queries)
                {
                    subRequests.Add(q.CreateBatchRequest());
                }
                return new BatchRequest(s.BatchType, subRequests, consistency, s.IsTracing);
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
            var request = new PrepareRequest(query);
            return new RequestHandler<PreparedStatement>(this, request, null).Send();
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            WaitForSchemaAgreement(rs.Info.QueriedHost);
        }

        //TODO: Remove method
        public bool WaitForSchemaAgreement(IPAddress hostAddress)
        {
            if (Cluster.Metadata.AllHosts().Count == 1)
            {
                return true;
            }
            //This is trivial, but there isn't a reliable way to wait for all nodes to have the same schema.
            Thread.Sleep(1000);
            return false;
        }

        public void Dispose()
        {
            //Only dispose once
            if (Interlocked.Increment(ref _disposed) == 1)
            {
                //TODO: Cancel all pending operations and dispose every connection
            }
        }

        internal void WaitForAllPendingActions(int timeoutMs)
        {
            //TODO: Implement gracefully wait for all pending operations
            //WaitHandle.WaitAll()
        }

        internal void SetKeyspace(string keyspace)
        {
            throw new NotImplementedException();
        }

        internal void Init(bool allocate = false)
        {
            Policies.LoadBalancingPolicy.Initialize(Cluster);
        }

        //TODO: Remove
        internal CassandraConnection Connect(IEnumerator<Host> hostsIter, List<IPAddress> triedHosts, Dictionary<IPAddress, Exception> innerExceptions, out int streamId)
        {
            throw new NotImplementedException();
        }

        internal void SetHostDown(Host host)
        {
            if (Cluster.Metadata != null)
            {
                _logger.Warning("Setting host " + host.Address + " as DOWN");
                Cluster.Metadata.SetDownHost(host.Address, this);
            }
        }

        //TODO: Remove
        internal void RequestCallback(IAsyncResult ar)
        {
            throw new NotImplementedException();
        }

        internal void SimulateSingleConnectionDown()
        {
            throw new NotImplementedException();
        }

        //TODO: Remove
        internal static object GetTag(IAsyncResult ar)
        {
            throw new NotImplementedException();
        }

        internal static RetryDecision GetRetryDecision(Statement query, QueryValidationException exc, IRetryPolicy policy, int queryRetries)
        {
            throw new NotImplementedException();
        }
    }
}
