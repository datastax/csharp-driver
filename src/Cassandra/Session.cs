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
using Cassandra.RequestHandlers;

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ISession"/>.
    /// </summary>
    /// <inheritdoc cref="Cassandra.ISession" />
    public class Session : ISession
    {
        internal static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);

        public int BinaryProtocolVersion { get; protected set; }

        public Cluster Cluster { get; private set; }

        /// <summary>
        /// Gets the cluster configuration
        /// </summary>
        public Configuration Configuration { get; protected set; }

        public bool IsDisposed
        {
            get { throw new NotImplementedException(); }
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
        public void ChangeKeyspace(string keyspaceName)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void CreateKeyspace(string keyspaceName, Dictionary<string, string> replication = null, bool durable_writes = true)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void CreateKeyspaceIfNotExists(string keyspace_name, Dictionary<string, string> replication = null, bool durable_writes = true)
        {
            throw new NotImplementedException();
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

        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            throw new NotImplementedException();
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            throw new NotImplementedException();
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            throw new NotImplementedException();
        }

        public bool WaitForSchemaAgreement(IPAddress forHost)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        internal void WaitForAllPendingActions(int timeoutMs)
        {
            throw new NotImplementedException();
        }

        internal void SetKeyspace(string keyspace)
        {
            throw new NotImplementedException();
        }

        internal void Init(bool allocate = false)
        {
            throw new NotImplementedException();
        }

        internal void HostIsDown(IPAddress address)
        {
            throw new NotImplementedException();
        }

        //TODO: Remove and replace by another
        internal CassandraConnection Connect(IEnumerator<Host> hostsIter, List<IPAddress> triedHosts, Dictionary<IPAddress, List<Exception>> innerExceptions, out int streamId)
        {
            throw new NotImplementedException();
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
