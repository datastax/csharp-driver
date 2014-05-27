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
        internal Guid Guid;

        private readonly Logger _logger = new Logger(typeof(Session));
        
        private readonly Cluster _cluster;

        internal readonly Policies Policies;
        private readonly ProtocolOptions _protocolOptions;
        private readonly PoolingOptions _poolingOptions;
        private readonly SocketOptions _socketOptions;
        private readonly ClientOptions _clientOptions;
        private readonly IAuthProvider _authProvider;
        private readonly IAuthInfoProvider _authInfoProvider;
        
        public string Keyspace { get { return _keyspace; } }
        private string _keyspace;
        private int _binaryProtocolVersion;

        public Cluster Cluster { get { return _cluster; } }

        readonly ConcurrentDictionary<IPAddress, ConcurrentDictionary<Guid, CassandraConnection>> _connectionPool = new ConcurrentDictionary<IPAddress, ConcurrentDictionary<Guid, CassandraConnection>>();
        readonly ConcurrentDictionary<IPAddress, AtomicValue<int>> _allocatedConnections = new ConcurrentDictionary<IPAddress, AtomicValue<int>>();

        internal Session(Cluster cluster,
                         Policies policies,
                         ProtocolOptions protocolOptions,
                         PoolingOptions poolingOptions,
                         SocketOptions socketOptions,
                         ClientOptions clientOptions,
                         IAuthProvider authProvider,
                         IAuthInfoProvider authInfoProvider,
                         string keyspace,
                         int binaryProtocolVersion)
        {
            _binaryProtocolVersion = binaryProtocolVersion;
            _cluster = cluster;

            _protocolOptions = protocolOptions;
            _poolingOptions = poolingOptions;
            _socketOptions = socketOptions;
            _clientOptions = clientOptions;
            _authProvider = authProvider;
            _authInfoProvider = authInfoProvider;

            Policies = policies ?? Policies.DefaultPolicies;

            Policies.LoadBalancingPolicy.Initialize(_cluster);

            _keyspace = keyspace ?? clientOptions.DefaultKeyspace;

            Guid = Guid.NewGuid();
        }

        public int BinaryProtocolVersion { get { return _binaryProtocolVersion; } }

        Timer _trashcanCleaner;

        internal void Init(bool allock=true)
        {
            if (allock)
            {
                var ci = Policies.LoadBalancingPolicy.NewQueryPlan(null).GetEnumerator();
                if (!ci.MoveNext())
                {
                    var ex = new NoHostAvailableException(new Dictionary<IPAddress, List<Exception>>());
                    _logger.Error(ex.Message);
                    throw ex;
                }

                var triedHosts = new List<IPAddress>();
                var innerExceptions = new Dictionary<IPAddress, List<Exception>>();
                int streamId;
                var con = Connect( ci, triedHosts, innerExceptions, out streamId);
                con.FreeStreamId(streamId);
            }

            _trashcanCleaner = new Timer(TranscanCleanup, null, Timeout.Infinite, Timeout.Infinite);
        }

        readonly ConcurrentDictionary<IPAddress, ConcurrentDictionary<Guid, CassandraConnection>> _trashcan = new ConcurrentDictionary<IPAddress, ConcurrentDictionary<Guid, CassandraConnection>>();

        void TranscanCleanup(object state)
        {
            _trashcanCleaner.Change(Timeout.Infinite, Timeout.Infinite);

            foreach (var kv in _trashcan)
            {
                foreach(var ckv in kv.Value)
                {
                    CassandraConnection conn;
                    if(kv.Value.TryRemove(ckv.Key,out conn))
                    {
                        if (conn.IsEmpty())
                        {
                            _logger.Info("Connection trashed");
                            FreeConnection(conn);
                        }
                        else
                        {
                            kv.Value.TryAdd(conn.Guid,conn);
                        }
                    }
                }
            }
        }

        void TrashcanPut(CassandraConnection conn)
        {
            RETRY:
            if (!_trashcan.ContainsKey(conn.GetHostAdress()))
                _trashcan.TryAdd(conn.GetHostAdress(), new ConcurrentDictionary<Guid, CassandraConnection>());

            ConcurrentDictionary<Guid, CassandraConnection> trashes;
            if (_trashcan.TryGetValue(conn.GetHostAdress(), out trashes))
                trashes.TryAdd(conn.Guid,conn);
            else
                goto RETRY;

            _trashcanCleaner.Change(10000, Timeout.Infinite);
        }

        CassandraConnection TrashcanRecycle(IPAddress addr)
        {
            if (!_trashcan.ContainsKey(addr))
                return null;

            ConcurrentDictionary<Guid, CassandraConnection> trashes;
            if (_trashcan.TryGetValue(addr, out trashes))
            {
                foreach(var ckv in trashes)
                {
                    CassandraConnection conn;
                    if(trashes.TryRemove(ckv.Key,out conn))
                        return conn;
                }
            }

            return null;
        }

        internal CassandraConnection Connect(IEnumerator<Host> hostsIter, List<IPAddress> triedHosts, Dictionary<IPAddress, List<Exception>> innerExceptions, out int streamId)
        {
            CheckDisposed();

            while (true)
            {
                var currentHost = hostsIter.Current;
                if (currentHost == null)
                {
                    var ex = new NoHostAvailableException(innerExceptions);
                    _logger.Error("All hosts are not responding.", ex);
                    throw ex;
                }
                if (currentHost.IsConsiderablyUp)
                {
                    triedHosts.Add(currentHost.Address);
                    var hostDistance = Policies.LoadBalancingPolicy.Distance(currentHost);
                RETRY_GET_POOL:
                    if (!_connectionPool.ContainsKey(currentHost.Address))
                        _connectionPool.TryAdd(currentHost.Address, new ConcurrentDictionary<Guid, CassandraConnection>());

                    ConcurrentDictionary<Guid, CassandraConnection> pool;

                    if (!_connectionPool.TryGetValue(currentHost.Address, out pool))
                        goto RETRY_GET_POOL;

//                    CassandraCounters.SetConnectionsCount(currentHost.Address, pool.Count);
                    foreach (var kv in pool)
                    {
                        CassandraConnection conn = kv.Value;
                        if (!conn.IsHealthy)
                        {
                            CassandraConnection cc;
                            if(pool.TryRemove(conn.Guid, out cc))
                                FreeConnection(cc);
                        }
                        else
                        {
                            if (!conn.IsBusy(_poolingOptions.GetMaxSimultaneousRequestsPerConnectionTreshold(hostDistance)))
                            {
                                streamId = conn.AllocateStreamId();
                                return conn;
                            }
                            else
                            {
                                if (pool.Count > _poolingOptions.GetCoreConnectionsPerHost(hostDistance))
                                {
                                    if (conn.IsFree(_poolingOptions.GetMinSimultaneousRequestsPerConnectionTreshold(hostDistance)))
                                    {
                                        CassandraConnection cc;
                                        if (pool.TryRemove(conn.Guid, out cc))
                                            TrashcanPut(cc);
                                    }
                                }
                            }
                        }
                    }
                    {

                        var conn = TrashcanRecycle(currentHost.Address);
                        if (conn != null)
                        {
                            if (!conn.IsHealthy)
                                FreeConnection(conn);
                            else
                            {
                                pool.TryAdd(conn.Guid, conn);
                                streamId = conn.AllocateStreamId();
                                return conn;
                            }
                        }
                        // if not recycled
                        {
                            Exception outExc;
                            conn = AllocateConnection(currentHost.Address, hostDistance, out outExc);
                            if (conn != null)
                            {
                                if (_cluster.Metadata != null)
                                    _cluster.Metadata.BringUpHost(currentHost.Address, this);
                                pool.TryAdd(conn.Guid, conn);
                                streamId = conn.AllocateStreamId();
                                return conn;
                            }
                            else
                            {
                                if (!innerExceptions.ContainsKey(currentHost.Address))
                                    innerExceptions.Add(currentHost.Address, new List<Exception>());
                                innerExceptions[currentHost.Address].Add(outExc);
                                _logger.Info("New connection attempt failed - goto another host.");
                            }
                        }
                    }
                }

                _logger.Verbose(string.Format("Currently tried host: {0} have all of connections busy. Switching to the next host.", currentHost.Address));

                if (!hostsIter.MoveNext())
                {
                    var ex = new NoHostAvailableException(innerExceptions);
                    _logger.Error("Cannot connect to any host from pool.", ex);
                    throw ex;
                }
            }
        }


        internal void HostIsDown(IPAddress endpoint)
        {
			var metadata = _cluster.Metadata;
            if(metadata!=null)
                metadata.SetDownHost(endpoint, this);
        }

        void FreeConnection(CassandraConnection connection)
        {
            connection.Dispose();
            AtomicValue<int> val;
            _allocatedConnections.TryGetValue(connection.GetHostAdress(), out val);
            var no = Interlocked.Decrement(ref val.RawValue);
        }

        CassandraConnection AllocateConnection(IPAddress endPoint, HostDistance hostDistance, out Exception outExc)
        {
            CassandraConnection nconn = null;
            outExc = null;

            try
            {
                int no = 1;
                if (!_allocatedConnections.TryAdd(endPoint, new AtomicValue<int>(1)))
                {
                    AtomicValue<int> val;
                    _allocatedConnections.TryGetValue(endPoint, out val);
                    no = Interlocked.Increment(ref val.RawValue);
                    if (no > _poolingOptions.GetMaxConnectionPerHost(hostDistance))
                    {
                        Interlocked.Decrement(ref val.RawValue);
                        outExc = new ToManyConnectionsPerHost();
                        return null;
                    }
                }

            RETRY:
                nconn = new CassandraConnection(this, endPoint, _protocolOptions, _socketOptions, _clientOptions, _authProvider, _authInfoProvider,_binaryProtocolVersion);

                var streamId = nconn.AllocateStreamId();

                try
                {
                    var options = ProcessExecuteOptions(nconn.ExecuteOptions(streamId));
                }
                catch (CassandraConnectionBadProtocolVersionException)
                {
                    if (_binaryProtocolVersion == 1)
                        throw;
                    else
                    {
                        _binaryProtocolVersion = 1;
                        goto RETRY;
                    }
                }

                if (!string.IsNullOrEmpty(_keyspace))
                    nconn.SetKeyspace(_keyspace);
            }
            catch (Exception ex)
            {
                if (nconn != null)
                {
                    nconn.Dispose();
                    nconn = null;
                }

                AtomicValue<int> val;
                _allocatedConnections.TryGetValue(endPoint, out val);
                Interlocked.Decrement(ref val.RawValue); 

                if (CassandraConnection.IsStreamRelatedException(ex))
                {
                    HostIsDown(endPoint);
                    outExc = ex;
                    return null;
                }
                else
                    throw ex;
            }

            _logger.Info("Allocated new connection");            
            
            return nconn;
        }
        
        public void CreateKeyspace(string keyspace_name, Dictionary<string, string> replication = null, bool durable_writes = true)
        {
            WaitForSchemaAgreement(Execute(CqlQueryTools.GetCreateKeyspaceCql(keyspace_name, replication, durable_writes, false)));
            _logger.Info("Keyspace [" + keyspace_name + "] has been successfully CREATED.");
        }

        public void CreateKeyspaceIfNotExists(string keyspace_name, Dictionary<string, string> replication = null, bool durable_writes = true)
        {
            try
            {
                CreateKeyspace(keyspace_name, replication, durable_writes);
            }
            catch (AlreadyExistsException)
            {
                _logger.Info(string.Format("Cannot CREATE keyspace:  {0}  because it already exists.", keyspace_name));
            }
        }
        public void DeleteKeyspace(string keyspace_name)
        {
            WaitForSchemaAgreement(Execute(CqlQueryTools.GetDropKeyspaceCql(keyspace_name, false)));
            _logger.Info("Keyspace [" + keyspace_name + "] has been successfully DELETED");
        }

        public void DeleteKeyspaceIfExists(string keyspace_name)
        {
            if (_binaryProtocolVersion > 1)
            {
                WaitForSchemaAgreement(Execute(CqlQueryTools.GetDropKeyspaceCql(keyspace_name, true)));
                _logger.Info("Keyspace [" + keyspace_name + "] has been successfully DELETED");
            }
            else
            {
                try
                {
                    DeleteKeyspace(keyspace_name);
                }
                catch (InvalidQueryException)
                {
                    _logger.Info(string.Format("Cannot DELETE keyspace:  {0}  because it not exists.", keyspace_name));
                }
            }
        }

        public void ChangeKeyspace(string keyspace_name)
        {
            Execute(CqlQueryTools.GetUseKeyspaceCql(keyspace_name));
        }

        internal void SetKeyspace(string keyspace_name)
        {
            foreach (var kv in _connectionPool)
            {
                foreach (var kvv in kv.Value)
                {
                    var conn = kvv.Value;
                    if (conn.IsHealthy)
                        conn.SetKeyspace(keyspace_name);
                }
            }
            foreach (var kv in _trashcan)
            {
                foreach (var ckv in kv.Value)
                {
                    if (ckv.Value.IsHealthy)
                        ckv.Value.SetKeyspace(keyspace_name);
                }
            }
            _keyspace = keyspace_name;
            _logger.Info("Changed keyspace to [" + _keyspace + "]");
        }

        BoolSwitch _alreadyDisposed = new BoolSwitch();

        void CheckDisposed()
        {
            if (_alreadyDisposed.IsTaken())
                throw new ObjectDisposedException("CassandraSession");
        }

        internal void InternalDispose()
        {
            if (!_alreadyDisposed.TryTake())
            {
                return;
            }

            if (_trashcanCleaner != null)
            {
                _trashcanCleaner.Change(Timeout.Infinite, Timeout.Infinite);
            }

            foreach (var kv in _connectionPool)
            {
                foreach (var kvv in kv.Value)
                {
                    var conn = kvv.Value;
                    FreeConnection(conn);
                }
            }
            foreach (var kv in _trashcan)
            {
                foreach (var ckv in kv.Value)
                {
                    FreeConnection(ckv.Value);
                }
            }
        }

        public void Dispose()
        {
            InternalDispose();
            Cluster.SessionDisposed(this);
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~Session()
        {
            try
            {
                Dispose(); 
            }
            catch
            {
                //Finalizers are called in their own specific threads.
                //There is no point in throwing an exception
            }
        }

        public bool IsDisposed
        {
            get { return this._alreadyDisposed.IsTaken(); }
        }

        #region Execute

        private ConcurrentDictionary<long, IAsyncResult> _startedActons = new ConcurrentDictionary<long, IAsyncResult>();

        /// <inheritdoc />
        public IAsyncResult BeginExecute(IStatement statement, object tag, AsyncCallback callback, object state)
        {
            if (statement == null)
            {
                throw new ArgumentNullException("statement");
            }
            var options = Cluster.Configuration.QueryOptions;
            var consistency = statement.ConsistencyLevel ?? options.GetConsistencyLevel();
            if (statement is RegularStatement)
            {
                return BeginQuery((RegularStatement)statement, callback, state, tag);
            }
            if (statement is BoundStatement)
            {
                return BeginExecuteQuery((BoundStatement)statement, callback, state, tag);
            }
            if (statement is BatchStatement)
            {
                return BeginBatch((BatchStatement)statement, callback, state, tag);
            }
            throw new NotSupportedException("Statement of type " + statement.GetType().FullName + " not supported");
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            return BeginExecute(statement, null, callback, state);
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, object tag, AsyncCallback callback, object state)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), tag, callback, state);
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            return BeginExecute(cqlQuery, consistency, null, callback, state);
        }

        public static object GetTag(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<RowSet>;
            return longActionAc.Tag;
        }

        /// <inheritdoc />
        public RowSet EndExecute(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<RowSet>;
            IAsyncResult oar;
            _startedActons.TryRemove(longActionAc.Id, out oar);
            if (longActionAc.AsyncSender is RegularStatement)
            {
                return EndQuery(ar);
            }
            if (longActionAc.AsyncSender is BoundStatement)
            {
                return EndExecuteQuery(ar);
            }
            if (longActionAc.AsyncSender is BatchStatement)
            {
                return EndBatch(ar);
            }
            throw new NotSupportedException("Async result sender not supported " + longActionAc.AsyncSender);
        }

        internal void WaitForAllPendingActions(int timeoutMs)
        {
            while (_startedActons.Count > 0)
            {
                var it = _startedActons.GetEnumerator();
                it.MoveNext();
                var ar = it.Current.Value as AsyncResultNoResult;
                ar.AsyncWaitHandle.WaitOne(timeoutMs);
                IAsyncResult oar;
                _startedActons.TryRemove(ar.Id, out oar);
            }
        }


        /// <inheritdoc />
        public RowSet Execute(IStatement query)
        {
            return EndExecute(BeginExecute(query, null, null));
        }


        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            return Execute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency).SetPageSize(_cluster.Configuration.QueryOptions.GetPageSize()));
        }


        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, int pageSize)
        {
            return Execute(new SimpleStatement(cqlQuery).SetConsistencyLevel(_cluster.Configuration.QueryOptions.GetConsistencyLevel()).SetPageSize(pageSize));
        }


        /// <inheritdoc />
        public RowSet Execute(string cqlQuery)
        {
            return Execute(new SimpleStatement(cqlQuery).SetConsistencyLevel(_cluster.Configuration.QueryOptions.GetConsistencyLevel()).SetPageSize(_cluster.Configuration.QueryOptions.GetPageSize()));
        }


        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement query)
        {
            return Task.Factory.FromAsync<IStatement, RowSet>(BeginExecute, EndExecute, query, null);
        }
        #endregion

        #region Prepare

        /// <inheritdoc />
        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            var ar = BeginPrepareQuery(cqlQuery, callback, state) as AsyncResultNoResult;
            _startedActons.TryAdd(ar.Id, ar);
            return ar;
        }

        /// <inheritdoc />
        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            IAsyncResult oar;
            _startedActons.TryRemove((ar as AsyncResultNoResult).Id, out oar);
            RowSetMetadata metadata;
            var id = EndPrepareQuery(ar, out metadata);
            return new PreparedStatement(metadata, id.Item1, id.Item2, id.Item3);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery)
        {
            return EndPrepare(BeginPrepare(cqlQuery, null, null));
        }
        
        #endregion

        internal static RetryDecision GetRetryDecision(Statement query, QueryValidationException exc, IRetryPolicy policy, int queryRetries)        {
            if (exc is OverloadedException) return RetryDecision.Retry(null);
            else if (exc is IsBootstrappingException) return RetryDecision.Retry(null);
            else if (exc is TruncateException) return RetryDecision.Retry(null);

            else if (exc is ReadTimeoutException)
            {
                var e = exc as ReadTimeoutException;
                return policy.OnReadTimeout(query, e.ConsistencyLevel, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, e.WasDataRetrieved, queryRetries);
            }
            else if (exc is WriteTimeoutException)
            {
                var e = exc as WriteTimeoutException;
                return policy.OnWriteTimeout(query, e.ConsistencyLevel, e.WriteType, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, queryRetries);
            }
            else if (exc is UnavailableException)
            {
                var e = exc as UnavailableException;
                return policy.OnUnavailable(query, e.Consistency, e.RequiredReplicas, e.AliveReplicas, queryRetries);
            }

            else if (exc is AlreadyExistsException) return RetryDecision.Rethrow();
            else if (exc is InvalidConfigurationInQueryException) return RetryDecision.Rethrow();
            else if (exc is PreparedQueryNotFoundException) return RetryDecision.Rethrow();
            else if (exc is ProtocolErrorException) return RetryDecision.Rethrow();
            else if (exc is InvalidQueryException) return RetryDecision.Rethrow();
            else if (exc is UnauthorizedException) return RetryDecision.Rethrow();
            else if (exc is SyntaxError) return RetryDecision.Rethrow();

            else if (exc is ServerErrorException) return null;
            else return null;
        }

        private IDictionary<string, string[]> ProcessExecuteOptions(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                {
                    var ex = (outp as OutputError).CreateException();
                    _logger.Error(ex);
                    throw ex;
                }
                else if (outp is OutputOptions)
                {
                    return (outp as OutputOptions).Options;
                }
                else
                {
                    var ex = new DriverInternalError("Unexpected output kind");
                    _logger.Error("Prepared Query has returned an unexpected output kind.", ex);
                    throw ex;
                }
            }
        }

        void ExecConn(RequestHandler handler, bool moveNext)
        {
            while (true)
            {
                try
                {
                    int streamId;
                    handler.Connect(this, moveNext, out streamId);
                    handler.Begin(this,streamId);
                    return;
                }
                catch (Exception ex)
                {
                    if (!CassandraConnection.IsStreamRelatedException(ex))
                    {
                        handler.Complete(this, null, ex);
                        return;
                    }
                    else if (_alreadyDisposed.IsTaken())
                    {
                        return;
                    }
                }
            }
        }

        internal void RequestCallback(IAsyncResult ar)
        {
            var handler = ar.AsyncState as RequestHandler;
            try
            {
                try
                {
                    object value;
                    handler.Process(this, ar, out value);
                    handler.Complete(this, value);
                }
                catch (QueryValidationException exc)
                {
                    var decision = GetRetryDecision(handler.Statement, exc, handler.Statement != null ? (handler.Statement.RetryPolicy ?? Policies.RetryPolicy) : Policies.RetryPolicy, handler.QueryRetries);
                    if (decision == null)
                    {
                        if (!handler.InnerExceptions.ContainsKey(handler.Connection.GetHostAdress()))
                            handler.InnerExceptions.Add(handler.Connection.GetHostAdress(), new List<Exception>());

                        handler.InnerExceptions[handler.Connection.GetHostAdress()].Add(exc);
                        ExecConn(handler, true);
                    }
                    else
                    {
                        switch (decision.DecisionType)
                        {
                            case RetryDecision.RetryDecisionType.Rethrow:
                                handler.Complete(this, null, exc);
                                return;
                            case RetryDecision.RetryDecisionType.Retry:
                                if (handler.LongActionAc.IsCompleted)
                                    return;
                                handler.Consistency = (decision.RetryConsistencyLevel.HasValue && (decision.RetryConsistencyLevel.Value<ConsistencyLevel.Serial)) ? decision.RetryConsistencyLevel.Value : handler.Consistency;
                                handler.QueryRetries++;

                                if (!handler.InnerExceptions.ContainsKey(handler.Connection.GetHostAdress()))
                                    handler.InnerExceptions.Add(handler.Connection.GetHostAdress(), new List<Exception>());

                                handler.InnerExceptions[handler.Connection.GetHostAdress()].Add(exc);
                                ExecConn(handler, exc is UnavailableException);
                                return;
                            default:
                                handler.Complete(this, null);
                                return;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (CassandraConnection.IsStreamRelatedException(ex))
                {
                    if (!handler.InnerExceptions.ContainsKey(handler.Connection.GetHostAdress()))
                    {
                        handler.InnerExceptions.Add(handler.Connection.GetHostAdress(), new List<Exception>());
                    }
                    handler.InnerExceptions[handler.Connection.GetHostAdress()].Add(ex);
                    ExecConn(handler, true);
                }
                else
                {
                    handler.Complete(this, null, ex);
                }
            }
        }

        internal IAsyncResult BeginQuery(RegularStatement query, AsyncCallback callback, object state, object tag = null)
        {
            var options = QueryProtocolOptions.CreateFromQuery(query, Cluster.Configuration.QueryOptions.GetConsistencyLevel());
            var longActionAc = new AsyncResult<RowSet>(-1, callback, state, this, "SessionQuery", query, tag);
            var handler = new QueryRequestHandler() 
            { 
                Consistency = options.Consistency, 
                CqlQuery = query.QueryString, 
                Statement = query, 
                QueryProtocolOptions = options, 
                LongActionAc = longActionAc, 
                IsTracing = query.IsTracing 
            };

            ExecConn(handler, false);

            return longActionAc;
        }

        internal RowSet EndQuery(IAsyncResult ar)
        {
            return AsyncResult<RowSet>.End(ar, this, "SessionQuery");
        }

        private readonly ConcurrentDictionary<byte[], string> _preparedQueries = new ConcurrentDictionary<byte[], string>();

        internal virtual void AddPreparedQuery(byte[] id, string query)
        {
            _preparedQueries.AddOrUpdate(id, query, (k, o) => o);
        }

        internal virtual string GetPreparedQuery(byte[] id)
        {
            return _preparedQueries[id];
        }

        internal IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state, object sender = null, object tag = null)
        {
            var longActionAc = new AsyncResult<KeyValuePair<RowSetMetadata, Tuple<byte[],string, RowSetMetadata>>>(-1, callback, state, this, "SessionPrepareQuery", sender, tag);
            var handler = new PrepareRequestHandler() { Consistency = _cluster.Configuration.QueryOptions.GetConsistencyLevel(), CqlQuery = cqlQuery, LongActionAc = longActionAc };

            ExecConn(handler, false);

            return longActionAc;
        }

        internal Tuple<byte[], string, RowSetMetadata> EndPrepareQuery(IAsyncResult ar, out RowSetMetadata metadata)
        {
            var longActionAc = ar as AsyncResult<KeyValuePair<RowSetMetadata, byte[]>>;
            var ret = AsyncResult<KeyValuePair<RowSetMetadata, Tuple<byte[], string, RowSetMetadata>>>.End(ar, this, "SessionPrepareQuery");
            metadata = ret.Key;
            return ret.Value;
        }

        internal Tuple<byte[], string, RowSetMetadata> PrepareQuery(string cqlQuery, out RowSetMetadata metadata)
        {
            var ar = BeginPrepareQuery(cqlQuery, null, null, null);
            return EndPrepareQuery(ar, out metadata);
        }

        internal IAsyncResult BeginExecuteQuery(BoundStatement statement, AsyncCallback callback, object state, object tag = null)
        {
            var options = QueryProtocolOptions.CreateFromQuery(statement, Cluster.Configuration.QueryOptions.GetConsistencyLevel());
            var queryId = statement.PreparedStatement.Id;

            var longActionAc = new AsyncResult<RowSet>(-1, callback, state, this, "SessionExecuteQuery", statement, tag);
            var handler = new ExecuteQueryRequestHandler() 
            { 
                Consistency = options.Consistency, 
                Id = queryId, 
                CqlQuery = GetPreparedQuery(queryId), 
                Metadata = statement.PreparedStatement.Metadata,
                QueryProtocolOptions = options, 
                Statement = statement, 
                LongActionAc = longActionAc, 
                IsTracing = statement.IsTracing
            };
            ExecConn(handler, false);
            return longActionAc;
        }

        internal RowSet EndExecuteQuery(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<RowSet>;
            return AsyncResult<RowSet>.End(ar, this, "SessionExecuteQuery");
        }

        internal IAsyncResult BeginBatch(BatchStatement statement, AsyncCallback callback, object state, object tag = null)
        {
            var longActionAc = new AsyncResult<RowSet>(-1, callback, state, this, "SessionBatch", statement, tag);
            var handler = new BatchRequestHandler() 
            { 
                Consistency = statement.ConsistencyLevel ?? _cluster.Configuration.QueryOptions.GetConsistencyLevel(), 
                Queries = statement.Queries, 
                BatchType = statement.BatchType, 
                Statement = statement, 
                LongActionAc = longActionAc,
                IsTracing = statement.IsTracing 
            };
            ExecConn(handler, false);
            return longActionAc;
        }

        /// <inheritdoc />
        internal RowSet EndBatch(IAsyncResult ar)
        {
            return AsyncResult<RowSet>.End(ar, this, "SessionBatch");
        }

        internal const long MaxSchemaAgreementWaitMs = 10000;
        internal static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);


        public void WaitForSchemaAgreement(RowSet rs)
        {
            WaitForSchemaAgreement(rs.Info.QueriedHost);
        }

        public bool WaitForSchemaAgreement(IPAddress forHost)
        {
            var start = DateTimeOffset.Now;
            long elapsed = 0;
            while (elapsed < MaxSchemaAgreementWaitMs)
            {
                var versions = new HashSet<Guid>();

                int streamId1;
                int streamId2;
                CassandraConnection connection;
                {
                    var localhost = _cluster.Metadata.GetHost(forHost);
                    var iterLiof = new List<Host>() { localhost }.GetEnumerator();
                    iterLiof.MoveNext();
                    List<IPAddress> tr = new List<IPAddress>();
                    Dictionary<IPAddress, List<Exception>> exx = new Dictionary<IPAddress, List<Exception>>();

                    connection = Connect(iterLiof, tr, exx, out streamId1);
                    while (true)
                    {
                        try
                        {
                            streamId2 = connection.AllocateStreamId();
                            break;
                        }
                        catch (CassandraConnection.StreamAllocationException)
                        {
                            Thread.Sleep(100);
                        }
                    }
                }
                {

                    using (var outp = connection.Query(streamId1, CqlQueryTools.SelectSchemaPeers, false, QueryProtocolOptions.Default, _cluster.Configuration.QueryOptions.GetConsistencyLevel()))
                    {
                        if (outp is OutputRows)
                        {
                            var requestHandler = new QueryRequestHandler();
                            var rowset = requestHandler.ProcessResponse(outp, null);
                            foreach (var row in rowset.GetRows())
                            {
                                if (row.IsNull("rpc_address") || row.IsNull("schema_version"))
                                    continue;

                                var rpc = row.GetValue<IPAddress>("rpc_address");
                                if (rpc.Equals(BindAllAddress))
                                {
                                    if (!row.IsNull("peer"))
                                        rpc = row.GetValue<IPAddress>("peer");
                                }

                                Host peer = _cluster.Metadata.GetHost(rpc);
                                if (peer != null && peer.IsConsiderablyUp)
                                    versions.Add(row.GetValue<Guid>("schema_version"));
                            }
                        }
                    }
                }

                {
                    using (var outp = connection.Query(streamId2, CqlQueryTools.SelectSchemaLocal, false, QueryProtocolOptions.Default, _cluster.Configuration.QueryOptions.GetConsistencyLevel()))
                    {
                        if (outp is OutputRows)
                        {
                            var requestHandler = new QueryRequestHandler();
                            var rowset = requestHandler.ProcessResponse(outp, null);
                            foreach (var localRow in rowset.GetRows())
                            {
                                if (!localRow.IsNull("schema_version"))
                                {
                                    versions.Add(localRow.GetValue<Guid>("schema_version"));
                                    break;
                                }
                            }
                        }
                    }
                }


                if (versions.Count <= 1)
                    return true;

                // let's not flood the node too much
                Thread.Sleep(200);
                elapsed = (long)(DateTimeOffset.Now - start).TotalMilliseconds;
            }

            return false;
        }

        internal void SimulateSingleConnectionDown()
        {
            if (_connectionPool.Count > 0)
            {
                var endpoints = new List<IPAddress>(_connectionPool.Keys);
                var hostidx = StaticRandom.Instance.Next(endpoints.Count);
                var endpoint = endpoints[hostidx];
                ConcurrentDictionary<Guid, CassandraConnection> pool;
                if (_connectionPool.TryGetValue(endpoint, out pool))
                {
                    var items = new List<Guid>(pool.Keys);
                    if (items.Count == 0)
                        return;
                    var conidx = StaticRandom.Instance.Next(items.Count);
                    var k = items[conidx];
                    CassandraConnection con;
                    if (pool.TryGetValue(k, out con))
                        con.KillSocket();
                }
            }
        }
    }
}
