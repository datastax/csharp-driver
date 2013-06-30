using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Cassandra
{

    public class Session : IDisposable
    {
        private readonly Logger _logger = new Logger(typeof(Session));
        
        private readonly Cluster _cluster;

        internal readonly Policies _policies;
        private readonly ProtocolOptions _protocolOptions;
        private readonly PoolingOptions _poolingOptions;
        private readonly SocketOptions _socketOptions;
        private readonly ClientOptions _clientOptions;
        private readonly IAuthInfoProvider _authProvider;
        
        /// <summary>
        /// Gets name of currently used keyspace. 
        /// </summary>
        public string Keyspace { get { return _keyspace; } }
        private string _keyspace;

        public Cluster Cluster { get { return _cluster; } }

        readonly Dictionary<IPAddress, List<CassandraConnection>> _connectionPool = new Dictionary<IPAddress, List<CassandraConnection>>();

        internal Session(Cluster cluster,
                         Policies policies,
                         ProtocolOptions protocolOptions,
                         PoolingOptions poolingOptions,
                         SocketOptions socketOptions,
                         ClientOptions clientOptions,
                         IAuthInfoProvider authProvider,
                         string keyspace)
        {
            this._cluster = cluster;

            this._protocolOptions = protocolOptions;
            this._poolingOptions = poolingOptions;
            this._socketOptions = socketOptions;
            this._clientOptions = clientOptions;
            this._authProvider = authProvider;

            this._policies = policies ?? Policies.DefaultPolicies;

            this._policies.LoadBalancingPolicy.Initialize(_cluster);

            _keyspace = keyspace ?? clientOptions.DefaultKeyspace;

        }

        internal void Init()
        {
            var ci = this._policies.LoadBalancingPolicy.NewQueryPlan(null).GetEnumerator();
            if (!ci.MoveNext())
            {
                var ex = new NoHostAvailableException(new Dictionary<IPAddress, List<Exception>>());
                _logger.Error(ex.Message);
                throw ex;
            }

            var triedHosts = new List<IPAddress>();
            var innerExceptions = new Dictionary<IPAddress, List<Exception>>();
            Connect(null, ci, triedHosts, innerExceptions);
        }

        readonly List<CassandraConnection> _trashcan = new List<CassandraConnection>();

        internal CassandraConnection Connect(Query query, IEnumerator<Host> hostsIter, List<IPAddress> triedHosts, Dictionary<IPAddress, List<Exception>> innerExceptions)
        {
            CheckDisposed();
            lock (_trashcan)
            {
                foreach (var conn in _trashcan)
                {
                    if (conn.IsEmpty())
                    {
                        _logger.Error("Connection trashed");
                        conn.Dispose();
                    }
                }
            }
            lock (_connectionPool)
            {
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
                        var hostDistance = _policies.LoadBalancingPolicy.Distance(currentHost);
                        if (!_connectionPool.ContainsKey(currentHost.Address))
                            _connectionPool.Add(currentHost.Address, new List<CassandraConnection>());

                        var pool = _connectionPool[currentHost.Address];
                        var poolCpy = new List<CassandraConnection>(pool);
                        CassandraConnection toReturn = null;
                        foreach (var conn in poolCpy)
                        {
                            if (!conn.IsHealthy)
                            {
                                pool.Remove(conn);
                                conn.Dispose();
                            }
                            else
                            {
                                if (toReturn == null)
                                {
                                    if (!conn.IsBusy(_poolingOptions.GetMaxSimultaneousRequestsPerConnectionTreshold(hostDistance)))
                                        toReturn = conn;
                                }
                                else
                                {
                                    if (pool.Count > _poolingOptions.GetCoreConnectionsPerHost(hostDistance))
                                    {
                                        if (conn.IsFree(_poolingOptions.GetMinSimultaneousRequestsPerConnectionTreshold(hostDistance)))
                                        {
                                            lock (_trashcan)
                                                _trashcan.Add(conn);
                                            pool.Remove(conn);
                                        }
                                    }
                                }
                            }
                        }
                        if (toReturn != null)
                            return toReturn;
                        if (pool.Count < _poolingOptions.GetMaxConnectionPerHost(hostDistance) - 1)
                        {
                            bool error = false;
                            CassandraConnection conn = null;
                            do
                            {
                                Exception outExc;
                                conn = AllocateConnection(currentHost.Address, out outExc);
                                if (conn != null)
                                {
                                    if (_cluster.Metadata != null)
                                        _cluster.Metadata.BringUpHost(currentHost.Address, this);
                                    pool.Add(conn);
                                }
                                else
                                {
                                    if (!innerExceptions.ContainsKey(currentHost.Address))
                                        innerExceptions.Add(currentHost.Address, new List<Exception>());
                                    innerExceptions[currentHost.Address].Add(outExc);
                                    _logger.Info("New connection attempt failed - goto another host.");
                                    error = true;
                                    break;
                                }
                            }
                            while (pool.Count < _poolingOptions.GetCoreConnectionsPerHost(hostDistance));
                            if (!error)
                                return conn;
                        }
                    }
                    
                    if (!hostsIter.MoveNext())
                    {
                        var ex = new NoHostAvailableException(innerExceptions);
                        _logger.Error("Cannot connect to any host from pool.", ex);
                        throw ex;
                    }
                }
            }
        }


        internal void HostIsDown(IPAddress endpoint)
        {
			var metadata = _cluster.Metadata;
            if(metadata!=null)
                metadata.SetDownHost(endpoint, this);
        }

        CassandraConnection AllocateConnection(IPAddress endPoint, out Exception outExc)
        {
            CassandraConnection nconn = null;
            outExc = null;

            try
            {
                nconn = new CassandraConnection(this, endPoint, _protocolOptions, _socketOptions, _clientOptions, _authProvider);

                var options = nconn.ExecuteOptions();

                if (!string.IsNullOrEmpty(_keyspace))
                {
                    nconn.SetKeyspace(_keyspace);
                }
            }
            catch (Exception ex)
            {
                if (nconn != null)
                    nconn.Dispose();

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
        


        /// <summary>
        ///  Creates new keyspace in current cluster.        
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace to be created.</param>
        /// <param name="replication">Replication property for this keyspace.
        /// To set it, refer to the <see cref="ReplicationStrategies"/> class methods. 
        /// It is a dictionary of replication property sub-options where key is a sub-option name and value is a value for that sub-option. 
        /// <p>Default value is <code>'SimpleStrategy'</code> with <code>'replication_factor' = 1</code></p></param>
        /// <param name="durable_writes">Whether to use the commit log for updates on this keyspace. Default is set to <code>true</code>.</param>
        public void CreateKeyspace(string keyspace_name, Dictionary<string, string> replication = null, bool durable_writes = true)
        {
            Cluster.WaitForSchemaAgreement(
                Query(CqlQueryTools.GetCreateKeyspaceCQL(keyspace_name, replication, durable_writes), ConsistencyLevel.Default));
            _logger.Info("Keyspace [" + keyspace_name + "] has been successfully CREATED.");
        }


        /// <summary>
        ///  Creates new keyspace in current cluster.
        ///  If keyspace with specified name already exists, then this method does nothing.
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace to be created.</param>
        /// <param name="replication">Replication property for this keyspace.
        /// To set it, refer to the <see cref="ReplicationStrategies"/> class methods. 
        /// It is a dictionary of replication property sub-options where key is a sub-option name and value is a value for that sub-option.
        /// <p>Default value is <code>'SimpleStrategy'</code> with <code>'replication_factor' = 2</code></p></param>
        /// <param name="durable_writes">Whether to use the commit log for updates on this keyspace. Default is set to <code>true</code>.</param>
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

        /// <summary>
        ///  Deletes specified keyspace from current cluster.
        ///  If keyspace with specified name does not exist, then exception will be thrown.
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace to be deleted.</param>
        public void DeleteKeyspace(string keyspace_name)
        {
            Cluster.WaitForSchemaAgreement(
                Query(CqlQueryTools.GetDropKeyspaceCQL(keyspace_name), ConsistencyLevel.Default));
            _logger.Info("Keyspace [" + keyspace_name + "] has been successfully DELETED");
        }

        /// <summary>
        ///  Deletes specified keyspace from current cluster.
        ///  If keyspace with specified name does not exist, then this method does nothing.
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace to be deleted.</param>
        public void DeleteKeyspaceIfExists(string keyspace_name)
        {
            try
            {
                DeleteKeyspace(keyspace_name);
            }
            catch (InvalidConfigurationInQueryException)
            {
                _logger.Info(string.Format("Cannot DELETE keyspace:  {0}  because it not exists.", keyspace_name));
            }
        }

        /// <summary>
        ///  Switches to the specified keyspace.
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace that is to be used.</param>
        public void ChangeKeyspace(string keyspace_name)
        {
            Execute(CqlQueryTools.GetUseKeyspaceCQL(keyspace_name));
        }

        private void SetKeyspace(string keyspace_name)
        {
            lock (_connectionPool)
            {
                foreach (var kv in _connectionPool)
                {
                    foreach (var conn in kv.Value)
                    {
                        if (conn.IsHealthy)
                            conn.SetKeyspace(keyspace_name);
                    }
                }
                this._keyspace = keyspace_name;
                _logger.Info("Changed keyspace to [" + this._keyspace + "]");
            }
        }

        readonly Guarded<bool> _alreadyDisposed = new Guarded<bool>(false);

        void CheckDisposed()
        {
            lock (_alreadyDisposed)
                if (_alreadyDisposed.Value)
                    throw new ObjectDisposedException("CassandraSession");
        }

        internal void InternalDispose()
        {
            lock (_alreadyDisposed)
            {
                if (_alreadyDisposed.Value)
                    return;
                _alreadyDisposed.Value = true;

                lock (_connectionPool)
                {
                    foreach (var kv in _connectionPool)
                        foreach (var conn in kv.Value)
                            conn.Dispose();
                }
            }
        }

        public void Dispose()
        {
            lock (_alreadyDisposed)
            {
                InternalDispose();
                Cluster.SessionDisposed(this);
            }
        }

        ~Session()
        {
            Dispose();
        }

        #region Execute

        public IAsyncResult BeginExecute(Query query, object tag , AsyncCallback callback, object state)
        {
            return query.BeginSessionExecute(this, tag, callback, state);
        }

        public IAsyncResult BeginExecute(Query query, AsyncCallback callback, object state)
        {
            return query.BeginSessionExecute(this, null, callback, state);
        }

        public static object GetTag(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<RowSet>;
            return longActionAc.Tag;
        }

        public RowSet EndExecute(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<RowSet>;
            return (longActionAc.AsyncSender as Query).EndSessionExecute(this, ar);
        }

        public RowSet Execute(Query query)
        {
            return EndExecute(BeginExecute(query, null, null));
        }

        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, object tag, AsyncCallback callback, object state)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), tag, callback, state);
        }

        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), null, callback, state);
        }

        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.Default)
        {
            return Execute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency));
        }


        #endregion

        #region Prepare

        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            return BeginPrepareQuery(cqlQuery, callback, state);
        }

        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            RowSetMetadata metadata;
            var id = EndPrepareQuery(ar, out metadata);
            return new PreparedStatement(metadata, id);
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            return EndPrepare(BeginPrepare(cqlQuery, null, null));
        }
        
        #endregion

        static RetryDecision GetRetryDecision(Query query, QueryValidationException exc, IRetryPolicy policy, int queryRetries)
        {
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

        private void ProcessPrepareQuery(IOutput outp, out RowSetMetadata metadata, out byte[] queryId)
        {
            using (outp)
            {
                if (outp is OutputError)
                {
                    var ex = (outp as OutputError).CreateException();
                    _logger.Error(ex);
                    throw ex; 
                }
                else if (outp is OutputPrepared)
                {
                    queryId = (outp as OutputPrepared).QueryID;
                    metadata = (outp as OutputPrepared).Metadata;
                    _logger.Info("Prepared Query has been successfully processed.");
                    return; //ok
                }
                else
                {
                    var ex = new DriverInternalError("Unexpected output kind");
                    _logger.Error("Prepared Query has returned an unexpected output kind.", ex);
                    throw ex; 
                }
            }
        }

        private RowSet ProcessRowset(IOutput outp)
        {
            bool ok = false;
            try
            {
                if (outp is OutputError)
                {
                    var ex = (outp as OutputError).CreateException();
                    _logger.Error(ex);
                    throw ex;
                }
                else if (outp is OutputVoid)
                    return new RowSet(outp as OutputVoid, this);
                else if (outp is OutputSchemaChange)
                    return new RowSet(outp as OutputSchemaChange, this);
                else if (outp is OutputSetKeyspace)
                {
                    SetKeyspace((outp as OutputSetKeyspace).Value);
                    return new RowSet(outp as OutputSetKeyspace, this);
                }
                else if (outp is OutputRows)
                {
                    ok = true;
                    return new RowSet(outp as OutputRows, this, true);
                }
                else
                {
                    var ex = new DriverInternalError("Unexpected output kind");
                    _logger.Error(ex);
                    throw ex; 
                }
            }
            finally
            {
                if (!ok)
                    outp.Dispose();
            }
        }

        abstract class LongToken
        {
            private readonly Logger _logger = new Logger(typeof(LongToken));
            public CassandraConnection Connection;
            public ConsistencyLevel Consistency;
            public Query Query;
            private IEnumerator<Host> _hostsIter = null;
            public IAsyncResult LongActionAc;
            public readonly Dictionary<IPAddress, List<Exception>> InnerExceptions = new Dictionary<IPAddress, List<Exception>>();
            public readonly List<IPAddress> TriedHosts = new List<IPAddress>();
            public int QueryRetries = 0;
            virtual public void Connect(Session owner, bool moveNext)
            {
                if (_hostsIter == null)
                {
                    _hostsIter = owner._policies.LoadBalancingPolicy.NewQueryPlan(Query).GetEnumerator();
                    if (!_hostsIter.MoveNext())
                    {
                        var ex = new NoHostAvailableException(new Dictionary<IPAddress, List<Exception>>());
                        _logger.Error(ex);
                        throw ex;
                    }
                }
                else
                {
                    if (moveNext)
                        if (!_hostsIter.MoveNext())
                        {
                            var ex = new NoHostAvailableException(InnerExceptions);
                            _logger.Error(ex);
                            throw ex;
                        }
                }

                Connection = owner.Connect(Query, _hostsIter, TriedHosts, InnerExceptions);
            }
            abstract public void Begin(Session owner);
            abstract public void Process(Session owner, IAsyncResult ar, out object value);
            abstract public void Complete(Session owner, object value, Exception exc = null);
        }

        void ExecConn(LongToken token, bool moveNext)
        {
            while (true)
            {
                try
                {
                    token.Connect(this, moveNext);
                    token.Begin(this);
                    return;
                }
                catch (Exception ex)
                {
                    if (!CassandraConnection.IsStreamRelatedException(ex))
                    {
                        token.Complete(this, null, ex);
                        return;
                    }
                    //else
                        //retry
                }
            }
        }

        void ClbNoQuery(IAsyncResult ar)
        {
            var token = ar.AsyncState as LongToken;
            try
            {
                try
                {
                    object value;
                    token.Process(this, ar, out value);
                    token.Complete(this, value);
                }
                catch (QueryValidationException exc)
                {
                    var decision = GetRetryDecision(token.Query, exc, token.Query != null ? (token.Query.RetryPolicy ?? _policies.RetryPolicy) : _policies.RetryPolicy, token.QueryRetries);
                    if (decision == null)
                    {
                        if (!token.InnerExceptions.ContainsKey(token.Connection.GetHostAdress()))
                            token.InnerExceptions.Add(token.Connection.GetHostAdress(), new List<Exception>());

                        token.InnerExceptions[token.Connection.GetHostAdress()].Add(exc);
                        ExecConn(token, true);
                    }
                    else
                    {
                        switch (decision.DecisionType)
                        {
                            case RetryDecision.RetryDecisionType.Rethrow:
                                token.Complete(this, null, exc);
                                return;
                            case RetryDecision.RetryDecisionType.Retry:
                                if (token.LongActionAc.IsCompleted)
                                    return;
                                token.Consistency = decision.RetryConsistencyLevel.HasValue ? decision.RetryConsistencyLevel.Value : token.Consistency;
                                token.QueryRetries++;

                                if (!token.InnerExceptions.ContainsKey(token.Connection.GetHostAdress()))
                                    token.InnerExceptions.Add(token.Connection.GetHostAdress(), new List<Exception>());

                                token.InnerExceptions[token.Connection.GetHostAdress()].Add(exc);
                                ExecConn(token, false);
                                return;
                            default:
                                token.Complete(this, null);
                                return;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (CassandraConnection.IsStreamRelatedException(ex))
                {
                    if (!token.InnerExceptions.ContainsKey(token.Connection.GetHostAdress()))
                        token.InnerExceptions.Add(token.Connection.GetHostAdress(), new List<Exception>());
                    token.InnerExceptions[token.Connection.GetHostAdress()].Add(ex);
                    ExecConn(token, true);
                }
                else
                    token.Complete(this, null, ex);
            }
        }

        #region Query

        class LongQueryToken : LongToken
        {
            public string CqlQuery;
            public bool IsTracing;
            public Stopwatch StartedAt;
            override public void Connect(Session owner, bool moveNext)
            {
                StartedAt = Stopwatch.StartNew();
                base.Connect(owner, moveNext);
            }

            override public void Begin(Session owner)
            {           
                Connection.BeginQuery(CqlQuery, owner.ClbNoQuery, this, owner, Consistency, IsTracing);
            }
            override public void Process(Session owner, IAsyncResult ar, out object value)
            {
                value = owner.ProcessRowset(Connection.EndQuery(ar, owner));
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                try
                {                    
                    var ar = LongActionAc as AsyncResult<RowSet>;
                    if (exc != null)
                        ar.Complete(exc);
                    else
                    {
	                    RowSet rowset = value as RowSet;
						if(rowset==null)
							rowset = new RowSet(null,owner,false);
                        rowset.Info.SetTriedHosts(TriedHosts);
                        rowset.Info.SetAchievedConsistency(Consistency);
                        ar.SetResult(rowset);
                        ar.Complete();
					}
                }
                finally
                {                    
                    var ts = StartedAt.ElapsedTicks;
                    CassandraCounters.IncrementCqlQueryCount();
                    CassandraCounters.IncrementCqlQueryBeats((ts * 1000000000));                    
                    CassandraCounters.UpdateQueryTimeRollingAvrg((ts * 1000000000) / Stopwatch.Frequency);
                    CassandraCounters.IncrementCqlQueryBeatsBase();                    
                }
            }
        }

        internal IAsyncResult BeginQuery(string cqlQuery, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.Default, bool isTracing=false, Query query = null, object sender = null, object tag = null)
        {
            var longActionAc = new AsyncResult<RowSet>(callback, state, this, "SessionQuery", sender, tag, _clientOptions.AsyncCallAbortTimeout);
            var token = new LongQueryToken() { Consistency = consistency, CqlQuery = cqlQuery, Query = query, LongActionAc = longActionAc, IsTracing = isTracing };

            ExecConn(token, false);

            return longActionAc;
        }

        internal RowSet EndQuery(IAsyncResult ar)
        {
            return AsyncResult<RowSet>.End(ar, this, "SessionQuery");
        }

        internal RowSet Query(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.Default, bool isTracing = false, Query query = null)
        {
            return EndQuery(BeginQuery(cqlQuery, null, null, consistency,isTracing, query));
        }

        #endregion

        #region Prepare

        readonly Dictionary<byte[], string> _preparedQueries = new Dictionary<byte[], string>();


        class LongPrepareQueryToken : LongToken
        {
            public string CqlQuery;
            override public void Begin(Session owner)
            {
                Connection.BeginPrepareQuery(CqlQuery, owner.ClbNoQuery, this, owner);
            }
            override public void Process(Session owner, IAsyncResult ar, out object value)
            {
                byte[] id;
                RowSetMetadata metadata;
                owner.ProcessPrepareQuery(Connection.EndPrepareQuery(ar, owner), out metadata, out id);
                value = new KeyValuePair<RowSetMetadata, byte[]>(metadata, id);
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                var ar = LongActionAc as AsyncResult<KeyValuePair<RowSetMetadata, byte[]>>;
                if (exc != null)
                    ar.Complete(exc);
                else
                {
                    var kv = (KeyValuePair<RowSetMetadata, byte[]>)value;
                    ar.SetResult(kv);
                    lock (owner._preparedQueries)
                        owner._preparedQueries[kv.Value] = CqlQuery;
                    ar.Complete();
                }
            }
        }

        internal IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state, object sender = null, object tag = null)
        {
            var longActionAc = new AsyncResult<KeyValuePair<RowSetMetadata, byte[]>>(callback, state, this, "SessionPrepareQuery", sender, tag,  _clientOptions.AsyncCallAbortTimeout);
            var token = new LongPrepareQueryToken() { Consistency = ConsistencyLevel.Default, CqlQuery = cqlQuery, LongActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal byte[] EndPrepareQuery(IAsyncResult ar, out RowSetMetadata metadata)
        {
            var longActionAc = ar as AsyncResult<KeyValuePair<RowSetMetadata, byte[]>>;
            var ret = AsyncResult<KeyValuePair<RowSetMetadata, byte[]>>.End(ar, this, "SessionPrepareQuery");
            metadata = ret.Key;
            return ret.Value;
        }

        internal byte[] PrepareQuery(string cqlQuery, out RowSetMetadata metadata)
        {
            var ar = BeginPrepareQuery(cqlQuery, null, null, null);
            return EndPrepareQuery(ar, out metadata);
        }


        #endregion

        #region ExecuteQuery

        class LongExecuteQueryToken : LongToken
        {
            public byte[] Id;
            public string cql;
            public RowSetMetadata Metadata;
            public object[] Values;
            public bool IsTracinig;
            public Stopwatch StartedAt;
            
            override public void Connect(Session owner, bool moveNext)
            {
                StartedAt = Stopwatch.StartNew();
                base.Connect(owner, moveNext);
            }
            
            override public void Begin(Session owner)
            {
                Connection.BeginExecuteQuery(Id, cql, Metadata, Values, owner.ClbNoQuery, this, owner, Consistency, IsTracinig);
            }
            override public void Process(Session owner, IAsyncResult ar, out object value)
            {
                value = owner.ProcessRowset(Connection.EndExecuteQuery(ar, owner));
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                try
                {
                    var ar = LongActionAc as AsyncResult<RowSet>;
                    if (exc != null)
                        ar.Complete(exc);
                    else
                    {
                        RowSet rowset = value as RowSet;
                        if (rowset == null)
                            rowset = new RowSet(null, owner, false);
                        rowset.Info.SetTriedHosts(TriedHosts);
                        rowset.Info.SetAchievedConsistency(Consistency);
                        ar.SetResult(rowset);
                        ar.Complete();
                    }
                }
                finally
                {
                    var ts = StartedAt.ElapsedTicks;
                    CassandraCounters.IncrementCqlQueryCount();
                    CassandraCounters.IncrementCqlQueryBeats((ts * 1000000000));
                    CassandraCounters.UpdateQueryTimeRollingAvrg((ts * 1000000000) / Stopwatch.Frequency);
                    CassandraCounters.IncrementCqlQueryBeatsBase();
                }
            }
        }

        internal IAsyncResult BeginExecuteQuery(byte[] id, RowSetMetadata metadata, object[] values, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.Default, Query query = null, object sender = null, object tag = null, bool isTracing=false)
        {
            var longActionAc = new AsyncResult<RowSet>(callback, state, this, "SessionExecuteQuery", sender, tag, _clientOptions.AsyncCallAbortTimeout);
            var token = new LongExecuteQueryToken() { Consistency = consistency, Id = id, cql= _preparedQueries[id], Metadata = metadata, Values = values, Query = query, LongActionAc = longActionAc, IsTracinig = isTracing};

            ExecConn(token, false);

            return longActionAc;
        }

        internal RowSet EndExecuteQuery(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<RowSet>;
            return AsyncResult<RowSet>.End(ar, this, "SessionExecuteQuery");
        }

        internal RowSet ExecuteQuery(byte[] id, RowSetMetadata metadata, object[] values, ConsistencyLevel consistency = ConsistencyLevel.Default, Query query = null, bool isTracing=false)
        {
            var ar = BeginExecuteQuery(id,metadata,values, null, null, consistency, query, isTracing);
            return EndExecuteQuery(ar);
        }

        #endregion

#if ERRORINJECTION
        public void SimulateSingleConnectionDown()
        {
            lock (_connectionPool)
                if (_connectionPool.Count > 0)
                {
                    var hostidx = StaticRandom.Instance.Next(_connectionPool.Keys.Count);
                    var endpoint = new List<IPAddress>(_connectionPool.Keys)[hostidx];
                    if (_connectionPool.Count > 0)
                    {
                        var conn = _connectionPool[endpoint][StaticRandom.Instance.Next(_connectionPool[endpoint].Count)];
                        conn.KillSocket();
                    }
                }
        }

        public void SimulateAllConnectionsDown()
        {
            lock (_connectionPool)
            {
                foreach (var kv in _connectionPool)
                    foreach (var conn in kv.Value)
                        conn.KillSocket();
            }
        }
#endif
    }

    public static class ReplicationStrategies
    {
        public const string NetworkTopologyStrategy = "NetworkTopologyStrategy";
        public const string SimpleStrategy = "SimpleStrategy";


        /// <summary>
        ///  Returns replication property for SimpleStrategy.
        /// </summary>        
        /// <param name="replication_factor">Replication factor for the whole cluster.</param>
        /// <returns>a dictionary of replication property sub-options.</returns>         
        public static Dictionary<string, string> CreateSimpleStrategyReplicationProperty(int replication_factor)
        {
            return new Dictionary<string, string> { { "class", SimpleStrategy }, { "replication_factor", replication_factor.ToString() } };
        }


        /// <summary>
        ///  Returns replication property for NetworkTopologyStrategy.
        /// </summary>        
        /// <param name="datacenters_replication_factors">Dictionary in which key is the name of a data-center,
        /// value is a replication factor for that data-center.</param>
        /// <returns>a dictionary of replication property sub-options.</returns>         
        public static Dictionary<string, string> CreateNetworkTopologyStrategyReplicationProperty(Dictionary<string, int> datacenters_replication_factors)
        {
            Dictionary<string, string> result = new Dictionary<string, string> { { "class", NetworkTopologyStrategy } };
            if (datacenters_replication_factors.Count > 0)
                foreach (var datacenter in datacenters_replication_factors)
                    result.Add(datacenter.Key, datacenter.Value.ToString());
            return result;
        }


        /// <summary>
        ///  Returns replication property for other replication strategy. 
        ///  Use it only if there is no dedicated method that creates replication property for specified replication strategy.
        /// </summary>
        /// <param name="strategy_class">Name of replication strategy.</param>
        /// <param name="sub_options">Dictionary in which key is the name of sub-option,
        /// value is a value for that sub-option.</param>
        /// <returns>a dictionary of replication property sub-options.</returns>         
        public static Dictionary<string, string> CreateReplicationProperty(string strategy_class, Dictionary<string, string> sub_options)
        {
            Dictionary<string, string> result = new Dictionary<string, string> { { "class", strategy_class } };
            if (sub_options.Count > 0)
                foreach (var elem in sub_options)
                    result.Add(elem.Key, elem.Value);
            return result;
        }
    }

}
