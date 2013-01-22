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

        private Cluster _cluster;

        internal readonly Policies _policies;
        private readonly ProtocolOptions _protocolOptions;
        private readonly PoolingOptions _poolingOptions;
        private readonly SocketOptions _socketOptions;
        private readonly ClientOptions _clientOptions;
        private readonly IAuthInfoProvider _authProvider;
        private readonly bool _metricsEnabled;

        public string Keyspace { get { return _keyspace; } }
        private string _keyspace;

        private readonly Hosts _hosts;
        readonly Dictionary<IPAddress, List<CassandraConnection>> _connectionPool = new Dictionary<IPAddress, List<CassandraConnection>>();
        internal readonly ControlConnection ControlConnection;

        internal Session(Cluster cluster,
                         IEnumerable<IPAddress> clusterEndpoints,
                         Policies policies,
                         ProtocolOptions protocolOptions,
                         PoolingOptions poolingOptions,
                         SocketOptions socketOptions,
                         ClientOptions clientOptions,
                         IAuthInfoProvider authProvider,
                         bool metricsEnabled,
                         string keyspace=null,
                         Hosts hosts = null)
        {
            this._cluster = cluster;

            this._protocolOptions = protocolOptions;
            this._poolingOptions = poolingOptions;
            this._socketOptions = socketOptions;
            this._clientOptions = clientOptions;
            this._authProvider = authProvider;
            this._metricsEnabled = metricsEnabled;

            this._policies = policies ?? Policies.DefaultPolicies;

            _hosts = hosts ?? new Hosts();

            foreach (var ep in clusterEndpoints)
                _hosts.AddIfNotExistsOrBringUpIfDown(ep, this._policies.ReconnectionPolicy);

            if (hosts == null)
            {
                var controlpolicies = new Cassandra.Policies(
                    new RoundRobinPolicy(),
                    new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000),
                    Cassandra.Policies.DefaultRetryPolicy);

                ControlConnection = new ControlConnection(_cluster, this, new List<IPAddress>(), controlpolicies, _protocolOptions,
                                                           _poolingOptions, _socketOptions,
                                                           new ClientOptions(_clientOptions.WithoutRowSetBuffering,
                                                                             _clientOptions.QueryAbortTimeout, null,
                                                                             _clientOptions.AsyncCallAbortTimeout),
                                                           _authProvider, _metricsEnabled,_hosts);
            }

            this._policies.LoadBalancingPolicy.Initialize(_cluster);

            _keyspace = keyspace ?? clientOptions.DefaultKeyspace;
        }

        internal  void Init()
        {
            var ci = this._policies.LoadBalancingPolicy.NewQueryPlan(null).GetEnumerator();
            if (!ci.MoveNext())
                throw new NoHostAvailableException(new Dictionary<IPAddress, Exception>());


            Connect(null, ci);

            if (ControlConnection != null)
            {
                ControlConnection.Init();
            }
        }

        readonly List<CassandraConnection> _trahscan = new List<CassandraConnection>();

        internal CassandraConnection Connect(Query query, IEnumerator<Host> hostsIter, Dictionary<IPAddress, Exception> innerExceptions = null)
        {
            CheckDisposed();
            lock (_trahscan)
            {
                foreach (var conn in _trahscan)
                {
                    if (conn.IsEmpty())
                    {
                        Debug.WriteLine("Connection trashed");
                        conn.Dispose();
                    }
                }
            }
            lock (_connectionPool)
            {
                while (true)
                {
                    var current = hostsIter.Current;
                    if(current==null)
                        throw new NoHostAvailableException(innerExceptions ?? new Dictionary<IPAddress, Exception>());
                    if (current.IsConsiderablyUp)
                    {
                        var hostDistance = _policies.LoadBalancingPolicy.Distance(current);
                        if (!_connectionPool.ContainsKey(current.Address))
                            _connectionPool.Add(current.Address, new List<CassandraConnection>());

                        var pool = _connectionPool[current.Address];
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
                                            lock (_trahscan)
                                                _trahscan.Add(conn);
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
                                conn = AllocateConnection(current.Address, out outExc);
                                if (conn != null)
                                {
                                    current.BringUpIfDown();
                                    if (ControlConnection != null)
                                        ControlConnection.OwnerHostBringUpIfDown(current.Address);
                                    pool.Add(conn);
                                }
                                else
                                {
                                    if (innerExceptions == null)
                                        innerExceptions = new Dictionary<IPAddress, Exception>();
                                    innerExceptions[current.Address] = outExc;
                                    Debug.WriteLine("new connection attempt failed - goto another host");
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
                        throw new NoHostAvailableException(innerExceptions ?? new Dictionary<IPAddress, Exception>());
                }
            }
        }

        internal void OnAddHost(IPAddress endpoint)
        {
            _hosts.AddIfNotExistsOrBringUpIfDown(endpoint, _policies.ReconnectionPolicy);
        }

        internal void OnDownHost(IPAddress endpoint)
        {
            _hosts.SetDownIfExists(endpoint);
        }

        internal void OnRemovedHost(IPAddress endpoint)
        {
            _hosts.RemoveIfExists(endpoint);
        }

        internal void HostIsDown(IPAddress endpoint)
        {
            lock (_connectionPool)
            {
                _hosts.SetDownIfExists(endpoint);
                if (ControlConnection != null)
                    ControlConnection.OwnerHostIsDown(endpoint);
            }
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
                    var keyspaceId = CqlQueryTools.CqlIdentifier(_keyspace);
                    string retKeyspaceId;
                    try
                    {
                        retKeyspaceId = ProcessSetKeyspace(nconn.Query(GetUseKeyspaceCQL(keyspaceId), ConsistencyLevel.Ignore));
                    }
                    catch (InvalidException)
                    {
                        throw;
                    }
                    catch (QueryValidationException)
                    {
                        return null;
                    }
                    
                    if (CqlQueryTools.CqlIdentifier(retKeyspaceId) != CqlQueryTools.CqlIdentifier(keyspaceId))
                        throw new DriverInternalError("USE query returned " + retKeyspaceId + ". We expected " + keyspaceId + ".");

                    lock(_preparedQueries)
                        foreach (var prepQ in _preparedQueries)
                        {
                            try
                            {
                                byte[] queryid;
                                TableMetadata metadata;
                                ProcessPrepareQuery(nconn.PrepareQuery(prepQ.Key), out metadata, out queryid);
                            }
                            catch (QueryValidationException)
                            {
                                return null;
                            }
                            //TODO: makesure that ids are equal;
                        }
                }
            }
            catch (Exception ex)
            {
                if (nconn != null)
                    nconn.Dispose();

                if (CassandraConnection.IsStreamRelatedException(ex))
                {
                    outExc = ex;
                    return null;
                }
                else
                    throw ex;
            }

            Debug.WriteLine("Allocated new connection");

            return nconn;
        }

        static string GetCreateKeyspaceCQL(string keyspace)
        {
            return string.Format(
  @"CREATE KEYSPACE {0} 
  WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }}"
              , CqlQueryTools.CqlIdentifier(keyspace));
        }

        static string GetUseKeyspaceCQL(string keyspace)
        {
            return string.Format(
  @"USE {0}"
              , CqlQueryTools.CqlIdentifier(keyspace));
        }

        static string GetDropKeyspaceCQL(string keyspace)
        {
            return string.Format(
  @"DROP KEYSPACE {0}"
              , CqlQueryTools.CqlIdentifier(keyspace));
        }

        public void CreateKeyspace(string ksname)
        {
            Query(GetCreateKeyspaceCQL(ksname), ConsistencyLevel.Ignore);
        }

        public void CreateKeyspaceIfNotExists(string ksname)
        {
            try
            {
                CreateKeyspace(ksname);
            }
            catch (AlreadyExistsException)
            {
                //already exists
            }
        }

        public void DeleteKeyspace(string ksname)
        {
            Query(GetDropKeyspaceCQL(ksname), ConsistencyLevel.Ignore);
        }
        public void DeleteKeyspaceIfExists(string ksname)
        {
            try
            {
                DeleteKeyspace(ksname);
            }
            catch (InvalidConfigurationInQueryException)
            {
                //not exists
            }
        }
        
        public void ChangeKeyspace(string keyspace)
        {
            lock (_connectionPool)
            {
                foreach (var kv in _connectionPool)
                {
                    foreach (var conn in kv.Value)
                    {
                        if (conn.IsHealthy)
                        {
                        retry:
                            try
                            {
                                var keyspaceId = CqlQueryTools.CqlIdentifier(keyspace);
                                string retKeyspaceId;
                                try
                                {
                                    retKeyspaceId = ProcessSetKeyspace(conn.Query(GetUseKeyspaceCQL(keyspace), ConsistencyLevel.Ignore));
                                }
                                catch (QueryValidationException)
                                {
                                    throw;
                                }
                                if (CqlQueryTools.CqlIdentifier(retKeyspaceId) != keyspaceId)
                                    throw new DriverInternalError("USE query returned " + retKeyspaceId + ". We expected " + keyspaceId + ".");
                            }
                            catch (Cassandra.CassandraConnection.StreamAllocationException)
                            {
                                goto retry;
                            }
                        }
                    }
                }
                this._keyspace = keyspace;
            }
        }

        readonly Guarded<bool> _alreadyDisposed = new Guarded<bool>(false);

        void CheckDisposed()
        {
            lock (_alreadyDisposed)
                if (_alreadyDisposed.Value)
                    throw new ObjectDisposedException("CassandraSession");
        }

        public void Dispose()
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

        ~Session()
        {
            Dispose();
        }

        #region Execute

        public IAsyncResult BeginExecute(Query query, AsyncCallback callback, object state)
        {
            return query.BeginExecute(this, callback, state);
        }

        public CqlRowSet EndExecute(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<CqlRowSet>;
            return (longActionAc.AsyncSender as Query).EndExecute(this, ar);
        }

        public CqlRowSet Execute(Query query)
        {
            return EndExecute(BeginExecute(query, null, null));
        }

        public IAsyncResult BeginExecute(string cqlQuery, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.Default)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), callback, state);
        }

        public CqlRowSet Execute(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.Default)
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
            TableMetadata metadata;
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
            else if (exc is InvalidException) return RetryDecision.Rethrow();
            else if (exc is UnauthorizedException) return RetryDecision.Rethrow();
            else if (exc is SyntaxError) return RetryDecision.Rethrow();

            else if (exc is ServerErrorException) return null;
            else return null;
        }

        private void ProcessRegisterForEvent(IOutput outp)
        {
            using (outp)
            {
                if (!(outp is OutputVoid))
                {
                    if (outp is OutputError)
                        throw (outp as OutputError).CreateException();
                    else
                        throw new DriverInternalError("Unexpected output kind: " + outp.GetType().Name);
                }
                else
                    return; //ok
            }
        }

        private string ProcessSetKeyspace(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                {
                    throw (outp as OutputError).CreateException();
                }
                else if (outp is OutputSetKeyspace)
                {
                    return (outp as OutputSetKeyspace).Value;
                }
                else
                    throw new DriverInternalError("Unexpected output kind");
            }
        }

        private void ProcessPrepareQuery(IOutput outp, out TableMetadata metadata, out byte[] queryId)
        {
            using (outp)
            {
                if (outp is OutputError)
                {
                    throw (outp as OutputError).CreateException();
                }
                else if (outp is OutputPrepared)
                {
                    queryId = (outp as OutputPrepared).QueryID;
                    metadata = (outp as OutputPrepared).Metadata;
                    return; //ok
                }
                else
                    throw new DriverInternalError("Unexpected output kind");
            }
        }

        private CqlRowSet ProcessRowset(IOutput outp)
        {
            bool ok = false;
            try
            {
                if (outp is OutputError)
                {
                    throw (outp as OutputError).CreateException();
                }
                else if (outp is OutputVoid)
                    return null;
                else if (outp is OutputSchemaChange)
                    return null;
                else if (outp is OutputRows)
                {
                    ok = true;
                    return new CqlRowSet(outp as OutputRows, true);
                }
                else
                    throw new DriverInternalError("Unexpected output kind");
            }
            finally
            {
                if (!ok)
                    outp.Dispose();
            }
        }

        abstract class LongToken
        {
            public CassandraConnection Connection;
            public ConsistencyLevel Consistency;
            public Query Query;
            private IEnumerator<Host> _hostsIter = null;
            public IAsyncResult LongActionAc;
            public readonly Dictionary<IPAddress, Exception> InnerExceptions = new Dictionary<IPAddress, Exception>();
            public int QueryRetries = 0;
            virtual public void Connect(Session owner, bool moveNext)
            {
                if (_hostsIter == null)
                {
                    _hostsIter = owner._policies.LoadBalancingPolicy.NewQueryPlan(Query).GetEnumerator();
                    if (!_hostsIter.MoveNext())
                        throw new NoHostAvailableException(new Dictionary<IPAddress, Exception>());
                }
                else
                {
                    if (moveNext)
                        if (!_hostsIter.MoveNext())
                            throw new NoHostAvailableException(InnerExceptions ?? new Dictionary<IPAddress, Exception>());
                }

                Connection = owner.Connect(Query, _hostsIter, InnerExceptions);
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
                    var decision = GetRetryDecision(token.Query, exc, _policies.RetryPolicy, token.QueryRetries);
                    if (decision == null)
                    {
                        token.InnerExceptions[token.Connection.GetHostAdress()] = exc;
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
                                token.Consistency = decision.RetryConsistencyLevel ?? token.Consistency;
                                token.QueryRetries++;
                                token.InnerExceptions[token.Connection.GetHostAdress()] = exc;
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
                    token.InnerExceptions[token.Connection.GetHostAdress()] = ex;
                    ExecConn(token, true);
                }
                else
                    token.Complete(this, null, ex);
            }
        }

        #region SetKeyspace

        class LongSetKeyspaceToken : LongToken
        {
            public string CqlQuery;
            override public void Begin(Session owner)
            {
                Connection.BeginQuery(CqlQuery, owner.ClbNoQuery, this, owner, Consistency);
            }
            override public void Process(Session owner, IAsyncResult ar, out object value)
            {
                value = owner.ProcessSetKeyspace(Connection.EndQuery(ar, owner));
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                var ar = LongActionAc as AsyncResult<string>;
                if (exc != null)
                    ar.Complete(new ExecutionException("Unable to complete the query.", exc, InnerExceptions));
                else
                {
                    ar.SetResult(value as string);
                    ar.Complete();
                }
            }
        }

        internal IAsyncResult BeginSetKeyspace(string cqlQuery, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.Default, Query query = null)
        {
            var longActionAc = new AsyncResult<string>(callback, state, this, "SessionSetKeyspace", null, _clientOptions.AsyncCallAbortTimeout);
            var token = new LongSetKeyspaceToken() { Consistency = consistency, CqlQuery = cqlQuery, Query = query, LongActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal object EndSetKeyspace(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<string>;
            return AsyncResult<string>.End(ar, this, "SessionSetKeyspace");
        }

        internal object SetKeyspace(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.Default, Query query = null)
        {
            var ar = BeginSetKeyspace(cqlQuery, null, null, consistency, query);
            return EndSetKeyspace(ar);
        }

        #endregion

        #region Query

        class LongQueryToken : LongToken
        {
            public string CqlQuery;
            override public void Begin(Session owner)
            {
                Connection.BeginQuery(CqlQuery, owner.ClbNoQuery, this, owner, Consistency);
            }
            override public void Process(Session owner, IAsyncResult ar, out object value)
            {
                value = owner.ProcessRowset(Connection.EndQuery(ar, owner));
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                CqlRowSet rowset = value as CqlRowSet;
                var ar = LongActionAc as AsyncResult<CqlRowSet>;
                if (exc != null)
                    ar.Complete(exc);
                else
                {
                    ar.SetResult(rowset);
                    ar.Complete();
                }
            }
        }

        internal IAsyncResult BeginQuery(string cqlQuery, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.Default, Query query = null, object sender = null)
        {
            var longActionAc = new AsyncResult<CqlRowSet>(callback, state, this, "SessionQuery", sender,  _clientOptions.AsyncCallAbortTimeout);
            var token = new LongQueryToken() { Consistency = consistency, CqlQuery = cqlQuery, Query = query, LongActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal CqlRowSet EndQuery(IAsyncResult ar)
        {
            return AsyncResult<CqlRowSet>.End(ar, this, "SessionQuery");
        }

        internal CqlRowSet Query(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.Default, Query query = null)
        {
            return EndQuery(BeginQuery(cqlQuery, null, null, consistency, query));
        }

        #endregion

        #region Prepare

        readonly Dictionary<string, KeyValuePair<TableMetadata, byte[]>> _preparedQueries = new Dictionary<string, KeyValuePair<TableMetadata, byte[]>>();


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
                TableMetadata metadata;
                owner.ProcessPrepareQuery(Connection.EndPrepareQuery(ar, owner), out metadata, out id);
                value = new KeyValuePair<TableMetadata, byte[]>(metadata, id);
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                var kv = (KeyValuePair<TableMetadata, byte[]>)value;
                var ar = LongActionAc as AsyncResult<KeyValuePair<TableMetadata, byte[]>>;
                if (exc != null)
                    ar.Complete(exc);
                else
                {
                    ar.SetResult(kv);
                    lock (owner._preparedQueries)
                        owner._preparedQueries[CqlQuery] = kv;
                    ar.Complete();
                }
            }
        }

        internal IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state, object sender = null)
        {
            var longActionAc = new AsyncResult<KeyValuePair<TableMetadata, byte[]>>(callback, state, this, "SessionPrepareQuery", sender,  _clientOptions.AsyncCallAbortTimeout);
            var token = new LongPrepareQueryToken() { Consistency = ConsistencyLevel.Ignore, CqlQuery = cqlQuery, LongActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal byte[] EndPrepareQuery(IAsyncResult ar, out TableMetadata metadata)
        {
            var longActionAc = ar as AsyncResult<KeyValuePair<TableMetadata, byte[]>>;
            var ret = AsyncResult<KeyValuePair<TableMetadata, byte[]>>.End(ar, this, "SessionPrepareQuery");
            metadata = ret.Key;
            return ret.Value;
        }

        internal byte[] PrepareQuery(string cqlQuery, out TableMetadata metadata)
        {
            var ar = BeginPrepareQuery(cqlQuery, null, null, null);
            return EndPrepareQuery(ar, out metadata);
        }


        #endregion

        #region ExecuteQuery

        class LongExecuteQueryToken : LongToken
        {
            public byte[] Id;
            public TableMetadata Metadata;
            public object[] Values;
            override public void Begin(Session owner)
            {
                Connection.BeginExecuteQuery(Id, Metadata, Values, owner.ClbNoQuery, this, owner, Consistency);
            }
            override public void Process(Session owner, IAsyncResult ar, out object value)
            {
                value = owner.ProcessRowset(Connection.EndExecuteQuery(ar, owner));
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                CqlRowSet rowset = value as CqlRowSet;
                var ar = LongActionAc as AsyncResult<CqlRowSet>;
                if (exc != null)
                    ar.Complete(exc);
                else
                {
                    ar.SetResult(rowset);
                    ar.Complete();
                }
            }
        }

        internal IAsyncResult BeginExecuteQuery(byte[] Id, TableMetadata Metadata, object[] values, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.Default, Query query = null, object sender = null)
        {
            AsyncResult<CqlRowSet> longActionAc = new AsyncResult<CqlRowSet>(callback, state, this, "SessionExecuteQuery", sender,  _clientOptions.AsyncCallAbortTimeout);
            var token = new LongExecuteQueryToken() { Consistency = consistency, Id = Id, Metadata = Metadata, Values = values, Query = query, LongActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal CqlRowSet EndExecuteQuery(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<CqlRowSet>;
            return AsyncResult<CqlRowSet>.End(ar, this, "SessionExecuteQuery");
        }

        internal CqlRowSet ExecuteQuery(byte[] Id, TableMetadata Metadata, object[] values, ConsistencyLevel consistency = ConsistencyLevel.Default, Query query = null)
        {
            var ar = BeginExecuteQuery(Id,Metadata,values, null, null, consistency, query);
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
}
