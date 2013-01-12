using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Cassandra;
using System.Net.Sockets;

namespace Cassandra
{
    public interface ISessionInfoProvider
    {
        ICollection<Host> GetAllHosts();
        ICollection<Host> GetReplicas(byte[] routingInfo);
    }

    public class Session : IDisposable
    {
        readonly IAuthInfoProvider _credentialsDelegate;
        public Policies Policies { get; private set; }

        readonly CompressionType _compression;
        readonly int _abortTimeout = Timeout.Infinite;
        readonly int _clientAbortAsyncCommandTimeout = Timeout.Infinite;

        class CassandraSessionInfoProvider : ISessionInfoProvider
        {
            readonly Session _owner;
            internal CassandraSessionInfoProvider(Session owner)
            {
                this._owner = owner;
            }
            public ICollection<Host> GetAllHosts()
            {
                return _owner.Hosts.All();
            }
            public ICollection<Host> GetReplicas(byte[] routingInfo)
            {
                var replicas = _owner._controlConnection.Metadata.GetReplicas(routingInfo);
                var ret = new List<Host>();
                foreach (var repl in replicas)
                    ret.Add(_owner.Hosts[repl]);
                return ret;
            }
        }


        internal Hosts Hosts;

        readonly Dictionary<IPAddress, List<CassandraConnection>> _connectionPool = new Dictionary<IPAddress, List<CassandraConnection>>();

        readonly PoolingOptions _poolingOptions = new PoolingOptions();
        string _keyspace = string.Empty;

        public string Keyspace { get { return _keyspace; } }

#if ERRORINJECTION
        public void SimulateSingleConnectionDown()
        {
            lock (_connectionPool)
                if (_connectionPool.Count > 0)
                {
                    var hostidx = StaticRandom.Instance.Next(_connectionPool.Keys.Count);
                    var endpoint  = new List<IPAddress>(_connectionPool.Keys)[hostidx];
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

        readonly bool _noBufferingIfPossible;

        readonly ControlConnection _controlConnection;

        readonly int _port;

        internal Session(IEnumerable<IPAddress> clusterEndpoints, int port, string keyspace, CompressionType compression = CompressionType.NoCompression,
            int abortTimeout = Timeout.Infinite, Policies policies = null, IAuthInfoProvider credentialsDelegate = null, PoolingOptions poolingOptions = null, bool noBufferingIfPossible = false, Hosts hosts = null)
        {
            this.Policies = policies ?? Policies.DefaultPolicies;
            if (poolingOptions != null)
                this._poolingOptions = poolingOptions;
            this._noBufferingIfPossible = noBufferingIfPossible;

            Hosts = hosts ?? new Hosts();

            this._port = port;

            foreach (var ep in clusterEndpoints)
                Hosts.AddIfNotExistsOrBringUpIfDown(ep, this.Policies.ReconnectionPolicy);

            this._compression = compression;
            this._abortTimeout = abortTimeout;

            this._credentialsDelegate = credentialsDelegate;
            this._keyspace = keyspace;
            this.Policies.LoadBalancingPolicy.Initialize(new CassandraSessionInfoProvider(this));

            var ci =this.Policies.LoadBalancingPolicy.NewQueryPlan(null).GetEnumerator();
            if(!ci.MoveNext())
                throw new NoHostAvailableException(new Dictionary<IPAddress,Exception>());

            Connect(null, ci);

            if (hosts == null)
            {
                var controlpolicies = new Cassandra.Policies(
                    new RoundRobinPolicy(),
                    new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000),
                    Cassandra.Policies.DefaultRetryPolicy);
                _controlConnection = new ControlConnection(this, clusterEndpoints, port, null, compression, abortTimeout, controlpolicies, credentialsDelegate, poolingOptions, noBufferingIfPossible);
            }
        }

        readonly List<CassandraConnection> _trahscan = new List<CassandraConnection>();

        internal CassandraConnection Connect(CassandraRoutingKey routingKey, IEnumerator<Host> hostsIter, Dictionary<IPAddress, Exception> innerExceptions = null)
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
                    if (current.IsConsiderablyUp)
                    {
                        var host_distance = Policies.LoadBalancingPolicy.Distance(current);
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
                                    if (!conn.IsBusy(_poolingOptions.GetMaxSimultaneousRequestsPerConnectionTreshold(host_distance)))
                                        toReturn = conn;
                                }
                                else
                                {
                                    if (pool.Count > _poolingOptions.GetCoreConnectionsPerHost(host_distance))
                                    {
                                        if (conn.IsFree(_poolingOptions.GetMinSimultaneousRequestsPerConnectionTreshold(host_distance)))
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
                        if (pool.Count < _poolingOptions.GetMaxConnectionPerHost(host_distance) - 1)
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
                                    if (_controlConnection != null)
                                        _controlConnection.OwnerHostBringUpIfDown(current.Address);
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
                            while (pool.Count < _poolingOptions.GetCoreConnectionsPerHost(host_distance));
                            if (!error)
                                return conn;
                        }
                    }
                    if (hostsIter.MoveNext())
                        current = hostsIter.Current;
                    else
                        throw new NoHostAvailableException(innerExceptions ?? new Dictionary<IPAddress, Exception>());
                }
            }
        }

        internal void OnAddHost(IPAddress endpoint)
        {
            Hosts.AddIfNotExistsOrBringUpIfDown(endpoint, Policies.ReconnectionPolicy);
        }

        internal void OnDownHost(IPAddress endpoint)
        {
            Hosts.SetDownIfExists(endpoint);
        }

        internal void OnRemovedHost(IPAddress endpoint)
        {
            Hosts.RemoveIfExists(endpoint);
        }

        internal void HostIsDown(IPAddress endpoint)
        {
            lock (_connectionPool)
            {
                Hosts.SetDownIfExists(endpoint);
                if (_controlConnection != null)
                    _controlConnection.OwnerHostIsDown(endpoint);
            }
        }

        CassandraConnection AllocateConnection(IPAddress endPoint, out Exception outExc)
        {
            CassandraConnection nconn = null;
            outExc = null;

            try
            {
                nconn = new CassandraConnection(this, endPoint, _port, _credentialsDelegate, this._compression, this._abortTimeout, this._noBufferingIfPossible);

                var options = nconn.ExecuteOptions();

                if (!string.IsNullOrEmpty(_keyspace))
                {
                    var keyspaceId = CqlQueryTools.CqlIdentifier(_keyspace);
                    string retKeyspaceId;
                    try
                    {
                        retKeyspaceId = ProcessSetKeyspace(nconn.Query(GetUseKeyspaceCQL(keyspaceId), ConsistencyLevel.IGNORE));
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
            Query(GetCreateKeyspaceCQL(ksname), ConsistencyLevel.IGNORE);
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
            Query(GetDropKeyspaceCQL(ksname), ConsistencyLevel.IGNORE);
        }
        public void DeleteKeyspaceIfExists(string ksname)
        {
            try
            {
                DeleteKeyspace(ksname);
            }
            catch (CassandraClusterConfigErrorException)
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
                                    retKeyspaceId = ProcessSetKeyspace(conn.Query(GetUseKeyspaceCQL(keyspace), ConsistencyLevel.IGNORE));
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

        public IAsyncResult BeginExecute(string cqlQuery, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), callback, state);
        }

        public CqlRowSet Execute(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT)
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

        static RetryDecision GetRetryDecision(QueryValidationException exc, RetryPolicy policy, int queryRetries)
        {
            if (exc is OverloadedException) return RetryDecision.Retry(null);
            else if (exc is IsBootstrappingException) return RetryDecision.Retry(null);
            else if (exc is TruncateException) return RetryDecision.Retry(null);

            else if (exc is ReadTimeoutException)
            {
                var e = exc as ReadTimeoutException;
                return policy.OnReadTimeout(e.ConsistencyLevel, e.BlockFor, e.Received, e.IsDataPresent, queryRetries);
            }
            else if (exc is WriteTimeoutException)
            {
                var e = exc as WriteTimeoutException;
                return policy.OnWriteTimeout(e.ConsistencyLevel, e.WriteType, e.BlockFor, e.Received, queryRetries);
            }
            else if (exc is UnavailableException)
            {
                var e = exc as UnavailableException;
                return policy.OnUnavailable(e.ConsistencyLevel, e.Required, e.Alive, queryRetries);
            }

            else if (exc is AlreadyExistsException) return RetryDecision.Rethrow();
            else if (exc is CassandraClusterConfigErrorException) return RetryDecision.Rethrow();
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
            public CassandraRoutingKey RoutingKey;
            private IEnumerator<Host> _hostsIter = null;
            public IAsyncResult LongActionAc;
            public readonly Dictionary<IPAddress, Exception> InnerExceptions = new Dictionary<IPAddress, Exception>();
            public int QueryRetries = 0;
            virtual public void Connect(Session owner, bool moveNext)
            {
                if (_hostsIter == null)
                {
                    _hostsIter = owner.Policies.LoadBalancingPolicy.NewQueryPlan(RoutingKey).GetEnumerator();
                    if (!_hostsIter.MoveNext())
                        throw new NoHostAvailableException(new Dictionary<IPAddress, Exception>());
                }
                else
                {
                    if (moveNext)
                        if (!_hostsIter.MoveNext())
                            throw new NoHostAvailableException(InnerExceptions ?? new Dictionary<IPAddress, Exception>());
                }

                Connection = owner.Connect(RoutingKey, _hostsIter, InnerExceptions);
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
                    var decision = GetRetryDecision(exc, Policies.RetryPolicy, token.QueryRetries);
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

        internal IAsyncResult BeginSetKeyspace(string cqlQuery, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            var longActionAc = new AsyncResult<string>(callback, state, this, "SessionSetKeyspace", null, _clientAbortAsyncCommandTimeout);
            var token = new LongSetKeyspaceToken() { Consistency = consistency, CqlQuery = cqlQuery, RoutingKey = routingKey, LongActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal object EndSetKeyspace(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<string>;
            return AsyncResult<string>.End(ar, this, "SessionSetKeyspace");
        }

        internal object SetKeyspace(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            var ar = BeginSetKeyspace(cqlQuery, null, null, consistency, routingKey);
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

        internal IAsyncResult BeginQuery(string cqlQuery, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null, object sender = null)
        {
            var longActionAc = new AsyncResult<CqlRowSet>(callback, state, this, "SessionQuery", sender, _clientAbortAsyncCommandTimeout);
            var token = new LongQueryToken() { Consistency = consistency, CqlQuery = cqlQuery, RoutingKey = routingKey, LongActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal CqlRowSet EndQuery(IAsyncResult ar)
        {
            return AsyncResult<CqlRowSet>.End(ar, this, "SessionQuery");
        }

        internal CqlRowSet Query(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            return EndQuery(BeginQuery(cqlQuery, null, null, consistency, routingKey));
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
            var longActionAc = new AsyncResult<KeyValuePair<TableMetadata, byte[]>>(callback, state, this, "SessionPrepareQuery", sender, _clientAbortAsyncCommandTimeout);
            var token = new LongPrepareQueryToken() { Consistency = ConsistencyLevel.IGNORE, CqlQuery = cqlQuery, LongActionAc = longActionAc };

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

        internal byte[] PrepareQuery(string cqlQuery, out TableMetadata metadata, CassandraRoutingKey routingKey = null)
        {
            var ar = BeginPrepareQuery(cqlQuery, null, null, routingKey);
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

        internal IAsyncResult BeginExecuteQuery(byte[] Id, TableMetadata Metadata, object[] values, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null, object sender = null)
        {
            AsyncResult<CqlRowSet> longActionAc = new AsyncResult<CqlRowSet>(callback, state, this, "SessionExecuteQuery", sender, _clientAbortAsyncCommandTimeout);
            var token = new LongExecuteQueryToken() { Consistency = consistency, Id = Id, Metadata = Metadata, Values = values, RoutingKey = routingKey, LongActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal CqlRowSet EndExecuteQuery(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<CqlRowSet>;
            return AsyncResult<CqlRowSet>.End(ar, this, "SessionExecuteQuery");
        }

        internal CqlRowSet ExecuteQuery(byte[] Id, TableMetadata Metadata, object[] values, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            var ar = BeginExecuteQuery(Id,Metadata,values, null, null, consistency, routingKey);
            return EndExecuteQuery(ar);
        }

        #endregion

        public KeyspaceMetadata GetKeyspaceMetadata(string keyspaceName)
        {
            List<TableMetadata> tables = new List<TableMetadata>();
            List<string> tablesNames = new List<string>();
            using( var rows = Query(string.Format("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='{0}';", keyspaceName)))
            {
                foreach (var row in rows.GetRows())
                    tablesNames.Add(row.GetValue<string>("columnfamily_name")); 
            }
            
            foreach (var tblName in tablesNames)
                tables.Add(GetTableMetadata(tblName));
                        
            StrategyClass strClass = StrategyClass.Unknown;
            bool? drblWrites = null;
            SortedDictionary<string, int?> rplctOptions = new SortedDictionary<string, int?>();

            using (var rows = Query(string.Format("SELECT * FROM system.schema_keyspaces WHERE keyspace_name='{0}';", keyspaceName)))
            {                
                foreach (var row in rows.GetRows())
                {
                    strClass = GetStrategyClass(row.GetValue<string>("strategy_class"));
                    drblWrites = row.GetValue<bool>("durable_writes");
                    rplctOptions = Utils.ConvertStringToMap(row.GetValue<string>("strategy_options"));                    
                }
            }

            return new KeyspaceMetadata()
            {
                Keyspace = keyspaceName,
                Tables = tables,
                 StrategyClass = strClass,
                  ReplicationOptions = rplctOptions,
                   DurableWrites = drblWrites
            };
    
        }

        public StrategyClass GetStrategyClass(string strClass)
        {
            if( strClass != null)
            {                
                strClass = strClass.Replace("org.apache.cassandra.locator.", "");                
                List<StrategyClass> strategies = new List<StrategyClass>((StrategyClass[])Enum.GetValues(typeof(StrategyClass)));
                foreach(var stratg in strategies)
                    if(strClass == stratg.ToString())
                        return stratg;
            }

            return StrategyClass.Unknown;
        }

        public TableMetadata GetTableMetadata(string tableName, string keyspaceName = null)
        {
            object[] collectionValuesTypes;
            List<TableMetadata.ColumnDesc> cols = new List<TableMetadata.ColumnDesc>();
            using (var rows = Query(string.Format("SELECT * FROM system.schema_columns WHERE columnfamily_name='{0}' AND keyspace_name='{1}';", tableName, keyspaceName ?? _keyspace)))
            {
                foreach (var row in rows.GetRows())
                {
                    var tp_code = convertToColumnTypeCode(row.GetValue<string>("validator"), out collectionValuesTypes);
                    var dsc = new TableMetadata.ColumnDesc()
                    {
                        ColumnName = row.GetValue<string>("column_name"),
                        Keyspace = row.GetValue<string>("keyspace_name"),
                        Table = row.GetValue<string>("columnfamily_name"),
                        TypeCode = tp_code,
                        SecondaryIndexName = row.GetValue<string>("index_name"),
                        SecondaryIndexType = row.GetValue<string>("index_type"),
                        KeyType = row.GetValue<string>("index_name") != null ? TableMetadata.KeyType.Secondary : TableMetadata.KeyType.NotAKey,
                    };

                    if(tp_code == TableMetadata.ColumnTypeCode.List)
                        dsc.TypeInfo = new TableMetadata.ListColumnInfo() { 
                            ValueTypeCode = (TableMetadata.ColumnTypeCode)collectionValuesTypes[0] 
                        };
                    else if(tp_code == TableMetadata.ColumnTypeCode.Map)
                        dsc.TypeInfo = new TableMetadata.MapColumnInfo() { 
                            KeyTypeCode = (TableMetadata.ColumnTypeCode)collectionValuesTypes[0], 
                            ValueTypeCode = (TableMetadata.ColumnTypeCode)collectionValuesTypes[1] 
                        };
                    else if(tp_code == TableMetadata.ColumnTypeCode.Set)
                        dsc.TypeInfo = new TableMetadata.SetColumnInfo() { 
                            KeyTypeCode = (TableMetadata.ColumnTypeCode)collectionValuesTypes[0] 
                        } ;

                    cols.Add(dsc);
                }
            }

            using (var rows = Query(string.Format("SELECT * FROM system.schema_columnfamilies WHERE columnfamily_name='{0}' AND keyspace_name='{1}';", tableName, _keyspace)))
            {
                foreach (var row in rows.GetRows())
                {
                    var colNames = row.GetValue<string>("column_aliases");
                    var rowKeys = colNames.Substring(1,colNames.Length-2).Split(',');
                    for(int i=0;i<rowKeys.Length;i++)
                    {
                        if(rowKeys[i].StartsWith("\""))
                        {
                            rowKeys[i]=rowKeys[i].Substring(1,rowKeys[i].Length-2).Replace("\"\"","\"");
                        }
                    }

                    if (rowKeys.Length > 0 && rowKeys[0] != string.Empty)
                    {
                        var rg = new Regex(@"org\.apache\.cassandra\.db\.marshal\.\w+"); 

                        var rowKeysTypes = rg.Matches(row.GetValue<string>("comparator"));
                        int i = 0;
                        foreach (var keyName in rowKeys)
                        {
                            var tp_code = convertToColumnTypeCode(rowKeysTypes[i + 1].ToString(), out collectionValuesTypes);
                            var dsc = new TableMetadata.ColumnDesc()
                            {
                                ColumnName = keyName.ToString(),
                                Keyspace = row.GetValue<string>("keyspace_name"),
                                Table = row.GetValue<string>("columnfamily_name"),
                                TypeCode = tp_code,
                                KeyType = TableMetadata.KeyType.Row,
                            };
                            if (tp_code == TableMetadata.ColumnTypeCode.List)
                                dsc.TypeInfo = new TableMetadata.ListColumnInfo()
                                {
                                    ValueTypeCode = (TableMetadata.ColumnTypeCode)collectionValuesTypes[0]
                                };
                            else if (tp_code == TableMetadata.ColumnTypeCode.Map)
                                dsc.TypeInfo = new TableMetadata.MapColumnInfo()
                                {
                                    KeyTypeCode = (TableMetadata.ColumnTypeCode)collectionValuesTypes[0],
                                    ValueTypeCode = (TableMetadata.ColumnTypeCode)collectionValuesTypes[1]
                                };
                            else if (tp_code == TableMetadata.ColumnTypeCode.Set)
                                dsc.TypeInfo = new TableMetadata.SetColumnInfo()
                                {
                                    KeyTypeCode = (TableMetadata.ColumnTypeCode)collectionValuesTypes[0]
                                };
                            cols.Add(dsc);
                            i++;
                        }
                    }
                    cols.Add(new TableMetadata.ColumnDesc()
                    {
                        ColumnName = row.GetValue<string>("key_aliases").Replace("[\"", "").Replace("\"]", "").Replace("\"\"","\""),
                        Keyspace = row.GetValue<string>("keyspace_name"),
                        Table = row.GetValue<string>("columnfamily_name"),
                        TypeCode = convertToColumnTypeCode(row.GetValue<string>("key_validator"), out collectionValuesTypes),
                        KeyType = TableMetadata.KeyType.Partition
                    });                                        
                }
            }
            return new TableMetadata() { Columns = cols.ToArray() };
        }


        private TableMetadata.ColumnTypeCode convertToColumnTypeCode(string type, out object[] collectionValueTp)
        {
            object[] obj;
            collectionValueTp = new object[2];
            if (type.StartsWith("org.apache.cassandra.db.marshal.ListType"))
            {                
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.ListType(","").Replace(")",""), out obj); 
                return TableMetadata.ColumnTypeCode.List;
            }
            if (type.StartsWith("org.apache.cassandra.db.marshal.SetType"))
            {
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.SetType(", "").Replace(")", ""), out obj);
                return TableMetadata.ColumnTypeCode.Set;
            }

            if (type.StartsWith("org.apache.cassandra.db.marshal.MapType"))
            {
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.MapType(", "").Replace(")", "").Split(',')[0], out obj);
                collectionValueTp[1] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.MapType(", "").Replace(")", "").Split(',')[1], out obj); 
                return TableMetadata.ColumnTypeCode.Map;
            }
            
            collectionValueTp = null;
            switch (type)
            {
                case "org.apache.cassandra.db.marshal.UTF8Type":
                    return TableMetadata.ColumnTypeCode.Text;
                case "org.apache.cassandra.db.marshal.UUIDType":
                    return TableMetadata.ColumnTypeCode.Uuid;
                case "org.apache.cassandra.db.marshal.Int32Type":
                    return TableMetadata.ColumnTypeCode.Int;
                case "org.apache.cassandra.db.marshal.BytesType":
                    return TableMetadata.ColumnTypeCode.Blob;
                case "org.apache.cassandra.db.marshal.FloatType":
                    return TableMetadata.ColumnTypeCode.Float;
                case "org.apache.cassandra.db.marshal.DoubleType":
                    return TableMetadata.ColumnTypeCode.Double;
                case "org.apache.cassandra.db.marshal.BooleanType":
                    return TableMetadata.ColumnTypeCode.Boolean;
                case "org.apache.cassandra.db.marshal.InetAddressType":
                    return TableMetadata.ColumnTypeCode.Inet;
                case "org.apache.cassandra.db.marshal.DateType":
                    return TableMetadata.ColumnTypeCode.Timestamp;
#if NET_40_OR_GREATER
                case "org.apache.cassandra.db.marshal.DecimalType":
                    return TableMetadata.ColumnTypeCode.Decimal;
#endif
                case "org.apache.cassandra.db.marshal.LongType":
                    return TableMetadata.ColumnTypeCode.Bigint;
#if NET_40_OR_GREATER
                case "org.apache.cassandra.db.marshal.IntegerType":
                    return TableMetadata.ColumnTypeCode.Varint;
#endif
                default: throw new InvalidOperationException();
            }
        }
    }
}
