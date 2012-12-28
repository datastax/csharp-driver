using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Cassandra.Native;
using Cassandra;
using System.Net.Sockets;

namespace Cassandra
{
    public interface ISessionInfoProvider
    {
        ICollection<CassandraClusterHost> GetAllHosts();
        ICollection<CassandraClusterHost> GetReplicas(byte[] routingInfo);
    }

    public class Session : IDisposable
    {
        AuthInfoProvider credentialsDelegate;
        public Policies Policies { get; private set; }

        CompressionType compression;
        int abortTimeout;

        class CassandraSessionInfoProvider : ISessionInfoProvider
        {
            Session owner;
            internal CassandraSessionInfoProvider(Session owner)
            {
                this.owner = owner;
            }
            public ICollection<CassandraClusterHost> GetAllHosts()
            {
                return owner.Hosts.All();
            }
            public ICollection<CassandraClusterHost> GetReplicas(byte[] routingInfo)
            {
                var replicas = owner.control.metadata.GetReplicas(routingInfo);
                List<CassandraClusterHost> ret = new List<CassandraClusterHost>();
                foreach (var repl in replicas)
                    ret.Add(owner.Hosts[repl]);
                return ret;
            }
        }


        internal Hosts Hosts;

        Dictionary<IPAddress, List<CassandraConnection>> connectionPool = new Dictionary<IPAddress, List<CassandraConnection>>();

        PoolingOptions poolingOptions = new PoolingOptions();
        string keyspace = string.Empty;

        public string Keyspace { get { return keyspace; } }

#if ERRORINJECTION
        public void SimulateSingleConnectionDown(IPAddress endpoint)
        {
            while (true)
                lock (connectionPool)
                    if (connectionPool.Count > 0)
                    {
                        var conn = connectionPool[endpoint][StaticRandom.Instance.Next(connectionPool[endpoint].Count)];
                        conn.KillSocket();
                        return;
                    }
        }

        public void SimulateAllConnectionsDown()
        {
            lock (connectionPool)
            {
                foreach (var kv in connectionPool)
                    foreach (var conn in kv.Value)
                        conn.KillSocket();
            }
        }
#endif

        bool noBufferingIfPossible;

        ControlConnection control;

        int port;

        internal Session(IEnumerable<IPAddress> clusterEndpoints, int port, string keyspace, CompressionType compression = CompressionType.NoCompression,
            int abortTimeout = Timeout.Infinite, Policies policies = null, AuthInfoProvider credentialsDelegate = null, PoolingOptions poolingOptions = null, bool noBufferingIfPossible = false, Hosts hosts = null)
        {
            this.Policies = policies ?? Policies.DEFAULT_POLICIES;
            if (poolingOptions != null)
                this.poolingOptions = poolingOptions;
            this.noBufferingIfPossible = noBufferingIfPossible;

            Hosts = hosts ?? new Hosts();

            this.port = port;

            foreach (var ep in clusterEndpoints)
                Hosts.AddIfNotExistsOrBringUpIfDown(ep, this.Policies.ReconnectionPolicy);

            this.compression = compression;
            this.abortTimeout = abortTimeout;

            this.credentialsDelegate = credentialsDelegate;
            this.keyspace = keyspace;
            this.Policies.LoadBalancingPolicy.Initialize(new CassandraSessionInfoProvider(this));

            CassandraClusterHost current = null;
            connect(null, ref current);

            if (hosts == null)
            {
                var controlpolicies = new Cassandra.Policies(
                    new RoundRobinPolicy(),
                    new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000),
                    Cassandra.Policies.DEFAULT_RETRY_POLICY);
                control = new ControlConnection(this, clusterEndpoints, port, null, compression, abortTimeout, controlpolicies, credentialsDelegate, poolingOptions, noBufferingIfPossible);
            }
        }

        List<CassandraConnection> trahscan = new List<CassandraConnection>();

        internal CassandraConnection connect(CassandraRoutingKey routingKey, ref CassandraClusterHost current, bool getNext=false)
        {
            checkDisposed();
            lock (trahscan)
            {
                foreach (var conn in trahscan)
                {
                    if (conn.isEmpty())
                    {
                        Debug.WriteLine("Connection trashed");
                        conn.Dispose();
                    }
                }
            }
            lock (connectionPool)
            {
                var hosts = Policies.LoadBalancingPolicy.NewQueryPlan(routingKey);
                var hostsIter = hosts.GetEnumerator();

                if (current != null)
                    while (hostsIter.MoveNext())
                    {
                        if (current == hostsIter.Current)
                        {
                            if (getNext)
                                if (!hostsIter.MoveNext())
                                    throw new CassandraNoHostAvaliableException("No host is avaliable");
                            break;
                        }
                    }

                while (hostsIter.MoveNext())
                {
                    current = hostsIter.Current;
                    if (current.IsConsiderablyUp)
                    {
                        var host_distance = Policies.LoadBalancingPolicy.Distance(current);
                        if (!connectionPool.ContainsKey(current.Address))
                            connectionPool.Add(current.Address, new List<CassandraConnection>());

                        var pool = connectionPool[current.Address];
                        List<CassandraConnection> poolCpy = new List<CassandraConnection>(pool);
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
                                    if (!conn.isBusy(poolingOptions.GetMaxSimultaneousRequestsPerConnectionTreshold(host_distance)))
                                        toReturn = conn;
                                }
                                else
                                {
                                    if (pool.Count > poolingOptions.GetCoreConnectionsPerHost(host_distance))
                                    {
                                        if (conn.isFree(poolingOptions.GetMinSimultaneousRequestsPerConnectionTreshold(host_distance)))
                                        {
                                            lock (trahscan)
                                                trahscan.Add(conn);
                                            pool.Remove(conn);
                                        }
                                    }
                                }
                            }
                        }
                        if (toReturn != null)
                            return toReturn;
                        if (pool.Count < poolingOptions.GetMaxConnectionPerHost(host_distance) - 1)
                        {
                            bool error = false;
                            CassandraConnection conn = null;
                            do
                            {
                                conn = allocateConnection(current.Address);
                                if (conn != null)
                                {
                                    current.BringUpIfDown();
                                    if (control != null)
                                        control.ownerHostBringUpIfDown(current.Address);
                                    pool.Add(conn);
                                }
                                else
                                {
                                    Debug.WriteLine("new connection attempt failed - goto another host");
                                    error = true;
                                    break;
                                }
                            }
                            while (pool.Count < poolingOptions.GetCoreConnectionsPerHost(host_distance));
                            if (!error)
                                return conn;
                        }
                    }
                }
            }
            throw new CassandraNoHostAvaliableException("No host is avaliable");
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

        internal void hostIsDown(IPAddress endpoint)
        {
            lock (connectionPool)
            {
                Hosts.SetDownIfExists(endpoint);
                if (control != null)
                    control.ownerHostIsDown(endpoint);
            }
        }

        CassandraConnection allocateConnection(IPAddress endPoint)
        {
            CassandraConnection nconn = null;

            try
            {
                nconn = new CassandraConnection(this, endPoint, port, credentialsDelegate, this.compression, this.abortTimeout, this.noBufferingIfPossible);

                var options = nconn.ExecuteOptions();

                if (!string.IsNullOrEmpty(keyspace))
                {
                    var keyspaceId = CqlQueryTools.CqlIdentifier(keyspace);
                    string retKeyspaceId;
                    var exc = processSetKeyspace(nconn.Query(GetUseKeyspaceCQL(keyspaceId), ConsistencyLevel.IGNORE), out retKeyspaceId);
                    if (exc != null)
                    {
                        if (exc is CassandraClusterInvalidException)
                            throw exc;
                        else
                            return null;
                    }
                    
                    if (CqlQueryTools.CqlIdentifier(retKeyspaceId) != CqlQueryTools.CqlIdentifier(keyspaceId))
                        throw new CassandraClientProtocolViolationException("USE query returned " + retKeyspaceId + ". We expected " + keyspaceId + ".");

                    lock(preparedQueries)
                        foreach (var prepQ in preparedQueries)
                        {
                            byte[] queryid;
                            Metadata metadata;
                            var exc2 = processPrepareQuery(nconn.PrepareQuery(prepQ.Key), out metadata, out queryid);
                            if (exc2 != null)
                                return null;
                            //TODO: makesure that ids are equal;
                        }
                }
            }
            catch (Exception ex)
            {
                if (nconn != null)
                    nconn.Dispose();

                if (ex is SocketException
                    || ex is CassandraConncectionIOException
                    || ex is IOException
                    || ex is ObjectDisposedException
                    || ex is CassandraConnection.StreamAllocationException)
                {
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
            catch (CassandraClusterAlreadyExistsException)
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
            lock (connectionPool)
            {
                foreach (var kv in connectionPool)
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
                                var exc = processSetKeyspace(conn.Query(GetUseKeyspaceCQL(keyspace), ConsistencyLevel.IGNORE), out retKeyspaceId);
                                if (exc != null)
                                    throw exc;
                                if (retKeyspaceId != keyspaceId)
                                    throw new CassandraClientProtocolViolationException("USE query returned " + retKeyspaceId + ". We expected " + keyspaceId + ".");
                            }
                            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
                            {
                                goto retry;
                            }
                        }
                    }
                }
                this.keyspace = keyspace;
            }
        }

        Guarded<bool> alreadyDisposed = new Guarded<bool>(false);

        void checkDisposed()
        {
            lock (alreadyDisposed)
                if (alreadyDisposed.Value)
                    throw new ObjectDisposedException("CassandraSession");
        }

        public void Dispose()
        {
            lock (alreadyDisposed)
            {
                if (alreadyDisposed.Value)
                    return;
                alreadyDisposed.Value = true;
                lock (connectionPool)
                {
                    foreach (var kv in connectionPool)
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
            return BeginExecute(new SimpleStatement(cqlQuery) { Consistency = consistency }, callback, state);
        }

        public CqlRowSet Execute(string cqlQuery, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT)
        {
            return Execute(new SimpleStatement(cqlQuery) { Consistency = consistency });
        }


        #endregion

        #region Prepare

        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            return BeginPrepareQuery(cqlQuery, callback, state);
        }

        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            Metadata metadata;
            var id = EndPrepareQuery(ar, out metadata);
            return new PreparedStatement(metadata, id);
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            return EndPrepare(BeginPrepare(cqlQuery, null, null));
        }
        
        #endregion

        private CassandraServerException processSetKeyspace(IOutput outp, out string keyspacename)
        {
            using (outp)
            {
                if (outp is OutputError)
                {
                    keyspacename = null;
                    return (outp as OutputError).CreateException();
                }
                else if (outp is OutputSetKeyspace)
                {
                    keyspacename = (outp as OutputSetKeyspace).Value;
                    return null;
                }
                else
                    throw new CassandraClientProtocolViolationException("Unexpected output kind");
            }
        }

        private CassandraServerException processPrepareQuery(IOutput outp, out Metadata metadata, out byte[] queryId)
        {
            using (outp)
            {
                if (outp is OutputError)
                {
                    queryId = null;
                    metadata = null;
                    return (outp as OutputError).CreateException();
                }
                else if (outp is OutputPrepared)
                {
                    queryId = (outp as OutputPrepared).QueryID;
                    metadata = (outp as OutputPrepared).Metadata;
                    return null;
                }
                else
                    throw new CassandraClientProtocolViolationException("Unexpected output kind");
            }
        }


        private CassandraServerException processRowset(IOutput outp, out CqlRowSet rowset)
        {
            rowset = null;
            if (outp is OutputError)
            {
                try
                {
                    return (outp as OutputError).CreateException();
                }
                finally
                {
                    outp.Dispose();
                }
            }
            else if (outp is OutputVoid)
                return null;
            else if (outp is OutputSchemaChange)
                return null;
            else if (outp is OutputRows)
            {
                rowset = new CqlRowSet(outp as OutputRows, true);
                return null;
            }
            else
                throw new CassandraClientProtocolViolationException("Unexpected output kind");
        }

        abstract class LongToken
        {
            public CassandraConnection connection;
            public ConsistencyLevel consistency;
            public CassandraRoutingKey routingKey;
            public CassandraClusterHost current = null;
            public IAsyncResult longActionAc;
            public int queryRetries = 0;
            virtual public void Connect(Session owner, bool moveNext)
            {
                connection = owner.connect(routingKey, ref current, moveNext);
            }
            abstract public void Begin(Session owner);
            abstract public CassandraServerException Process(Session owner, IAsyncResult ar, out object value);
            abstract public void Complete(Session owner, object value, Exception exc = null);
        }

        void ExecConn(LongToken token, bool moveNext)
        {
            token.Connect(this, moveNext);
            try
            {
                token.Begin(this);
            }
            catch (Exception ex)
            {
                if (ex is Cassandra.Native.CassandraConnection.StreamAllocationException
                 || ex is CassandraConncectionIOException
                 || ex is IOException
                 || ex is ObjectDisposedException)
                {
                    ExecConn(token, true);
                }
                else
                {
                    throw;
                }
            }
        }

        void ClbNoQuery(IAsyncResult ar)
        {
                var token = ar.AsyncState as LongToken;
            try
            {
                CassandraServerException exc;
                object value;
                exc = token.Process(this, ar, out value);
                if (exc != null)
                {
                    var decision = exc.GetRetryDecition(Policies.RetryPolicy, token.queryRetries);
                    if (decision == null)
                    {
                        ExecConn(token, true);
                    }
                    else
                    {
                        switch (decision.getType())
                        {
                            case RetryDecision.RetryDecisionType.RETHROW:
                                token.Complete(this, null, exc);
                                return;
                            case RetryDecision.RetryDecisionType.RETRY:
                                token.consistency = decision.getRetryConsistencyLevel() ?? token.consistency;
                                token.queryRetries++;
                                ExecConn(token, false);
                                return;
                            default:
                                break;
                        }
                    }
                }
                token.Complete(this, value);
            }
            catch (Exception ex)
            {
                token.Complete(this, null, ex);
            }
        }

        #region SetKeyspace

        class LongSetKeyspaceToken : LongToken
        {
            public string cqlQuery;
            override public void Begin(Session owner)
            {
                connection.BeginQuery(cqlQuery, owner.ClbNoQuery, this, owner, consistency);
            }
            override public CassandraServerException Process(Session owner, IAsyncResult ar, out object value)
            {
                string keyspace;
                var exc = owner.processSetKeyspace(connection.EndQuery(ar, owner), out keyspace);
                value = keyspace;
                return exc;
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                var ar = longActionAc as AsyncResult<string>;
                if (exc != null)
                    ar.Complete(exc);
                else
                {
                    ar.SetResult(value as string);
                    ar.Complete();
                }
            }
        }

        internal IAsyncResult BeginSetKeyspace(string cqlQuery, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            AsyncResult<string> longActionAc = new AsyncResult<string>(callback, state, this, "SessionSetKeyspace");
            var token = new LongSetKeyspaceToken() { consistency = consistency, cqlQuery = cqlQuery, routingKey = routingKey, longActionAc = longActionAc };

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
            public string cqlQuery;
            override public void Begin(Session owner)
            {
                connection.BeginQuery(cqlQuery, owner.ClbNoQuery, this, owner, consistency);
            }
            override public CassandraServerException Process(Session owner, IAsyncResult ar, out object value)
            {
                CqlRowSet rowset;
                var exc = owner.processRowset(connection.EndQuery(ar, owner), out rowset);
                value = rowset;
                return exc;
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                CqlRowSet rowset = value as CqlRowSet;
                var ar = longActionAc as AsyncResult<CqlRowSet>;
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
            AsyncResult<CqlRowSet> longActionAc = new AsyncResult<CqlRowSet>(callback, state, this, "SessionQuery", sender);
            var token = new LongQueryToken() { consistency = consistency, cqlQuery = cqlQuery, routingKey = routingKey, longActionAc = longActionAc };

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

        Dictionary<string, KeyValuePair<Metadata, byte[]>> preparedQueries = new Dictionary<string, KeyValuePair<Metadata, byte[]>>();


        class LongPrepareQueryToken : LongToken
        {
            public string cqlQuery;
            override public void Begin(Session owner)
            {
                connection.BeginPrepareQuery(cqlQuery, owner.ClbNoQuery, this, owner);
            }
            override public CassandraServerException Process(Session owner, IAsyncResult ar, out object value)
            {
                byte[] id;
                Metadata metadata;
                var exc = owner.processPrepareQuery(connection.EndPrepareQuery(ar, owner), out metadata, out id);
                value = new KeyValuePair<Metadata, byte[]>(metadata, id);
                return exc;
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                KeyValuePair<Metadata, byte[]> kv = (KeyValuePair<Metadata, byte[]>)value;
                var ar = longActionAc as AsyncResult<KeyValuePair<Metadata, byte[]>>;
                if (exc != null)
                    ar.Complete(exc);
                else
                {
                    ar.SetResult(kv);
                    lock (owner.preparedQueries)
                        owner.preparedQueries[cqlQuery] = kv;
                    ar.Complete();
                }
            }
        }

        internal IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state, object sender = null)
        {
            AsyncResult<KeyValuePair<Metadata, byte[]>> longActionAc = new AsyncResult<KeyValuePair<Metadata, byte[]>>(callback, state, this, "SessionPrepareQuery", sender);
            var token = new LongPrepareQueryToken() { consistency = ConsistencyLevel.IGNORE, cqlQuery = cqlQuery, longActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal byte[] EndPrepareQuery(IAsyncResult ar, out Metadata metadata)
        {
            var longActionAc = ar as AsyncResult<KeyValuePair<Metadata, byte[]>>;
            var ret = AsyncResult<KeyValuePair<Metadata, byte[]>>.End(ar, this, "SessionPrepareQuery");
            metadata = ret.Key;
            return ret.Value;
        }

        internal byte[] PrepareQuery(string cqlQuery, out Metadata metadata, CassandraRoutingKey routingKey = null)
        {
            var ar = BeginPrepareQuery(cqlQuery, null, null, routingKey);
            return EndPrepareQuery(ar, out metadata);
        }


        #endregion

        #region ExecuteQuery

        class LongExecuteQueryToken : LongToken
        {
            public byte[] id;
            public Metadata metadata;
            public object[] values;
            override public void Begin(Session owner)
            {
                connection.BeginExecuteQuery(id, metadata, values, owner.ClbNoQuery, this, owner, consistency);
            }
            override public CassandraServerException Process(Session owner, IAsyncResult ar, out object value)
            {
                CqlRowSet rowset;
                var exc = owner.processRowset(connection.EndExecuteQuery(ar, owner), out rowset);
                value = rowset;
                return exc;
            }
            override public void Complete(Session owner, object value, Exception exc = null)
            {
                CqlRowSet rowset = value as CqlRowSet;
                var ar = longActionAc as AsyncResult<CqlRowSet>;
                if (exc != null)
                    ar.Complete(exc);
                else
                {
                    ar.SetResult(rowset);
                    ar.Complete();
                }
            }
        }

        internal IAsyncResult BeginExecuteQuery(byte[] Id, Metadata Metadata, object[] values, AsyncCallback callback, object state, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            AsyncResult<CqlRowSet> longActionAc = new AsyncResult<CqlRowSet>(callback, state, this, "SessionExecuteQuery");
            var token = new LongExecuteQueryToken() { consistency = consistency, id = Id, metadata = Metadata, values = values, routingKey = routingKey, longActionAc = longActionAc };

            ExecConn(token, false);

            return longActionAc;
        }

        internal CqlRowSet EndExecuteQuery(IAsyncResult ar)
        {
            var longActionAc = ar as AsyncResult<CqlRowSet>;
            return AsyncResult<CqlRowSet>.End(ar, this, "SessionExecuteQuery");
        }

        internal CqlRowSet ExecuteQuery(byte[] Id, Metadata Metadata, object[] values, ConsistencyLevel consistency = ConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            var ar = BeginExecuteQuery(Id,Metadata,values, null, null, consistency, routingKey);
            return EndExecuteQuery(ar);
        }

        #endregion

        public Metadata.KeyspaceDesc GetKeyspaceMetadata(string keyspaceName)
        {
            List<Metadata> tables = new List<Metadata>();
            List<string> tablesNames = new List<string>();
            using( var rows = Query(string.Format("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='{0}';", keyspaceName)))
            {
                foreach (var row in rows.GetRows())
                    tablesNames.Add(row.GetValue<string>("columnfamily_name")); 
            }
            
            foreach (var tblName in tablesNames)
                tables.Add(GetTableMetadata(tblName));
                        
            Metadata.StrategyClass strClass = Metadata.StrategyClass.Unknown;
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

            return new Metadata.KeyspaceDesc()
            {
                ksName = keyspaceName,
                tables = tables,
                 strategyClass = strClass,
                  replicationOptions = rplctOptions,
                   durableWrites = drblWrites
            };
    
        }

        public Metadata.StrategyClass GetStrategyClass(string strClass)
        {
            if( strClass != null)
            {                
                strClass = strClass.Replace("org.apache.cassandra.locator.", "");                
                List<Metadata.StrategyClass> strategies = new List<Metadata.StrategyClass>((Metadata.StrategyClass[])Enum.GetValues(typeof(Metadata.StrategyClass)));
                foreach(var stratg in strategies)
                    if(strClass == stratg.ToString())
                        return stratg;
            }

            return Metadata.StrategyClass.Unknown;
        }

        public Metadata GetTableMetadata(string tableName, string keyspaceName = null)
        {
            object[] collectionValuesTypes;
            List<Metadata.ColumnDesc> cols = new List<Metadata.ColumnDesc>();
            using (var rows = Query(string.Format("SELECT * FROM system.schema_columns WHERE columnfamily_name='{0}' AND keyspace_name='{1}';", tableName, keyspaceName ?? keyspace)))
            {
                foreach (var row in rows.GetRows())
                {                    
                    var tp_code = convertToColumnTypeCode(row.GetValue<string>("validator"), out collectionValuesTypes);
                    cols.Add(new Metadata.ColumnDesc()
                    {            
                        column_name = row.GetValue<string>("column_name"),
                        ksname = row.GetValue<string>("keyspace_name"),
                        tablename = row.GetValue<string>("columnfamily_name"),
                        type_code = tp_code,
                        secondary_index_name = row.GetValue<string>("index_name"),
                        secondary_index_type = row.GetValue<string>("index_type"),
                        key_type = row.GetValue<string>("index_name")!= null ? Metadata.KeyType.SECONDARY : Metadata.KeyType.NOT_A_KEY,
                        listInfo = (tp_code == Metadata.ColumnTypeCode.List) ? new Metadata.ListColumnInfo() { value_type_code = (Metadata.ColumnTypeCode)collectionValuesTypes[0] } : null,
                        mapInfo = (tp_code == Metadata.ColumnTypeCode.Map) ? new Metadata.MapColumnInfo() { key_type_code = (Metadata.ColumnTypeCode)collectionValuesTypes[0], value_type_code = (Metadata.ColumnTypeCode)collectionValuesTypes[1]} : null,
                        setInfo = (tp_code == Metadata.ColumnTypeCode.Set) ? new Metadata.SetColumnInfo() { key_type_code = (Metadata.ColumnTypeCode)collectionValuesTypes[0] } : null
                    });
                }
            }

            using (var rows = Query(string.Format("SELECT * FROM system.schema_columnfamilies WHERE columnfamily_name='{0}' AND keyspace_name='{1}';", tableName, keyspace)))
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
                    
                    if (rowKeys.Length> 0 && rowKeys[0] != string.Empty)
                    {
                        Regex rg = new Regex(@"org\.apache\.cassandra\.db\.marshal\.\w+");                        
                        
                        var rowKeysTypes = rg.Matches(row.GetValue<string>("comparator"));                        
                        int i = 0;
                        foreach (var keyName in rowKeys)
                        {
                            var tp_code = convertToColumnTypeCode(rowKeysTypes[i+1].ToString(),out collectionValuesTypes);
                            cols.Add(new Metadata.ColumnDesc()
                            {
                                column_name = keyName.ToString(),
                                ksname = row.GetValue<string>("keyspace_name"),
                                tablename = row.GetValue<string>("columnfamily_name"),
                                type_code = tp_code,
                                key_type = Metadata.KeyType.ROW,
                                listInfo = (tp_code == Metadata.ColumnTypeCode.List) ? new Metadata.ListColumnInfo() { value_type_code = (Metadata.ColumnTypeCode)collectionValuesTypes[0] } : null,
                                mapInfo = (tp_code == Metadata.ColumnTypeCode.Map) ? new Metadata.MapColumnInfo() { key_type_code = (Metadata.ColumnTypeCode)collectionValuesTypes[0], value_type_code = (Metadata.ColumnTypeCode)collectionValuesTypes[1] } : null,
                                setInfo = (tp_code == Metadata.ColumnTypeCode.Set) ? new Metadata.SetColumnInfo() { key_type_code = (Metadata.ColumnTypeCode)collectionValuesTypes[0] } : null

                            });
                            i++;
                        }
                    }
                    cols.Add(new Metadata.ColumnDesc()
                    {
                        column_name = row.GetValue<string>("key_aliases").Replace("[\"", "").Replace("\"]", "").Replace("\"\"","\""),
                        ksname = row.GetValue<string>("keyspace_name"),
                        tablename = row.GetValue<string>("columnfamily_name"),
                        type_code = convertToColumnTypeCode(row.GetValue<string>("key_validator"), out collectionValuesTypes),
                        key_type = Metadata.KeyType.PARTITION
                    });                                        
                }
            }
            return new Metadata() { Columns = cols.ToArray() };
        }


        private Metadata.ColumnTypeCode convertToColumnTypeCode(string type, out object[] collectionValueTp)
        {
            object[] obj;
            collectionValueTp = new object[2];
            if (type.StartsWith("org.apache.cassandra.db.marshal.ListType"))
            {                
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.ListType(","").Replace(")",""), out obj); 
                return Metadata.ColumnTypeCode.List;
            }
            if (type.StartsWith("org.apache.cassandra.db.marshal.SetType"))
            {
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.SetType(", "").Replace(")", ""), out obj);
                return Metadata.ColumnTypeCode.Set;
            }

            if (type.StartsWith("org.apache.cassandra.db.marshal.MapType"))
            {
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.MapType(", "").Replace(")", "").Split(',')[0], out obj);
                collectionValueTp[1] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.MapType(", "").Replace(")", "").Split(',')[1], out obj); 
                return Metadata.ColumnTypeCode.Map;
            }
            
            collectionValueTp = null;
            switch (type)
            {
                case "org.apache.cassandra.db.marshal.UTF8Type":
                    return Metadata.ColumnTypeCode.Text;
                case "org.apache.cassandra.db.marshal.UUIDType":
                    return Metadata.ColumnTypeCode.Uuid;
                case "org.apache.cassandra.db.marshal.Int32Type":
                    return Metadata.ColumnTypeCode.Int;
                case "org.apache.cassandra.db.marshal.BytesType":
                    return Metadata.ColumnTypeCode.Blob;
                case "org.apache.cassandra.db.marshal.FloatType":
                    return Metadata.ColumnTypeCode.Float;
                case "org.apache.cassandra.db.marshal.DoubleType":
                    return Metadata.ColumnTypeCode.Double;
                case "org.apache.cassandra.db.marshal.BooleanType":
                    return Metadata.ColumnTypeCode.Boolean;
                case "org.apache.cassandra.db.marshal.InetAddressType":
                    return Metadata.ColumnTypeCode.Inet;
                case "org.apache.cassandra.db.marshal.DateType":
                    return Metadata.ColumnTypeCode.Timestamp;
                case "org.apache.cassandra.db.marshal.DecimalType":
                    return Metadata.ColumnTypeCode.Decimal;
                case "org.apache.cassandra.db.marshal.LongType":
                    return Metadata.ColumnTypeCode.Bigint;
                case "org.apache.cassandra.db.marshal.IntegerType":
                    return Metadata.ColumnTypeCode.Varint;
                default: throw new InvalidOperationException();
            }
        }
    }
}
