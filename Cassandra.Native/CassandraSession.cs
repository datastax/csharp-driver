using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Cassandra.Native
{
    public class CassandraSession : IDisposable
    {
        AuthInfoProvider credentialsDelegate;
        Policies.Policies policies;

        CassandraCompressionType compression;
        int abortTimeout;

        Dictionary<IPEndPoint, CassandraClusterHost> hosts = new Dictionary<IPEndPoint, CassandraClusterHost>();
        Dictionary<IPEndPoint, List<WeakReference<CassandraConnection>>> connectionPool = new Dictionary<IPEndPoint, List<WeakReference<CassandraConnection>>>();

        int maxConnectionsInPool = int.MaxValue;
        string keyspace = string.Empty;

        public string Keyspace { get { return keyspace; } }

#if ERRORINJECTION
        public void SimulateSingleConnectionDown(IPEndPoint endpoint)
        {
            while (true)
                lock (connectionPool)
                    if (connectionPool.Count > 0)
                    {
                        var conn = connectionPool[endpoint][StaticRandom.Instance.Next(connectionPool[endpoint].Count)];
                        if (conn.IsAlive)
                        {
                            conn.Value.KillSocket();
                            return;
                        }
                    }
        }

        public void SimulateAllConnectionsDown()
        {
            lock (connectionPool)
            {
                foreach (var kv in connectionPool)
                    foreach (var conn in kv.Value)
                    {
                        if (conn.IsAlive)
                            conn.Value.KillSocket();
                    }
            }
        }
#endif

        CassandraConnection eventRaisingConnection = null;
        bool noBufferingIfPossible;

        public CassandraSession(IEnumerable<IPEndPoint> clusterEndpoints, string keyspace, CassandraCompressionType compression = CassandraCompressionType.NoCompression,
            int abortTimeout = Timeout.Infinite, Policies.Policies policies = null, AuthInfoProvider credentialsDelegate = null, int maxConnectionsInPool = int.MaxValue, bool noBufferingIfPossible=false)
        {
            this.policies = policies ?? Policies.Policies.DEFAULT_POLICIES;
            this.maxConnectionsInPool = maxConnectionsInPool;
            this.noBufferingIfPossible = noBufferingIfPossible;

            foreach (var ep in clusterEndpoints)
                if (!hosts.ContainsKey(ep))
                    hosts.Add(ep, new CassandraClusterHost(ep, this.policies.getReconnectionPolicy().newSchedule()));

            this.compression = compression;
            this.abortTimeout = abortTimeout;

            this.credentialsDelegate = credentialsDelegate;
            this.keyspace = keyspace;
            this.policies.getLoadBalancingPolicy().init(hosts.Values);
            setupEventListeners(connect(null));
        }

        private void setupEventListeners(CassandraConnection nconn)
        {
            Exception theExc = null;

            nconn.CassandraEvent += new CassandraEventHandler(conn_CassandraEvent);
            using (var ret = nconn.RegisterForCassandraEvent(
                CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange))
            {
                if (!(ret is OutputVoid))
                {
                    if (ret is OutputError)
                        theExc = new Exception("CQL Error [" + (ret as OutputError).CassandraErrorType.ToString() + "] " + (ret as OutputError).Message);
                    else
                        theExc = new CassandraClientProtocolViolationException("Expected Error on Output");
                }
            }

            if (theExc != null)
                throw new CassandraConnectionException("Register event", theExc);

            eventRaisingConnection = nconn;
        }

        private CassandraConnection connect(CassandraRoutingKey routingKey)
        {
            checkDisposed();
            lock (connectionPool)
            {
            BIGRETRY:
                var hosts = policies.getLoadBalancingPolicy().newQueryPlan(routingKey);
                foreach (var host in hosts)
                {
                    if (host.isUp)
                    {
                        if (!connectionPool.ContainsKey(host.getAddress()))
                            connectionPool.Add(host.getAddress(), new List<WeakReference<CassandraConnection>>());
                        var pool = connectionPool[host.getAddress()];
                    RETRY:
                        foreach (var conn in pool)
                        {
                            if (!conn.IsAlive)
                            {
                                pool.Remove(conn);
                                goto RETRY;
                            }
                            else
                            {
                                if (!conn.Value.IsHealthy)
                                {
                                    var recoveryEvents = (eventRaisingConnection == conn.Value);
                                    conn.Value.Dispose();
                                    pool.Remove(conn);
                                    Monitor.Exit(connectionPool);
                                    try
                                    {
                                        if (recoveryEvents)
                                            setupEventListeners(connect(null));
                                    }
                                    finally
                                    {
                                        Monitor.Enter(connectionPool);
                                    }
                                    goto BIGRETRY;
                                }
                                else
                                {
                                    if (!conn.Value.isBusy())
                                        return conn.Value;
                                }
                            }
                        }
                        if (pool.Count < maxConnectionsInPool - 1)
                        {
                            var conn = allocateConnection(host.getAddress());
                            if (conn != null)
                            {
                                pool.Add(new WeakReference<CassandraConnection>(conn));
                                goto RETRY;
                            }
                        }
                    }
                }
            }
            throw new CassandraConnectionException("Cannot Allocate Connection");
        }


        internal void hostIsDown(IPEndPoint endpoint)
        {
            lock (connectionPool)
            {
                hosts[endpoint].setDown();
            }
        }
        
        CassandraConnection allocateConnection(IPEndPoint endPoint)
        {
            CassandraConnection nconn = null;

            try
            {
                nconn = new CassandraConnection(this, endPoint, credentialsDelegate, this.compression, this.abortTimeout, this.noBufferingIfPossible);

                var options = nconn.ExecuteOptions();

                if (!string.IsNullOrEmpty(keyspace))
                {
                    var keyspaceId = CqlQueryTools.CqlIdentifier(keyspace);
                    var retKeyspaceId = processScallar(nconn.Query(GetUseKeyspaceCQL(keyspaceId), CqlConsistencyLevel.IGNORE)).ToString();
                    if (CqlQueryTools.CqlIdentifier(retKeyspaceId) != CqlQueryTools.CqlIdentifier(keyspaceId))
                        throw new CassandraClientProtocolViolationException("USE query returned " + retKeyspaceId + ". We expected " + keyspaceId + ".");
                }

            }
            catch (System.Net.Sockets.SocketException ex)
            {
                Debug.WriteLine(ex.Message, "CassandraSession.Connect");
                if (nconn != null)
                    nconn.Dispose();
                return null;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message, "CassandraSession.Connect");
                if (nconn != null)
                    nconn.Dispose();
                throw new CassandraConnectionException("Cannot connect", ex);
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
            NonQuery(GetCreateKeyspaceCQL(ksname), CqlConsistencyLevel.IGNORE);
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
            NonQuery(GetDropKeyspaceCQL(ksname), CqlConsistencyLevel.IGNORE);
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
                        if (conn.IsAlive && conn.Value.IsHealthy)
                        {
                        retry:
                            try
                            {
                                var keyspaceId = CqlQueryTools.CqlIdentifier(keyspace);
                                var retKeyspaceId = processScallar(conn.Value.Query(GetUseKeyspaceCQL(keyspace), CqlConsistencyLevel.IGNORE)).ToString();
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

        void conn_CassandraEvent(object sender, CassandraEventArgs e)
        {
            if (e.CassandraEventType == CassandraEventType.StatusChange || e.CassandraEventType == CassandraEventType.TopologyChange)
            {
                if (e.Message == "UP" || e.Message == "NEW_NODE")
                {
                    lock (connectionPool)
                    {
                        if (!hosts.ContainsKey(e.IPEndPoint))
                            hosts.Add(e.IPEndPoint, new CassandraClusterHost(e.IPEndPoint, policies.getReconnectionPolicy().newSchedule()));
                        else
                            hosts[e.IPEndPoint].bringUp();
                    }
                    return;
                }
                else if (e.Message == "REMOVED_NODE")
                {
                    lock (connectionPool)
                        if (hosts.ContainsKey(e.IPEndPoint))
                            hosts.Remove(e.IPEndPoint);
                    return;
                }
                else if (e.Message == "DOWN")
                {
                    lock (connectionPool)
                        if (hosts.ContainsKey(e.IPEndPoint))
                            hosts[e.IPEndPoint].setDown();
                    return;
                }
            }

            if (e.CassandraEventType == CassandraEventType.SchemaChange)
            {
                if (e.Message.StartsWith("CREATED") || e.Message.StartsWith("UPDATED") || e.Message.StartsWith("DROPPED"))
                {
                }
                return;
            }
            throw new CassandraClientProtocolViolationException("Unknown Event");
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
                    foreach(var kv in connectionPool)
                        foreach (var conn in kv.Value)
                        {
                            if (conn.IsAlive)
                                conn.Value.Dispose();
                        }
                }
            }
        }

        ~CassandraSession()
        {
            Dispose();
        }

        class ConnectionWrapper
        {
            public CassandraConnection connection;
        }

        private void processNonQuery(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                    throw (outp as OutputError).CreateException();
                else if (outp is OutputVoid)
                    return;
                else if (outp is OutputSchemaChange)
                    return;
                else
                    throw new CassandraClientProtocolViolationException("Unexpected output kind");
            }
        }

        private object processScallar(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                    throw (outp as OutputError).CreateException();
                else if (outp is OutputSetKeyspace)
                    return (outp as OutputSetKeyspace).Value;
                else
                    throw new CassandraClientProtocolViolationException("Unexpected output kind");
            }
        }

        private byte[] processEndPrepare(IOutput outp, out Metadata metadata)
        {
            using (outp)
            {
                if (outp is OutputError)
                    throw (outp as OutputError).CreateException();
                else if (outp is OutputPrepared)
                {
                    metadata = (outp as OutputPrepared).Metadata;
                    return (outp as OutputPrepared).QueryID;
                }
                else
                    throw new CassandraClientProtocolViolationException("Unexpected output kind");
            }
        }
        

        private CqlRowSet processRowset(IOutput outp)
        {
            if (outp is OutputError)
            {
                try
                {
                    throw (outp as OutputError).CreateException();
                }
                finally
                {
                    outp.Dispose();
                }
            }
            else if (outp is OutputRows)
            {
                return new CqlRowSet(outp as OutputRows, true);
            }
            else
                throw new CassandraClientProtocolViolationException("Unexpected output kind");
        }

        public IAsyncResult BeginNonQuery(string cqlQuery, AsyncCallback callback, object state, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT,CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var c = new ConnectionWrapper() { connection = connect(routingKey) };
                return c.connection.BeginQuery(cqlQuery, callback, state, c, consistency);
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public void EndNonQuery(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((AsyncResult<IOutput>)result).AsyncOwner;
            processNonQuery(c.connection.EndQuery(result, c));
        }

        public void NonQuery(string cqlQuery, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
             {
                var connection = connect(routingKey);
                processNonQuery(connection.Query(cqlQuery, consistency));
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public void NonQueryWithRetries(string cqlQuery, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            int retryNo = 0;
        retry:
            try
            {
                var connection = connect(routingKey);
                processNonQuery(connection.Query(cqlQuery, consistency));
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
            catch (CassandraException)
            {
                if (retryNo >= 50000)
                    throw;
                retryNo++;
                goto retry;
            }
        }

        public IAsyncResult BeginScalar(string cqlQuery, AsyncCallback callback, object state, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var c = new ConnectionWrapper() { connection = connect(routingKey) };
                return c.connection.BeginQuery(cqlQuery, callback, state, c, consistency);
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public object EndScalar(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((AsyncResult<IOutput>)result).AsyncOwner;
            return processScallar(c.connection.EndQuery(result, c));
        }

        public object Scalar(string cqlQuery, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var connection = connect(routingKey);
                return processScallar(connection.Query(cqlQuery, consistency));
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public IAsyncResult BeginQuery(string cqlQuery, AsyncCallback callback, object state, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var c = new ConnectionWrapper() { connection = connect(routingKey) };
                return c.connection.BeginQuery(cqlQuery, callback, state, c, consistency);
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public CqlRowSet EndQuery(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((AsyncResult<IOutput>)result).AsyncOwner;
            return processRowset(c.connection.EndQuery(result, c));
        }

        public CqlRowSet Query(string cqlQuery, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var connection = connect(routingKey);
                return processRowset(connection.Query(cqlQuery, consistency));
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public CqlRowSet QueryWithRerties(string cqlQuery, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
            int retryNo = 0;
        retry:
            try
            {
                var connection = connect(routingKey);
                return processRowset(connection.Query(cqlQuery, consistency));
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
            catch (CassandraException)
            {
                if (retryNo >= 50000)
                    throw;
                retryNo++;
                goto retry;
            }
        }

        public IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var c = new ConnectionWrapper() { connection = connect(routingKey) };
                return c.connection.BeginPrepareQuery(cqlQuery, callback, state, c);
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public byte[] EndPrepareQuery(IAsyncResult result, out Metadata metadata)
        {
            var c = (ConnectionWrapper)((AsyncResult<IOutput>)result).AsyncOwner;
            return processEndPrepare(c.connection.EndPrepareQuery(result, c), out metadata);
        }

        public byte[] PrepareQuery(string cqlQuery, out Metadata metadata, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var connection = connect(routingKey);
                return processEndPrepare(connection.PrepareQuery(cqlQuery), out metadata);
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public IAsyncResult BeginExecuteQuery(byte[] Id, Metadata Metadata, object[] values, AsyncCallback callback, object state, bool delayedRelease, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var c = new ConnectionWrapper() { connection = connect(routingKey) };
                return c.connection.BeginExecuteQuery(Id, Metadata, values, callback, state, c, consistency);
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

        public CqlRowSet EndExecuteQuery(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((AsyncResult<IOutput>)result).AsyncOwner;
            return processRowset(c.connection.EndExecuteQuery(result, c));
        }

        public void ExecuteQuery(byte[] Id, Metadata Metadata, object[] values, CqlConsistencyLevel consistency = CqlConsistencyLevel.DEFAULT, CassandraRoutingKey routingKey = null)
        {
        retry:
            try
            {
                var connection = connect(routingKey);
                connection.ExecuteQuery(Id, Metadata, values, consistency);                                
            }
            catch (Cassandra.Native.CassandraConnection.StreamAllocationException)
            {
                goto retry;
            }
        }

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
                    
                    if (rowKeys.Length> 0 && rowKeys[0] != String.Empty)
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
