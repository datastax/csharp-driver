using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Net;

namespace Cassandra
{
    internal class ControlConnection :IDisposable
    {
        private readonly Logger _logger = new Logger(typeof(ControlConnection));
        private readonly Session _session;
        private readonly Cluster _cluster;

        private static IPAddress bindAllAddress = new IPAddress(new byte[4]);

        internal ControlConnection(Cluster cluster, 
                                   IEnumerable<IPAddress> clusterEndpoints,
                                   Policies policies,
                                   ProtocolOptions protocolOptions,
                                   PoolingOptions poolingOptions,
                                   SocketOptions socketOptions,
                                   ClientOptions clientOptions,
                                   IAuthInfoProvider authProvider)
        {
            this._cluster = cluster;
            this._reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            this._reconnectionTimer = new Timer(ReconnectionClb, null, Timeout.Infinite, Timeout.Infinite);

            _session = new Session(cluster, policies, protocolOptions, poolingOptions, socketOptions,
                                   clientOptions, authProvider, "");
        }

        void Metadata_HostsEvent(object sender, HostsEventArgs e)
        {
            if (sender == this)
                return;

            Action<object> act = new Action<object>((_) => SetupControlConnection());

            if (e.What == HostsEventArgs.Kind.Down)
            {
                if (e.IPAddress.Equals(listeningOnHost))
                    act.BeginInvoke(null, (ar) => { act.EndInvoke(ar); }, null);
            }
            else if (e.What == HostsEventArgs.Kind.Up)
            {
                if (_isDiconnected)
                    act.BeginInvoke(null, (ar) => { act.EndInvoke(ar); }, null);
            }
        }

        internal void Init()
        {
            _cluster.Metadata.HostsEvent += new HostsEventHandler(Metadata_HostsEvent);

            _session.Init();
            SetupControlConnection();
        }

        public void Dispose()
        {
            _session.Dispose();
        }

        IPAddress listeningOnHost;
        private void SetupEventListener()
        {
            var triedHosts = new List<IPAddress>();
            var innerExceptions = new Dictionary<IPAddress, List<Exception>>();

            var hostsIter = _session._policies.LoadBalancingPolicy.NewQueryPlan(null).GetEnumerator();
            
            if (!hostsIter.MoveNext())
            {
                var ex = new NoHostAvailableException(new Dictionary<IPAddress, List<Exception>>());
                _logger.Error(ex);
                throw ex;
            }

            var nconn = _session.Connect(null, hostsIter, triedHosts, innerExceptions);
            listeningOnHost = nconn.GetHostAdress();

            Exception theExc = null;

            nconn.CassandraEvent += new CassandraEventHandler(conn_CassandraEvent);
            using (var ret = nconn.RegisterForCassandraEvent(
                CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange))
            {
                if (!(ret is OutputVoid))
                {
                    if (ret is OutputError)
                        theExc = (ret as OutputError).CreateException();
                    else
                        theExc = new DriverInternalError("Expected Error on Output");
                }
            }

            if (theExc != null)
            {
                _logger.Error(theExc);
                throw theExc;
            }

        }

        private void conn_CassandraEvent(object sender, CassandraEventArgs e)
        {
            var act = new Action<object>((_) =>
            {
                if (e is TopologyChangeEventArgs)
                {
                    var tce = e as TopologyChangeEventArgs;
                    if (tce.What == TopologyChangeEventArgs.Reason.NewNode)
                    {
                        SetupControlConnection(true);
                        _cluster.Metadata.AddHost(tce.Address);
                        return;
                    }
                    else if (tce.What == TopologyChangeEventArgs.Reason.RemovedNode)
                    {
                        _cluster.Metadata.RemoveHost(tce.Address);
                        SetupControlConnection(!tce.Address.Equals(listeningOnHost));
                        return;
                    }
                }
                else if (e is StatusChangeEventArgs)
                {
                    var sce = e as StatusChangeEventArgs;
                    if (sce.What == StatusChangeEventArgs.Reason.Up)
                    {
                        _cluster.Metadata.BringUpHost(sce.Address, this);
                        return;
                    }
                    else if (sce.What == StatusChangeEventArgs.Reason.Down)
                    {
                        _cluster.Metadata.SetDownHost(sce.Address, this);
                        return;
                    }
                }
                else if (e is SchemaChangeEventArgs)
                {
                    var ssc = e as SchemaChangeEventArgs;

                    if (ssc.What == SchemaChangeEventArgs.Reason.Created)
                    {
                        SubmitSchemaRefresh(string.IsNullOrEmpty(ssc.Keyspace) ? null : ssc.Keyspace, null);
                        return;
                    }
                    else if (ssc.What == SchemaChangeEventArgs.Reason.Dropped)
                    {
                        SubmitSchemaRefresh(string.IsNullOrEmpty(ssc.Keyspace) ? null : ssc.Keyspace, null);
                        return;
                    }
                    else if (ssc.What == SchemaChangeEventArgs.Reason.Updated)
                    {
                        SubmitSchemaRefresh(ssc.Keyspace, string.IsNullOrEmpty(ssc.Table) ? null : ssc.Table);
                        return;
                    }
                }

                var ex = new DriverInternalError("Unknown Event");
                _logger.Error(ex);
                throw ex;
            });
            act.BeginInvoke(null, (ar) => { act.EndInvoke(ar); }, null);
        }

        private bool _isDiconnected = false;
        private readonly Timer _reconnectionTimer;

        private void ReconnectionClb(object state)
        {
            try
            {
                SetupControlConnection();
            }
            catch (Exception ex)
            {
                _logger.Error(ex);
            }
        }

        private readonly IReconnectionPolicy _reconnectionPolicy = new ExponentialReconnectionPolicy(2*1000, 5*60*1000);
        private IReconnectionSchedule _reconnectionSchedule = null;

        internal bool RefreshHosts()
        {
            lock (this)
            {
                try
                {
                    if (!_isDiconnected)
                    {
                        RefreshNodeListAndTokenMap();
                        return true;
                    }
                    return false;
                }
                catch (NoHostAvailableException)
                {
                    _logger.Error("ControlConnection is lost now.");
                    return false;
                }
                catch (Exception ex)
                {
                    if (CassandraConnection.IsStreamRelatedException(ex))
                    {
                        _logger.Error("ControlConnection is lost now.");
                        return false;
                    }
                    else
                    {
                        _logger.Error("Unexpected error occurred during forced ControlConnection refresh.", ex);
                        throw;
                    }
                }
            }
        }

        private void SetupControlConnection(bool refreshOnly = false)
        {
            lock (this)
            {
                try
                {
                    _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    _logger.Info("Refreshing ControlConnection...");
                    if (!refreshOnly)
                    {
                        Monitor.Exit(this);
                        try
                        {
                            SetupEventListener();
                        }
                        finally
                        {
                            Monitor.Enter(this);
                        }
                    } 
                     RefreshNodeListAndTokenMap();
                    _isDiconnected = false;
                    _logger.Info("ControlConnection is fresh!");
                }
                catch (NoHostAvailableException)
                {
                    _isDiconnected = true;
                    _logger.Error("ControlConnection is lost. Retrying..");
                    _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                }
                catch (Exception ex)
                {
                    if (CassandraConnection.IsStreamRelatedException(ex))
                    {
                        _isDiconnected = true;
                        _logger.Error("ControlConnection is lost. Retrying..");
                        _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                    }
                    else
                    {
                        _logger.Error("Unexpected error occurred during ControlConnection refresh.", ex);
//                        throw;
                    }
                }
            }
        }

        // schema
        private const string SelectPeers = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";                                            

        private const string SelectLocal =
            "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'";

        private void RefreshNodeListAndTokenMap()
        {
            _logger.Info("Refreshing NodeList and TokenMap..");
            // Make sure we're up to date on nodes and tokens            
            var tokenMap = new Dictionary<IPAddress, DictSet<string>>();
            string partitioner = null;

            var foundHosts = new List<IPAddress>();
            var dcs = new List<string>();
            var racks = new List<string>();
            var allTokens = new List<DictSet<string>>();
            IPAddress queriedHost;

            using (var rowset = _session.Query(SelectPeers, ConsistencyLevel.Quorum))
            {
                queriedHost = rowset.Info.QueriedHost;
                foreach (var row in rowset.GetRows())
                {
                    IPAddress hstip = null;
                    if(!row.IsNull("rpc_address"))
                         hstip = row.GetValue<IPEndPoint>("rpc_address").Address;
                    if (hstip == null)
                    {
                        if (!row.IsNull("peer"))
                            hstip = row.GetValue<IPEndPoint>("peer").Address;
                        _logger.Error("No rpc_address found for host in peers system table. ");
                    }
                    else if (hstip.Equals(bindAllAddress))
                    {
                        if (!row.IsNull("peer"))
                            hstip = row.GetValue<IPEndPoint>("peer").Address;
                    }

                    if (hstip != null)
                    {
                        foundHosts.Add(hstip);
                        dcs.Add(row.GetValue<string>("data_center"));
                        racks.Add(row.GetValue<string>("rack"));
                        var col = row.GetValue<IEnumerable<string>>("tokens");
                        if (col == null)
                            allTokens.Add(new DictSet<string>());
                        else
                            allTokens.Add(new DictSet<string>(col));
                    }
                }
            }

            var localhost = _cluster.Metadata.GetHost(queriedHost);
            var iterLiof = new List<Host>() { localhost }.GetEnumerator();
            iterLiof.MoveNext();
            List<IPAddress> tr = new List<IPAddress>();
            Dictionary<IPAddress, List<Exception>> exx = new Dictionary<IPAddress, List<Exception>>();
            var cn = _session.Connect(null, iterLiof, tr, exx);

            using (var outp = cn.Query(SelectLocal, ConsistencyLevel.Default,false))
            {
                if (outp is OutputRows)
                {
                    var rowset = new RowSet((outp as OutputRows), null, false);
                    // Update cluster name, DC and rack for the one node we are connected to
                    foreach (var localRow in rowset.GetRows())
                    {
                        var clusterName = localRow.GetValue<string>("cluster_name");
                        if (clusterName != null)
                            _cluster.Metadata.ClusterName = clusterName;



                        // In theory host can't be null. However there is no point in risking a NPE in case we
                        // have a race between a node removal and this.
                        if (localhost != null)
                        {
                            localhost.SetLocationInfo(localRow.GetValue<string>("data_center"), localRow.GetValue<string>("rack"));

                            partitioner = localRow.GetValue<string>("partitioner");
                            var tokens = localRow.GetValue<IList<string>>("tokens");
                            if (partitioner != null && tokens.Count > 0)
                            {
                                if (!tokenMap.ContainsKey(localhost.Address))
                                    tokenMap.Add(localhost.Address, new DictSet<string>());
                                tokenMap[localhost.Address].AddRange(tokens);
                            }
                        }

                        break; //fetch only one row
                    }
                }
            }

            for (int i = 0; i < foundHosts.Count; i++)
            {
                var host = _cluster.Metadata.GetHost(foundHosts[i]);
                if (host == null)
                {
                    // We don't know that node, add it.
                    host = _cluster.Metadata.AddHost(foundHosts[i]);
                }
                host.SetLocationInfo(dcs[i], racks[i]);

                if (partitioner != null && !allTokens[i].IsEmpty)
                    tokenMap.Add(host.Address, allTokens[i]);
            }

            // Removes all those that seems to have been removed (since we lost the control connection)
            var foundHostsSet = new DictSet<IPAddress>(foundHosts);
            foreach (var host in _cluster.Metadata.AllReplicas())
                if (!host.Equals(queriedHost) && !foundHostsSet.Contains(host))
                    _cluster.Metadata.RemoveHost(host);

            if (partitioner != null)
                _cluster.Metadata.RebuildTokenMap(partitioner, tokenMap);

            _logger.Info("NodeList and TokenMap have been successfully refreshed!");
        }

        private const long MaxSchemaAgreementWaitMs = 10000;

        private const string SelectSchemaPeers = "SELECT peer, rpc_address, schema_version FROM system.peers";
        private const string SelectSchemaLocal = "SELECT schema_version FROM system.local WHERE key='local'";

        internal bool WaitForSchemaAgreement(IPAddress forHost = null)
        {
            var start = DateTimeOffset.Now;
            long elapsed = 0;
            while (elapsed < MaxSchemaAgreementWaitMs)
            {
                var versions = new DictSet<Guid>();


                IPAddress queriedHost;

                if (forHost == null)
                {
                    using (var rowset = _session.Query(SelectSchemaPeers, ConsistencyLevel.Default))
                    {
                        queriedHost = rowset.Info.QueriedHost;
                        foreach (var row in rowset.GetRows())
                        {
                            if (row.IsNull("rpc_address") || row.IsNull("schema_version"))
                                continue;

                            var rpc = row.GetValue<IPEndPoint>("rpc_address").Address;
                            if (rpc.Equals(bindAllAddress))
                                if (!row.IsNull("peer"))
                                    rpc = row.GetValue<IPEndPoint>("peer").Address;

                            Host peer = _cluster.Metadata.GetHost(rpc);
                            if (peer != null && peer.IsConsiderablyUp)
                                versions.Add(row.GetValue<Guid>("schema_version"));
                        }
                    }
                }
                else
                {
                    queriedHost = forHost;
                    var localhost = _cluster.Metadata.GetHost(queriedHost);
                    var iterLiof = new List<Host>() { localhost }.GetEnumerator();
                    iterLiof.MoveNext();
                    List<IPAddress> tr = new List<IPAddress>();
                    Dictionary<IPAddress, List<Exception>> exx = new Dictionary<IPAddress, List<Exception>>();
                    var cn = _session.Connect(null, iterLiof, tr, exx);

                    using (var outp = cn.Query(SelectSchemaPeers, ConsistencyLevel.Default, false))
                    {
                        if (outp is OutputRows)
                        {
                            var rowset = new RowSet((outp as OutputRows), null, false);
                            foreach (var row in rowset.GetRows())
                            {
                                if (row.IsNull("rpc_address") || row.IsNull("schema_version"))
                                    continue;

                                var rpc = row.GetValue<IPEndPoint>("rpc_address").Address;
                                if (rpc.Equals(bindAllAddress))
                                {
                                    if (!row.IsNull("peer"))
                                        rpc = row.GetValue<IPEndPoint>("peer").Address;
                                }

                                Host peer = _cluster.Metadata.GetHost(rpc);
                                if (peer != null && peer.IsConsiderablyUp)
                                    versions.Add(row.GetValue<Guid>("schema_version"));
                            }
                        }
                    }
                }

                {
                    var localhost = _cluster.Metadata.GetHost(queriedHost);
                    var iterLiof = new List<Host>() { localhost }.GetEnumerator();
                    iterLiof.MoveNext();
                    List<IPAddress> tr = new List<IPAddress>();
                    Dictionary<IPAddress, List<Exception>> exx = new Dictionary<IPAddress, List<Exception>>();
                    var cn = _session.Connect(null, iterLiof, tr, exx);

                    using (var outp = cn.Query(SelectSchemaLocal, ConsistencyLevel.Default, false))
                    {
                        if (outp is OutputRows)
                        {
                            var rowset = new RowSet((outp as OutputRows), null, false);
                            // Update cluster name, DC and rack for the one node we are connected to
                            foreach (var localRow in rowset.GetRows())
                                if (!localRow.IsNull("schema_version"))
                                {
                                    versions.Add(localRow.GetValue<Guid>("schema_version"));
                                    break;
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

        private const String SelectKeyspaces = "SELECT * FROM system.schema_keyspaces";
        private const String SelectColumnFamilies = "SELECT * FROM system.schema_columnfamilies";
        private const String SelectColumns = "SELECT * FROM system.schema_columns";


        internal void SubmitSchemaRefresh(string keyspace, string table, AsyncResultNoResult ar = null)
        {
            if (keyspace == null)
                ResetSchema();
            else if (table == null)
                ResetKeyspace(keyspace);
            else
                ResetTable(keyspace, table);
        }

        private void ResetSchema()
        {
            _keyspaces.Value = null;
        }

        private ReadOnlyDictionary<string, KeyspaceMetadata> SetupSchema()
        {
            var ks = _keyspaces.Value;
            if (ks == null)
            {
                var newKeyspaces = new ReadOnlyDictionary<string, KeyspaceMetadata>();
                using (var rows = _session.Query(SelectKeyspaces))
                {
                    foreach (var row in rows.GetRows())
                    {
                        var strKsName = row.GetValue<string>("keyspace_name");
                        var strClass = GetStrategyClass(row.GetValue<string>("strategy_class"));
                        var drblWrites = row.GetValue<bool>("durable_writes");
                        var rplctOptions = Utils.ConvertStringToMapInt(row.GetValue<string>("strategy_options"));

                        var newMetadata = new KeyspaceMetadata(this, strKsName, drblWrites, strClass,
                                                               new ReadOnlyDictionary<string, int>(rplctOptions));

                        newKeyspaces.InternalSetup(strKsName, newMetadata);
                    }
                }
                return _keyspaces.Value = newKeyspaces;
            }
            else
            {
                return ks;
            }
        }

        private void ResetKeyspace(string keyspace)
        {
            var ks = _keyspaces.Value;
            if (ks != null)
            {
                if (ks.ContainsKey(keyspace))
                    ks[keyspace].Tables.Value=null;
            }
        }

        private KeyspaceMetadata SetupKeyspace(string keyspace)
        {
            var sc = SetupSchema();
            if (!sc.ContainsKey(keyspace) || sc[keyspace].Tables.Value == null)
            {
                WaitForSchemaAgreement();
                ResetSchema();
                sc= SetupSchema();
                KeyspaceMetadata ks = null;
                using (
                    var rows =
                        _session.Query(
                            string.Format(
                                SelectKeyspaces + " WHERE keyspace_name='{0}';",
                                keyspace)))
                {
                    foreach (var row in rows.GetRows())
                    {
                        var strKsName = row.GetValue<string>("keyspace_name");
                        var strClass = GetStrategyClass(row.GetValue<string>("strategy_class"));
                        var drblWrites = row.GetValue<bool>("durable_writes");
                        var rplctOptions = Utils.ConvertStringToMapInt(row.GetValue<string>("strategy_options"));

                        ks = new KeyspaceMetadata(this, strKsName, drblWrites, strClass,
                                                  new ReadOnlyDictionary<string, int>(rplctOptions));

                    }
                }
                if(ks==null)
                    throw new InvalidOperationException();

                var ktb = new ReadOnlyDictionary<string, AtomicValue<TableMetadata>>();
                using (
                    var rows =
                        _session.Query(string.Format(SelectColumnFamilies + " WHERE keyspace_name='{0}';", keyspace)))
                {                    
                    foreach (var row in rows.GetRows())
                        ktb.InternalSetup(row.GetValue<string>("columnfamily_name"), new AtomicValue<TableMetadata>(null));
                    
                }
                ks.Tables.Value = ktb;
                sc.InternalSetup(ks.Name, ks);
                return ks;
            }
            else
            {
                return sc[keyspace];
            }
        }


        private void ResetTable(string keyspace, string table)
        {
            var ks = _keyspaces.Value;

            if (ks != null)
            {
                if (ks.ContainsKey(keyspace))
                {
                    var kss = ks[keyspace].Tables.Value;
                    if(kss!=null)
                        if (kss.ContainsKey(table))
                            kss[table].Value = null;
                }
            }
        }

        private TableMetadata SetupTable(string keyspace, string table)
        {
            bool wasc = false;
            RETRY:
            var ks = SetupKeyspace(keyspace);
            var tbl = ks.Tables.Value;
            if (tbl == null)
            {
                goto RETRY;
            }
            if (!tbl.ContainsKey(table))
            {
                WaitForSchemaAgreement();
                ResetKeyspace(keyspace);
                if (wasc)
                    throw new IndexOutOfRangeException();
                wasc = true;
                goto RETRY;
            }

            if (tbl[table].Value == null)
            {
                var m = GetTableMetadata(table, keyspace);
                tbl[table].Value = m;
                return m;
            }
            else
            {
                return tbl[table].Value;
            }
        }

        internal void RefreshSchema(string keyspace, string table)
        {
           if(keyspace==null)
           {
               ResetSchema();
               SetupSchema();
           }
           else if (table == null)
           {
               ResetKeyspace(keyspace);
               SetupKeyspace(keyspace);
           }
           else
           {
               ResetTable(keyspace,table);
               SetupTable(keyspace, table);
           }
        }

        private readonly AtomicValue<ReadOnlyDictionary<string, KeyspaceMetadata>> _keyspaces =
            new AtomicValue<ReadOnlyDictionary<string, KeyspaceMetadata>>(null);

        public ICollection<string> GetKeyspaces()
        {
            return SetupSchema().Keys;
        }

        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            return SetupKeyspace(keyspace);
        }

        public ICollection<string> GetTables(string keyspace)
        {
            return SetupKeyspace(keyspace).Tables.Value.Keys;
        }

        public TableMetadata GetTable(string keyspace, string table)
        {
            return SetupTable(keyspace, table);
        }

        public string GetStrategyClass(string strClass)
        {
            if (strClass != null)
            {
                if (strClass.StartsWith("org.apache.cassandra.locator."))
                    strClass = strClass.Replace("org.apache.cassandra.locator.", "");
            }
            else
                throw new ArgumentNullException("Cannot retrieve informations about strategy class!");

            return strClass;
        }
        private SortedDictionary<string,string> getCompactionStrategyOptions(Row row)
        {
            SortedDictionary<string, string> result = new SortedDictionary<string, string>(){{"class",row.GetValue<string>("compaction_strategy_class")}};
            foreach(var entry in Utils.ConvertStringToMap(row.GetValue<string>("compaction_strategy_options")))            
                result.Add(entry.Key, entry.Value);             
            return result;
        }

        public TableMetadata GetTableMetadata(string tableName, string keyspaceName)
        {
            object[] collectionValuesTypes;
            List<TableColumn> cols = new List<TableColumn>();
            TableOptions Options = null;
            using (
                var rows =
                    _session.Query(
                        string.Format(SelectColumns + " WHERE columnfamily_name='{0}' AND keyspace_name='{1}';",
                                      tableName, keyspaceName)))
            {
                foreach (var row in rows.GetRows())
                {
                    var tp_code = convertToColumnTypeCode(row.GetValue<string>("validator"), out collectionValuesTypes);
                    var dsc = new TableColumn()
                        {
                            Name = row.GetValue<string>("column_name"),
                            Keyspace = row.GetValue<string>("keyspace_name"),
                            Table = row.GetValue<string>("columnfamily_name"),
                            TypeCode = tp_code,
                            SecondaryIndexName = row.GetValue<string>("index_name"),
                            SecondaryIndexType = row.GetValue<string>("index_type"),
                            KeyType =
                                row.GetValue<string>("index_name") != null
                                    ? KeyType.SecondaryIndex
                                    : KeyType.None,
                        };

                    if (tp_code == ColumnTypeCode.List)
                        dsc.TypeInfo = new ListColumnInfo()
                            {
                                ValueTypeCode = (ColumnTypeCode)collectionValuesTypes[0]
                            };
                    else if (tp_code == ColumnTypeCode.Map)
                        dsc.TypeInfo = new MapColumnInfo()
                            {
                                KeyTypeCode = (ColumnTypeCode)collectionValuesTypes[0],
                                ValueTypeCode = (ColumnTypeCode)collectionValuesTypes[1]
                            };
                    else if (tp_code == ColumnTypeCode.Set)
                        dsc.TypeInfo = new SetColumnInfo()
                            {
                                KeyTypeCode = (ColumnTypeCode)collectionValuesTypes[0]
                            };

                    cols.Add(dsc);
                }
            }

            using (
                var rows =
                    _session.Query(
                        string.Format(
                            SelectColumnFamilies + " WHERE columnfamily_name='{0}' AND keyspace_name='{1}';",
                            tableName, keyspaceName)))
            {
                foreach (var row in rows.GetRows()) // There is only one row!
                {
                    var colNames = row.GetValue<string>("column_aliases");
                    var rowKeys = colNames.Substring(1, colNames.Length - 2).Split(',');
                    for (int i = 0; i < rowKeys.Length; i++)
                    {
                        if (rowKeys[i].StartsWith("\""))
                        {
                            rowKeys[i] = rowKeys[i].Substring(1, rowKeys[i].Length - 2).Replace("\"\"", "\"");
                        }
                    }

                    if (rowKeys.Length > 0 && rowKeys[0] != string.Empty)
                    {                                                
                        bool isCompact = true;
                        string comparator = row.GetValue<string>("comparator");
                        if (comparator.StartsWith("org.apache.cassandra.db.marshal.CompositeType"))
                        {
                            comparator = comparator.Replace("org.apache.cassandra.db.marshal.CompositeType", "");
                            isCompact = false;
                        }
                        
                        var rg = new Regex(@"org\.apache\.cassandra\.db\.marshal\.\w+");
                        var rowKeysTypes = rg.Matches(comparator);                        

                        int i = 0;
                        foreach (var keyName in rowKeys)
                        {
                            var tp_code = convertToColumnTypeCode(rowKeysTypes[i].ToString(),
                                                                  out collectionValuesTypes);
                            var dsc = new TableColumn()
                                {
                                    Name = keyName.ToString(),
                                    Keyspace = row.GetValue<string>("keyspace_name"),
                                    Table = row.GetValue<string>("columnfamily_name"),
                                    TypeCode = tp_code,
                                    KeyType = KeyType.Clustering,
                                };
                            if (tp_code == ColumnTypeCode.List)
                                dsc.TypeInfo = new ListColumnInfo()
                                    {
                                        ValueTypeCode = (ColumnTypeCode) collectionValuesTypes[0]
                                    };
                            else if (tp_code == ColumnTypeCode.Map)
                                dsc.TypeInfo = new MapColumnInfo()
                                    {
                                        KeyTypeCode = (ColumnTypeCode) collectionValuesTypes[0],
                                        ValueTypeCode = (ColumnTypeCode) collectionValuesTypes[1]
                                    };
                            else if (tp_code == ColumnTypeCode.Set)
                                dsc.TypeInfo = new SetColumnInfo()
                                    {
                                        KeyTypeCode = (ColumnTypeCode) collectionValuesTypes[0]
                                    };
                            cols.Add(dsc);
                            i++;
                        }

                        Options = new TableOptions()
                        {
                            isCompactStorage = isCompact,
                            bfFpChance = row.GetValue<double>("bloom_filter_fp_chance"),
                            caching = row.GetValue<string>("caching"),
                            comment = row.GetValue<string>("comment"),
                            gcGrace = row.GetValue<int>("gc_grace_seconds"),
                            localReadRepair = row.GetValue<double>("local_read_repair_chance"),
                            readRepair = row.GetValue<double>("read_repair_chance"),
                            replicateOnWrite = row.GetValue<bool>("replicate_on_write"),
                            compactionOptions = (SortedDictionary<string, string>)getCompactionStrategyOptions(row),
                            compressionParams = (SortedDictionary<string, string>)Utils.ConvertStringToMap(row.GetValue<string>("compression_parameters"))
                        };

                    }
                    cols.Add(new TableColumn()
                        {
                            Name =
                                row.GetValue<string>("key_aliases")
                                   .Replace("[\"", "")
                                   .Replace("\"]", "")
                                   .Replace("\"\"", "\""),
                            Keyspace = row.GetValue<string>("keyspace_name"),
                            Table = row.GetValue<string>("columnfamily_name"),
                            TypeCode =
                                convertToColumnTypeCode(row.GetValue<string>("key_validator"), out collectionValuesTypes),
                            KeyType = KeyType.Partition
                        });
                }
            }
            return new TableMetadata(tableName, cols.ToArray(), Options);
        }


        private ColumnTypeCode convertToColumnTypeCode(string type, out object[] collectionValueTp)
        {
            object[] obj;
            collectionValueTp = new object[2];
            if (type.StartsWith("org.apache.cassandra.db.marshal.ListType"))
            {
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.ListType(", "").Replace(")", ""), out obj);
                return ColumnTypeCode.List;
            }
            if (type.StartsWith("org.apache.cassandra.db.marshal.SetType"))
            {
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.SetType(", "").Replace(")", ""), out obj);
                return ColumnTypeCode.Set;
            }

            if (type.StartsWith("org.apache.cassandra.db.marshal.MapType"))
            {
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.MapType(", "").Replace(")", "").Split(',')[0], out obj);
                collectionValueTp[1] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.MapType(", "").Replace(")", "").Split(',')[1], out obj);
                return ColumnTypeCode.Map;
            }

            collectionValueTp = null;
            switch (type)
            {
                case "org.apache.cassandra.db.marshal.UTF8Type":
                    return ColumnTypeCode.Text;
                case "org.apache.cassandra.db.marshal.UUIDType":
                    return ColumnTypeCode.Uuid;
                case "org.apache.cassandra.db.marshal.Int32Type":
                    return ColumnTypeCode.Int;
                case "org.apache.cassandra.db.marshal.BytesType":
                    return ColumnTypeCode.Blob;
                case "org.apache.cassandra.db.marshal.FloatType":
                    return ColumnTypeCode.Float;
                case "org.apache.cassandra.db.marshal.DoubleType":
                    return ColumnTypeCode.Double;
                case "org.apache.cassandra.db.marshal.BooleanType":
                    return ColumnTypeCode.Boolean;
                case "org.apache.cassandra.db.marshal.InetAddressType":
                    return ColumnTypeCode.Inet;
                case "org.apache.cassandra.db.marshal.DateType":
                    return ColumnTypeCode.Timestamp;
                case "org.apache.cassandra.db.marshal.LongType":
                    return ColumnTypeCode.Bigint;
#if NET_40_OR_GREATER
                case "org.apache.cassandra.db.marshal.DecimalType":
                    return ColumnTypeCode.Decimal;
                case "org.apache.cassandra.db.marshal.IntegerType":
                    return ColumnTypeCode.Varint;
#endif
                default: 
                    var ex = new DriverInternalError("Unsupported data type:" + type);  
                    _logger.Error(string.Format("Unsupported data type: {0}", type), ex);
                    throw ex;
            }
        }

    }
}
