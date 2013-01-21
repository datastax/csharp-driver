using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Net;

namespace Cassandra
{
    internal class ControlConnection : IDisposable
    {
        private readonly Session _session;
        private readonly Session _owner;
        private IEnumerator<Host> _hostsIter = null;
        private readonly Hosts _hosts;

        internal ControlConnection(Cluster cluster, Session owner,
                                   IEnumerable<IPAddress> clusterEndpoints,
                                   Policies policies,
                                   ProtocolOptions protocolOptions,
                                   PoolingOptions poolingOptions,
                                   SocketOptions socketOptions,
                                   ClientOptions clientOptions,
                                   IAuthInfoProvider authProvider,
                                   bool metricsEnabled, Hosts hosts)
        {
            this._hosts = hosts;
            this._reconnectionTimer = new Timer(ReconnectionClb, null, Timeout.Infinite, Timeout.Infinite);
            this._owner = owner;

            Metadata = new Metadata(hosts, this);

            _session = new Session(cluster, clusterEndpoints, policies, protocolOptions, poolingOptions, socketOptions,
                                   clientOptions, authProvider, metricsEnabled, "", _hosts);
        }

        internal void Init()
        {
            _session.Init();
            Go(true);
            StartRefreshSchemaThread();
        }
        
        public void Dispose()
        {
            StopRefreshSchemaThread();
        }

        private void SetupEventListeners(CassandraConnection nconn)
        {
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
                throw theExc;
        }

        private void conn_CassandraEvent(object sender, CassandraEventArgs e)
        {
            if (e is TopopogyChangeEventArgs)
            {
                var tce = e as TopopogyChangeEventArgs;
                if (tce.What == TopopogyChangeEventArgs.Reason.NewNode)
                {
                    _owner.OnAddHost(tce.Address);
                    _session.OnAddHost(tce.Address);
                    CheckConnectionUp(tce.Address);
                    return;
                }
                else if (tce.What == TopopogyChangeEventArgs.Reason.RemovedNode)
                {
                    _owner.OnRemovedHost(tce.Address);
                    _session.OnRemovedHost(tce.Address);
                    CheckConnectionDown(tce.Address);
                    return;
                }
            }
            else if (e is StatusChangeEventArgs)
            {
                var sce = e as StatusChangeEventArgs;
                if (sce.What == StatusChangeEventArgs.Reason.Up)
                {
                    _owner.OnAddHost(sce.Address);
                    _session.OnAddHost(sce.Address);
                    CheckConnectionUp(sce.Address);
                    return;
                }
                else if (sce.What == StatusChangeEventArgs.Reason.Down)
                {
                    _owner.OnDownHost(sce.Address);
                    _session.OnDownHost(sce.Address);
                    CheckConnectionDown(sce.Address);
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

            throw new DriverInternalError("Unknown Event");
        }

        internal void OwnerHostIsDown(IPAddress endpoint)
        {
            _session.OnDownHost(endpoint);
            CheckConnectionDown(endpoint);
        }

        internal void OwnerHostBringUpIfDown(IPAddress endpoint)
        {
            _session.OnAddHost(endpoint);
            CheckConnectionUp(endpoint);
        }

        private bool _isDiconnected = false;
        private readonly Timer _reconnectionTimer;

        private void ReconnectionClb(object state)
        {
            try
            {
                Go(true);
            }
            catch (Exception ex)
            {
                Console.WriteLine("!!"+ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private readonly IReconnectionPolicy _reconnectionPolicy = new ExponentialReconnectionPolicy(2*1000, 5*60*1000);
        private IReconnectionSchedule _reconnectionSchedule = null;

        private CassandraConnection _connection = null;
        internal readonly Metadata Metadata;

        private void Go(bool refresh)
        {
            try
            {
                if (_hostsIter == null)
                    _hostsIter = _owner._policies.LoadBalancingPolicy.NewQueryPlan(null).GetEnumerator();

                if (!_hostsIter.MoveNext())
                {
                    _isDiconnected = true;
                    _hostsIter = null;
                    _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                }
                else
                {
                    _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    _connection = _session.Connect(null, _hostsIter);
                    SetupEventListeners(_connection);
                    if (refresh)
                        RefreshNodeListAndTokenMap(_connection);
                }
            }
            catch (NoHostAvailableException)
            {
                _isDiconnected = true;
                _hostsIter = null;
                _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
            }
            catch (Exception ex)
            {
                if (CassandraConnection.IsStreamRelatedException(ex))
                {
                    _isDiconnected = true;
                    _hostsIter = null;
                    _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                }
                else
                    throw;
            }
        }

        private void CheckConnectionDown(IPAddress endpoint)
        {
            if (_hostsIter != null && _hostsIter.Current.Address == endpoint)
            {
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                Go(false);
            }
        }

        private void CheckConnectionUp(IPAddress endpoint)
        {
            if (_isDiconnected)
            {
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                Go(true);
            }
        }


        // schema
        private const string SelectPeers = "SELECT peer, data_center, rack, tokens FROM system.peers";

        private const string SelectLocal =
            "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'";

        private void RefreshNodeListAndTokenMap(CassandraConnection connection)
        {
            // Make sure we're up to date on nodes and tokens

            var tokenMap = new Dictionary<IPAddress, DictSet<string>>();
            string partitioner = null;

            using (var rowset = _session.Query(SelectLocal, ConsistencyLevel.Default))
            {
                // Update cluster name, DC and rack for the one node we are connected to
                foreach (var localRow in rowset.GetRows())
                {
                    var clusterName = localRow.GetValue<string>("cluster_name");
                    if (clusterName != null)
                        Metadata.ClusterName = clusterName;

                    var host = Metadata.GetHost(connection.GetHostAdress());
                    // In theory host can't be null. However there is no point in risking a NPE in case we
                    // have a race between a node removal and this.
                    if (host != null)
                        host.SetLocationInfo(localRow.GetValue<string>("data_center"), localRow.GetValue<string>("rack"));

                    partitioner = localRow.GetValue<string>("partitioner");
                    var tokens = localRow.GetValue<IList<string>>("tokens");
                    if (partitioner != null && tokens.Count > 0)
                    {
                        if (!tokenMap.ContainsKey(host.Address))
                            tokenMap.Add(host.Address, new DictSet<string>());
                        tokenMap[host.Address].AddRange(tokens);
                    }
                    break;
                }
            }

            var foundHosts = new List<IPAddress>();
            var dcs = new List<string>();
            var racks = new List<string>();
            var allTokens = new List<DictSet<string>>();

            using (var rowset = _session.Query(SelectPeers, ConsistencyLevel.Default))
            {
                foreach (var row in rowset.GetRows())
                {
                    var hstip = row.GetValue<IPAddress>("peer");
                    if (hstip != null)
                    {
                        foundHosts.Add(hstip);
                        dcs.Add(row.GetValue<string>("data_center"));
                        racks.Add(row.GetValue<string>("rack"));
                        allTokens.Add(new DictSet<string>(row.GetValue<IEnumerable<string>>("tokens")));
                    }
                }
            }

            for (int i = 0; i < foundHosts.Count; i++)
            {
                var host = Metadata.GetHost(foundHosts[i]);
                if (host == null)
                {
                    // We don't know that node, add it.
                    host = Metadata.AddHost(foundHosts[i], _owner._policies.ReconnectionPolicy);
                }
                host.SetLocationInfo(dcs[i], racks[i]);

                if (partitioner != null && !allTokens[i].IsEmpty)
                    tokenMap.Add(host.Address, allTokens[i]);
            }

            // Removes all those that seems to have been removed (since we lost the control connection)
            var foundHostsSet = new DictSet<IPAddress>(foundHosts);
            foreach (var host in Metadata.AllReplicas())
                if (!host.Equals(connection.GetHostAdress()) && !foundHostsSet.Contains(host))
                    Metadata.RemoveHost(host);

            if (partitioner != null)
                Metadata.RebuildTokenMap(partitioner, tokenMap);
        }

        private const long MaxSchemaAgreementWaitMs = 10000;

        private const string SelectSchemaPeers = "SELECT peer, schema_version FROM system.peers";
        private const string SelectSchemaLocal = "SELECT schema_version FROM system.local WHERE key='local'";

        private bool WaitForSchemaAgreement()
        {
            var start = DateTimeOffset.Now;
            long elapsed = 0;
            while (elapsed < MaxSchemaAgreementWaitMs)
            {
                var versions = new DictSet<Guid>();

                using (var rowset = _session.Query(SelectSchemaLocal, ConsistencyLevel.Default))
                {
                    foreach (var localRow in rowset.GetRows())
                        if (!localRow.IsNull("schema_version"))
                        {
                            versions.Add(localRow.GetValue<Guid>("schema_version"));
                            break;
                        }
                }

                using (var rowset = _session.Query(SelectSchemaPeers, ConsistencyLevel.Default))
                {
                    foreach (var row in rowset.GetRows())
                    {
                        if (row.IsNull("peer") || row.IsNull("schema_version"))
                            continue;

                        Host peer = Metadata.GetHost(row.GetValue<IPEndPoint>("peer").Address);
                        if (peer != null && peer.IsConsiderablyUp)
                            versions.Add(row.GetValue<Guid>("schema_version"));
                    }
                }

                if (versions.Count <= 1)
                    return true;

                // let's not flood the node too much
                Thread.Sleep(200);
                elapsed = (long) (DateTimeOffset.Now - start).TotalMilliseconds;
            }

            return false;
        }


        private const String SelectKeyspaces = "SELECT * FROM system.schema_keyspaces";
        private const String SelectColumnFamilies = "SELECT * FROM system.schema_columnfamilies";
        private const String SelectColumns = "SELECT * FROM system.schema_columns";

        private readonly AtomicValue<bool> _invalidKeyspace = new AtomicValue<bool>(false);

        private Thread _refreshSchemaThread = null;
        private bool _endOfRefreshSchemaThread;
        private readonly object _notifyRefreshSchemaThread = new object();
        private readonly Queue<RefreshSchemaCmd> _commandsRefreshSchemaThread = new Queue<RefreshSchemaCmd>();

        class RefreshSchemaCmd
        {
            public string Keyspace;
            public string Table;
            public AsyncResultNoResult AsyncResult;
        }

        private void StartRefreshSchemaThread()
        {
            _endOfRefreshSchemaThread = false;
            _refreshSchemaThread = new Thread((_) =>
                {
                    try
                    {
                        while (true)
                        {
                            lock (_notifyRefreshSchemaThread)
                            {
                                if (_endOfRefreshSchemaThread)
                                    return;

                                RefreshSchemaCmd cmd = null;

                                try
                                {
                                    while (_commandsRefreshSchemaThread.Count > 0)
                                    {
                                        cmd = _commandsRefreshSchemaThread.Dequeue();
                                        Monitor.Exit(_notifyRefreshSchemaThread);
                                        try
                                        {
                                            WaitForSchemaAgreement();
                                            RefreshSchema(cmd.Keyspace, cmd.Table);
                                        }
                                        finally
                                        {
                                            Monitor.Enter(_notifyRefreshSchemaThread);
                                        }
                                        if (_endOfRefreshSchemaThread)
                                            return;
                                        if (cmd.AsyncResult != null)
                                            cmd.AsyncResult.Complete();
                                    }
                                }
                                catch (NoHostAvailableException)
                                {
                                    if (cmd != null && cmd.AsyncResult != null)
                                        _commandsRefreshSchemaThread.Enqueue(cmd);
                                    //var newQ = new Queue<RefreshSchemaCmd>();
                                    //while (_commandsRefreshSchemaThread.Count > 0)
                                    //{
                                    //    var nc = _commandsRefreshSchemaThread.Dequeue();
                                    //    if (nc.AsyncResult != null)
                                    //        newQ.Enqueue(nc);
                                    //}
                                    //_commandsRefreshSchemaThread = newQ;
                                    _isDiconnected = true;
                                    _hostsIter = null;
                                    _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                                }
                                catch (Exception ex)
                                {
                                    if (CassandraConnection.IsStreamRelatedException(ex))
                                    {
                                        if (cmd != null && cmd.AsyncResult != null)
                                            _commandsRefreshSchemaThread.Enqueue(cmd);
                                        //var newQ = new Queue<RefreshSchemaCmd>();
                                        //while (_commandsRefreshSchemaThread.Count > 0)
                                        //{
                                        //    var nc = _commandsRefreshSchemaThread.Dequeue();
                                        //    if (nc.AsyncResult != null)
                                        //        newQ.Enqueue(nc);
                                        //}
                                        //_commandsRefreshSchemaThread = newQ;
                                        _isDiconnected = true;
                                        _hostsIter = null;
                                        _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                                    }
                                    else
                                    {
                                        if (cmd != null && cmd.AsyncResult != null)
                                            cmd.AsyncResult.Complete(ex);
                                    }
                                }
                                Monitor.Wait(_notifyRefreshSchemaThread);
                            }
                        }
                    }
                    finally
                    {
                        while (_commandsRefreshSchemaThread.Count > 0)
                        {
                            var cmd = _commandsRefreshSchemaThread.Dequeue();
                            if(cmd.AsyncResult!=null)
                                cmd.AsyncResult.Complete(new ObjectDisposedException("Session object is disposed"));
                        }
                    }
                });
            _refreshSchemaThread.Start();
        }

        private void StopRefreshSchemaThread()
        {
            lock (_notifyRefreshSchemaThread)
            {
                _endOfRefreshSchemaThread = true;
                Monitor.PulseAll(_notifyRefreshSchemaThread);
            }
            _refreshSchemaThread.Join();
        }

        internal void SubmitSchemaRefresh(string keyspace, string table, AsyncResultNoResult ar = null)
        {
            lock (_notifyRefreshSchemaThread)
            {
                _commandsRefreshSchemaThread.Enqueue(new RefreshSchemaCmd()
                    {
                        Keyspace = keyspace,
                        Table = table,
                        AsyncResult = ar
                    });
                Monitor.PulseAll(_notifyRefreshSchemaThread);
            }
        }

        private void RefreshSchema(string keyspace, string table)
        {
            bool totalUpdate = _invalidKeyspace.Value;

            _invalidKeyspace.Value = true;

            bool keyspaceUpdate = true;
            ReadOnlyDictionary<string, KeyspaceMetadata> newKeyspaces;
            if (totalUpdate || keyspace == null || _keyspaces.Value == null)
                newKeyspaces = new ReadOnlyDictionary<string, KeyspaceMetadata>();
            else if (keyspace != null && table == null)
                newKeyspaces = new ReadOnlyDictionary<string, KeyspaceMetadata>(_keyspaces.Value);
            else
            {
                newKeyspaces = _keyspaces.Value;
                keyspaceUpdate = false;
            }

            if (keyspaceUpdate)
            {
                using (
                    var rows =
                        _session.Query(
                            string.Format(
                                SelectKeyspaces +
                                ((totalUpdate || keyspace == null || _keyspaces.Value == null)
                                     ? ""
                                     : " WHERE keyspace_name='{0}';"),
                                keyspace)))
                {
                    foreach (var row in rows.GetRows())
                    {
                        var strKsName = row.GetValue<string>("keyspace_name");
                        var strClass = GetStrategyClass(row.GetValue<string>("strategy_class"));
                        var drblWrites = row.GetValue<bool>("durable_writes");
                        var rplctOptions = Utils.ConvertStringToMap(row.GetValue<string>("strategy_options"));

                        var newMetadata = new KeyspaceMetadata(strKsName, drblWrites, strClass,
                                                               new ReadOnlyDictionary<string, int?>(rplctOptions),
                                                               null);

                        newKeyspaces.InternalSetup(strKsName, newMetadata);
                    }
                }
            }
            else
            {
                newKeyspaces[keyspace].Tables.InternalSetup(table, null);
            }

            foreach (var keyspaceMetadata in newKeyspaces.Values)
            {
                if (keyspaceMetadata.Tables == null)
                {
                    var tablesNames = new List<string>();
                    using (
                        var rows =
                            _session.Query(string.Format(SelectColumnFamilies + " WHERE keyspace_name='{0}';",
                                                         keyspace)))
                    {
                        foreach (var row in rows.GetRows())
                            tablesNames.Add(row.GetValue<string>("columnfamily_name"));
                    }

                    keyspaceMetadata.Tables = new ReadOnlyDictionary<string, TableMetadata>();

                    foreach (var tblName in tablesNames)
                        keyspaceMetadata.Tables.InternalSetup(tblName, GetTableMetadata(tblName, keyspace));
                }
                else
                {
                    nextIter:
                    foreach (var tbl in keyspaceMetadata.Tables)
                    {
                        if (tbl.Value == null)
                        {
                            keyspaceMetadata.Tables.InternalSetup(tbl.Key, GetTableMetadata(tbl.Key, keyspace));
                            goto nextIter;
                            ;
                        }
                    }
                }
            }

            _keyspaces.Value = newKeyspaces;

            _invalidKeyspace.Value = false;
        }

        public bool IsSchemaReady
        {
            get
            {
                lock (_notifyRefreshSchemaThread)
                {
                    return _keyspaces.Value != null && !_invalidKeyspace.Value &&
                           _commandsRefreshSchemaThread.Count == 0;
                }
            }
        }

        private readonly AtomicValue<ReadOnlyDictionary<string, KeyspaceMetadata>> _keyspaces =
            new AtomicValue<ReadOnlyDictionary<string, KeyspaceMetadata>>(null);

        public ReadOnlyDictionary<string, KeyspaceMetadata> GetKeyspaces()
        {
            return _keyspaces.Value;
        }

        public StrategyClass GetStrategyClass(string strClass)
        {
            if (strClass != null)
            {
                strClass = strClass.Replace("org.apache.cassandra.locator.", "");
                List<StrategyClass> strategies = new List<StrategyClass>((StrategyClass[])Enum.GetValues(typeof(StrategyClass)));
                foreach (var stratg in strategies)
                    if (strClass == stratg.ToString())
                        return stratg;
            }

            return StrategyClass.Unknown;
        }

        public TableMetadata GetTableMetadata(string tableName, string keyspaceName)
        {
            object[] collectionValuesTypes;
            List<TableMetadata.ColumnDesc> cols = new List<TableMetadata.ColumnDesc>();
            using (
                var rows =
                    _session.Query(
                        string.Format(SelectColumns + " WHERE columnfamily_name='{0}' AND keyspace_name='{1}';",
                                      tableName, keyspaceName)))
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
                            KeyType =
                                row.GetValue<string>("index_name") != null
                                    ? TableMetadata.KeyType.Secondary
                                    : TableMetadata.KeyType.NotAKey,
                        };

                    if (tp_code == TableMetadata.ColumnTypeCode.List)
                        dsc.TypeInfo = new TableMetadata.ListColumnInfo()
                            {
                                ValueTypeCode = (TableMetadata.ColumnTypeCode) collectionValuesTypes[0]
                            };
                    else if (tp_code == TableMetadata.ColumnTypeCode.Map)
                        dsc.TypeInfo = new TableMetadata.MapColumnInfo()
                            {
                                KeyTypeCode = (TableMetadata.ColumnTypeCode) collectionValuesTypes[0],
                                ValueTypeCode = (TableMetadata.ColumnTypeCode) collectionValuesTypes[1]
                            };
                    else if (tp_code == TableMetadata.ColumnTypeCode.Set)
                        dsc.TypeInfo = new TableMetadata.SetColumnInfo()
                            {
                                KeyTypeCode = (TableMetadata.ColumnTypeCode) collectionValuesTypes[0]
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
                foreach (var row in rows.GetRows())
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
                        var rg = new Regex(@"org\.apache\.cassandra\.db\.marshal\.\w+");

                        var rowKeysTypes = rg.Matches(row.GetValue<string>("comparator"));
                        int i = 0;
                        foreach (var keyName in rowKeys)
                        {
                            var tp_code = convertToColumnTypeCode(rowKeysTypes[i + 1].ToString(),
                                                                  out collectionValuesTypes);
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
                                        ValueTypeCode = (TableMetadata.ColumnTypeCode) collectionValuesTypes[0]
                                    };
                            else if (tp_code == TableMetadata.ColumnTypeCode.Map)
                                dsc.TypeInfo = new TableMetadata.MapColumnInfo()
                                    {
                                        KeyTypeCode = (TableMetadata.ColumnTypeCode) collectionValuesTypes[0],
                                        ValueTypeCode = (TableMetadata.ColumnTypeCode) collectionValuesTypes[1]
                                    };
                            else if (tp_code == TableMetadata.ColumnTypeCode.Set)
                                dsc.TypeInfo = new TableMetadata.SetColumnInfo()
                                    {
                                        KeyTypeCode = (TableMetadata.ColumnTypeCode) collectionValuesTypes[0]
                                    };
                            cols.Add(dsc);
                            i++;
                        }
                    }
                    cols.Add(new TableMetadata.ColumnDesc()
                        {
                            ColumnName =
                                row.GetValue<string>("key_aliases")
                                   .Replace("[\"", "")
                                   .Replace("\"]", "")
                                   .Replace("\"\"", "\""),
                            Keyspace = row.GetValue<string>("keyspace_name"),
                            Table = row.GetValue<string>("columnfamily_name"),
                            TypeCode =
                                convertToColumnTypeCode(row.GetValue<string>("key_validator"), out collectionValuesTypes),
                            KeyType = TableMetadata.KeyType.Partition
                        });
                }
            }
            return new TableMetadata() {Name = tableName, Columns = cols.ToArray()};
        }


        private TableMetadata.ColumnTypeCode convertToColumnTypeCode(string type, out object[] collectionValueTp)
        {
            object[] obj;
            collectionValueTp = new object[2];
            if (type.StartsWith("org.apache.cassandra.db.marshal.ListType"))
            {
                collectionValueTp[0] = convertToColumnTypeCode(type.Replace("org.apache.cassandra.db.marshal.ListType(", "").Replace(")", ""), out obj);
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
