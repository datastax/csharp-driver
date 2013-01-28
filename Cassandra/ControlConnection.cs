using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Net;

namespace Cassandra
{
    internal class ControlConnection :IDisposable
    {
        private readonly Session _session;
        private IEnumerator<Host> _hostsIter = null;
        private readonly Cluster _cluster;

        internal ControlConnection(Cluster cluster, 
                                   IEnumerable<IPAddress> clusterEndpoints,
                                   Policies policies,
                                   ProtocolOptions protocolOptions,
                                   PoolingOptions poolingOptions,
                                   SocketOptions socketOptions,
                                   ClientOptions clientOptions,
                                   IAuthInfoProvider authProvider,
                                   bool metricsEnabled)
        {
            this._cluster = cluster;
            this._reconnectionTimer = new Timer(ReconnectionClb, null, Timeout.Infinite, Timeout.Infinite);

            _session = new Session(cluster, clusterEndpoints, policies, protocolOptions, poolingOptions, socketOptions,
                                   clientOptions, authProvider, metricsEnabled, "", _cluster._hosts);
        }

        internal void Init()
        {
            _session.Init();
            CCEvent += new CCEventHandler(ControlConnection_CCEvent);
            Go(true);
        }

        public void Dispose()
        {
            _session.Dispose();
        }

        private void ControlConnection_CCEvent(object sender, ControlConnection.CCEventArgs e)
        {
            if (e.What == CCEventArgs.Kind.Add)
                _session.OnAddHost(e.IPAddress);
            if (e.What == CCEventArgs.Kind.Remove)
                _session.OnRemovedHost(e.IPAddress);
            if (e.What == CCEventArgs.Kind.Down)
                _session.OnDownHost(e.IPAddress);
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

        internal class CCEventArgs : EventArgs
        {
            public enum Kind {Add,Remove,Down}
            public Kind What;
            public IPAddress IPAddress;
        }

        public delegate void CCEventHandler(object sender, CCEventArgs e);
        public event CCEventHandler CCEvent;

        private void conn_CassandraEvent(object sender, CassandraEventArgs e)
        {
            if (e is TopopogyChangeEventArgs)
            {
                var tce = e as TopopogyChangeEventArgs;
                if (tce.What == TopopogyChangeEventArgs.Reason.NewNode)
                {
                    if (CCEvent != null)
                        CCEvent.Invoke(this, new CCEventArgs() {IPAddress = tce.Address, What = CCEventArgs.Kind.Add});
                    CheckConnectionUp(tce.Address);
                    return;
                }
                else if (tce.What == TopopogyChangeEventArgs.Reason.RemovedNode)
                {
                    if (CCEvent != null)
                        CCEvent.Invoke(this, new CCEventArgs() { IPAddress = tce.Address, What = CCEventArgs.Kind.Remove });
                    CheckConnectionDown(tce.Address);
                    return;
                }
            }
            else if (e is StatusChangeEventArgs)
            {
                var sce = e as StatusChangeEventArgs;
                if (sce.What == StatusChangeEventArgs.Reason.Up)
                {
                    if (CCEvent != null)
                        CCEvent.Invoke(this, new CCEventArgs() { IPAddress = sce.Address, What = CCEventArgs.Kind.Add }); 
                    CheckConnectionUp(sce.Address);
                    return;
                }
                else if (sce.What == StatusChangeEventArgs.Reason.Down)
                {
                    if (CCEvent != null)
                        CCEvent.Invoke(this, new CCEventArgs() { IPAddress = sce.Address, What = CCEventArgs.Kind.Down });
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
            Go(true);
        }

        private readonly IReconnectionPolicy _reconnectionPolicy = new ExponentialReconnectionPolicy(2*1000, 5*60*1000);
        private IReconnectionSchedule _reconnectionSchedule = null;

        private CassandraConnection _connection = null;

        private void Go(bool refresh)
        {
            try
            {
                if (_hostsIter == null)
                    _hostsIter = _session._policies.LoadBalancingPolicy.NewQueryPlan(null).GetEnumerator();

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
                        _cluster.Metadata.ClusterName = clusterName;

                    var host = _cluster.Metadata.GetHost(connection.GetHostAdress());
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
                var host = _cluster.Metadata.GetHost(foundHosts[i]);
                if (host == null)
                {
                    // We don't know that node, add it.
                    host = _cluster.Metadata.AddHost(foundHosts[i], _session._policies.ReconnectionPolicy);
                }
                host.SetLocationInfo(dcs[i], racks[i]);

                if (partitioner != null && !allTokens[i].IsEmpty)
                    tokenMap.Add(host.Address, allTokens[i]);
            }

            // Removes all those that seems to have been removed (since we lost the control connection)
            var foundHostsSet = new DictSet<IPAddress>(foundHosts);
            foreach (var host in _cluster.Metadata.AllReplicas())
                if (!host.Equals(connection.GetHostAdress()) && !foundHostsSet.Contains(host))
                    _cluster.Metadata.RemoveHost(host);

            if (partitioner != null)
                _cluster.Metadata.RebuildTokenMap(partitioner, tokenMap);
        }

        private const long MaxSchemaAgreementWaitMs = 10000;

        private const string SelectSchemaPeers = "SELECT peer, schema_version FROM system.peers";
        private const string SelectSchemaLocal = "SELECT schema_version FROM system.local WHERE key='local'";

        internal bool WaitForSchemaAgreement()
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

                        Host peer = _cluster.Metadata.GetHost(row.GetValue<IPEndPoint>("peer").Address);
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
                        var rplctOptions = Utils.ConvertStringToMap(row.GetValue<string>("strategy_options"));

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
                        var rplctOptions = Utils.ConvertStringToMap(row.GetValue<string>("strategy_options"));

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

        public TableMetadata GetTableMetadata(string tableName, string keyspaceName)
        {
            object[] collectionValuesTypes;
            List<TableColumn> cols = new List<TableColumn>();
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
            return new TableMetadata(tableName, cols.ToArray());
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
#if NET_40_OR_GREATER
                case "org.apache.cassandra.db.marshal.DecimalType":
                    return ColumnTypeCode.Decimal;
#endif
                case "org.apache.cassandra.db.marshal.LongType":
                    return ColumnTypeCode.Bigint;
#if NET_40_OR_GREATER
                case "org.apache.cassandra.db.marshal.IntegerType":
                    return ColumnTypeCode.Varint;
#endif
                default: throw new InvalidOperationException();
            }
        }

    }
}
