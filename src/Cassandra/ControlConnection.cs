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

using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Net.Sockets;

namespace Cassandra
{
    internal class ControlConnection : IDisposable
    {
        private const long MaxSchemaAgreementWaitMs = 10000;
        private const string SelectPeers = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
        private const string SelectLocal = "SELECT * FROM system.local WHERE key='local'";
        private const String SelectKeyspaces = "SELECT * FROM system.schema_keyspaces";
        private const String SelectColumnFamilies = "SELECT * FROM system.schema_columnfamilies";
        private const String SelectColumns = "SELECT * FROM system.schema_columns";
        private const String SelectUdts = "SELECT * FROM system.schema_usertypes";
        private static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);
        /// <summary>
        /// Protocol version used by the control connection
        /// </summary>
        private int _controlConnectionProtocolVersion = 2;

        private readonly AtomicValue<Connection> _activeConnection = new AtomicValue<Connection>(null);
        private readonly Cluster _cluster;
        private readonly IAddressTranslator _addressTranslator;

        private readonly AtomicValue<ConcurrentDictionary<string, AtomicValue<KeyspaceMetadata>>> _keyspaces =
            new AtomicValue<ConcurrentDictionary<string, AtomicValue<KeyspaceMetadata>>>(null);

        private static readonly Logger _logger = new Logger(typeof (ControlConnection));
        private readonly IReconnectionPolicy _reconnectionPolicy = new ExponentialReconnectionPolicy(2*1000, 5*60*1000);
        private readonly IReconnectionSchedule _reconnectionSchedule;
        private readonly Timer _reconnectionTimer;
        private readonly Session _session;
        private readonly BoolSwitch _shutdownSwitch = new BoolSwitch();
        private bool _isDisconnected;
        private readonly object _setupLock = new Object();
        private readonly object _refreshLock = new Object();
        private int _protocolVersion;

        /// <summary>
        /// Gets the recommended binary protocol version to be used for this cluster.
        /// </summary>
        internal int ProtocolVersion
        {
            get
            {
                if (_protocolVersion != 0)
                {
                    return _protocolVersion;
                }
                if (_isDisconnected)
                {
                    return _controlConnectionProtocolVersion;
                }
                return 1;
            }
        }

        internal ControlConnection(Cluster cluster,
                                   Policies policies,
                                   ProtocolOptions protocolOptions,
                                   PoolingOptions poolingOptions,
                                   SocketOptions socketOptions,
                                   ClientOptions clientOptions,
                                   IAuthProvider authProvider,
                                   IAuthInfoProvider authInfoProvider,
                                   IAddressTranslator addressTranslator)
        {
            _cluster = cluster;
            _addressTranslator = addressTranslator;
            _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            _reconnectionTimer = new Timer(ReconnectionClb, null, Timeout.Infinite, Timeout.Infinite);

            var config = new Configuration
            (
                policies,
                protocolOptions,
                poolingOptions,
                socketOptions,
                clientOptions,
                authProvider,
                authInfoProvider,
                new QueryOptions(),
                addressTranslator
            );

            _session = new Session(cluster, config, "", _controlConnectionProtocolVersion);
        }

        public void Dispose()
        {
            Shutdown();
        }

        private void Metadata_HostsEvent(object sender, HostsEventArgs e)
        {
            if (sender == this)
                return;
            if (_activeConnection.Value == null)
                return;

            Action<object> act = _ => SetupControlConnection();
            var address = TranslateAddress(e.Address);

            if (e.What == HostsEventArgs.Kind.Down)
            {
                if (address.Equals(_activeConnection.Value.Address))
                    act.BeginInvoke(null, act.EndInvoke, null);
            }
            else if (e.What == HostsEventArgs.Kind.Up)
            {
                if (_isDisconnected)
                    act.BeginInvoke(null, act.EndInvoke, null);
            }
        }

        /// <summary>
        /// Translates Cassandra node address.
        /// </summary>
        /// <param name="address">Source address.</param>
        /// <returns>Result address.</returns>
        private IPEndPoint TranslateAddress(IPEndPoint address)
        {
            return _addressTranslator.Translate(address) ?? address;
        }

        internal void Init()
        {
            _cluster.Metadata.HostsEvent += Metadata_HostsEvent;

            _session.Init(false);
            SetupControlConnection();
        }

        public void Shutdown(int timeoutMs = Timeout.Infinite)
        {
            if (_shutdownSwitch.TryTake())
            {
                _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _session.WaitForAllPendingActions(timeoutMs);
                _session.Dispose();
            }
        }

        private void SetupEventListener()
        {
            _session.BinaryProtocolVersion = _controlConnectionProtocolVersion;
            var handler = new RequestHandler<RowSet>(_session, null, null);
            Connection connection = null;
            try
            {
                connection = handler.GetNextConnection(null);
            }
            catch (UnsupportedProtocolVersionException)
            {
                _logger.Verbose(String.Format("Unsupported protocol version {0}, trying with a lower version", _controlConnectionProtocolVersion));
                _controlConnectionProtocolVersion--;
                if (_controlConnectionProtocolVersion < 1)
                {
                    throw new DriverInternalError("Invalid protocol version");
                }
                SetupEventListener();
                return;
            }
            _activeConnection.Value = connection;
            _activeConnection.Value.CassandraEventResponse += conn_CassandraEvent;
            const CassandraEventType eventTypes = CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange;
            var registerTask = _activeConnection.Value.Send(new RegisterForEventRequest(_controlConnectionProtocolVersion, eventTypes));
            TaskHelper.WaitToComplete(registerTask, 10000);
            if (!(registerTask.Result is ReadyResponse))
            {
                throw new DriverInternalError("Expected ReadyResponse, obtained " + registerTask.Result.GetType().Name);
            }
        }

        private void conn_CassandraEvent(object sender, CassandraEventArgs e)
        {
            var act = new Action<object>(_ =>
            {
                if (e is TopologyChangeEventArgs)
                {
                    var tce = e as TopologyChangeEventArgs;
                    var address = TranslateAddress(tce.Address);

                    if (tce.What == TopologyChangeEventArgs.Reason.NewNode)
                    {
                        SetupControlConnection(true);
                        _cluster.Metadata.AddHost(address);
                        return;
                    }
                    if (tce.What == TopologyChangeEventArgs.Reason.RemovedNode)
                    {
                        _cluster.Metadata.RemoveHost(address);
                        SetupControlConnection(_activeConnection.Value != null && !address.Equals(_activeConnection.Value.Address));
                        return;
                    }
                }
                else if (e is StatusChangeEventArgs)
                {
                    var sce = e as StatusChangeEventArgs;
                    var address = TranslateAddress(sce.Address);

                    if (sce.What == StatusChangeEventArgs.Reason.Up)
                    {
                        _cluster.Metadata.BringUpHost(address, this);
                        return;
                    }
                    if (sce.What == StatusChangeEventArgs.Reason.Down)
                    {
                        _cluster.Metadata.SetDownHost(address, this);
                        return;
                    }
                }
                else if (e is SchemaChangeEventArgs)
                {
                    var ssc = e as SchemaChangeEventArgs;

                    if (ssc.What == SchemaChangeEventArgs.Reason.Created)
                    {
                        SubmitSchemaRefresh(string.IsNullOrEmpty(ssc.Keyspace) ? null : ssc.Keyspace, null);
                        _cluster.Metadata.FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Created,
                                                                 string.IsNullOrEmpty(ssc.Keyspace) ? null : ssc.Keyspace, ssc.Table);
                        return;
                    }
                    if (ssc.What == SchemaChangeEventArgs.Reason.Dropped)
                    {
                        SubmitSchemaRefresh(string.IsNullOrEmpty(ssc.Keyspace) ? null : ssc.Keyspace, null);
                        _cluster.Metadata.FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Dropped,
                                                                 string.IsNullOrEmpty(ssc.Keyspace) ? null : ssc.Keyspace, ssc.Table);
                        return;
                    }
                    if (ssc.What == SchemaChangeEventArgs.Reason.Updated)
                    {
                        SubmitSchemaRefresh(ssc.Keyspace, string.IsNullOrEmpty(ssc.Table) ? null : ssc.Table);
                        _cluster.Metadata.FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Updated,
                                                                 string.IsNullOrEmpty(ssc.Keyspace) ? null : ssc.Keyspace, ssc.Table);
                        return;
                    }
                }

                var ex = new DriverInternalError("Unknown Event");
                _logger.Error(ex);
                throw ex;
            });
            act.BeginInvoke(null, act.EndInvoke, null);
        }

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

        internal bool RefreshHosts()
        {
            lock (_refreshLock)
            {
                try
                {
                    if (!_isDisconnected)
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
                catch (SocketException)
                {
                    _logger.Error("ControlConnection is lost now.");
                    return false;
                }
                catch (Exception ex)
                {
                    _logger.Error("Unexpected error occurred during forced ControlConnection refresh.", ex);
                    throw;
                }
            }
        }

        private void SetupControlConnection(bool refreshOnly = false)
        {
            lock (_setupLock)
            {
                try
                {
                    _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    _logger.Info("Refreshing ControlConnection...");
                    if (!refreshOnly)
                    {
                        SetupEventListener();
                    }
                    RefreshNodeListAndTokenMap();
                    _isDisconnected = false;
                    _logger.Info("ControlConnection is listening, using binary protocol version " + _controlConnectionProtocolVersion);
                }
                catch (NoHostAvailableException)
                {
                    _isDisconnected = true;
                    if (!_shutdownSwitch.IsTaken())
                    {
                        _logger.Error("ControlConnection is lost. Retrying..");
                        _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                    }
                }
                catch (SocketException)
                {
                    _isDisconnected = true;
                    if (!_shutdownSwitch.IsTaken())
                    {
                        _logger.Error("ControlConnection is lost. Retrying..");
                        _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                    }
                }
                catch (Exception ex)
                {
                    _isDisconnected = true;
                    _logger.Error("Unexpected error occurred during ControlConnection refresh.", ex);
                }
            }
        }

        // schema

        private void RefreshNodeListAndTokenMap()
        {
            _logger.Info("Refreshing NodeList and TokenMap..");
            // Make sure we're up to date on nodes and tokens            
            var tokenMap = new Dictionary<IPEndPoint, HashSet<string>>();
            string partitioner = null;

            var foundHosts = new List<IPEndPoint>();
            var dcs = new List<string>();
            var racks = new List<string>();
            var allTokens = new List<HashSet<string>>();
            {
                var rowset = Query(SelectPeers);
                
                foreach (Row row in rowset.GetRows())
                {
                    IPAddress address = null;
                    if (!row.IsNull("rpc_address"))
                        address = row.GetValue<IPAddress>("rpc_address");
                    if (address == null)
                    {
                        if (!row.IsNull("peer"))
                            address = row.GetValue<IPAddress>("peer");
                        _logger.Error("No rpc_address found for host in peers system table. ");
                    }
                    else if (address.Equals(BindAllAddress))
                    {
                        if (!row.IsNull("peer"))
                            address = row.GetValue<IPAddress>("peer");
                    }

                    if (address != null)
                    {
                        // Translate discovered node address
                        var originalAddress = new IPEndPoint(address, ProtocolOptions.DefaultPort);
                        var translatedAddress = TranslateAddress(originalAddress);

                        foundHosts.Add(translatedAddress);
                        dcs.Add(row.GetValue<string>("data_center"));
                        racks.Add(row.GetValue<string>("rack"));
                        var col = row.GetValue<IEnumerable<string>>("tokens");
                        if (col == null)
                            allTokens.Add(new HashSet<string>());
                        else
                            allTokens.Add(new HashSet<string>(col));
                    }
                }
            }
            {
                Host localhost = _cluster.Metadata.GetHost(_activeConnection.Value.Address);

                var rowset = Query(SelectLocal);
                // Update cluster name, DC and rack for the one node we are connected to
                var localRow = rowset.First();
                var clusterName = localRow.GetValue<string>("cluster_name");
                if (clusterName != null)
                {
                    _cluster.Metadata.ClusterName = clusterName;
                }
                int protocolVersion;
                if (rowset.Columns.Any(c => c.Name == "native_protocol_version") && 
                    Int32.TryParse(localRow.GetValue<string>("native_protocol_version"), out protocolVersion))
                {
                    //In Cassandra < 2
                    //  there is no native protocol version column, it will get the default value
                    _protocolVersion = protocolVersion;
                }
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
                            tokenMap.Add(localhost.Address, new HashSet<string>());
                        tokenMap[localhost.Address].UnionWith(tokens);
                    }
                }
            }

            for (int i = 0; i < foundHosts.Count; i++)
            {
                Host host = _cluster.Metadata.GetHost(foundHosts[i]);
                if (host == null)
                {
                    // We don't know that node, add it.
                    host = _cluster.Metadata.AddHost(foundHosts[i]);
                }
                host.SetLocationInfo(dcs[i], racks[i]);

                if (partitioner != null && allTokens[i].Count != 0)
                    tokenMap.Add(host.Address, allTokens[i]);
            }

            // Removes all those that seems to have been removed (since we lost the control connection)
            var foundHostsSet = new HashSet<IPEndPoint>(foundHosts);
            foreach (IPEndPoint host in _cluster.Metadata.AllReplicas())
                if (!host.Equals(_activeConnection.Value.Address) && !foundHostsSet.Contains(host))
                    _cluster.Metadata.RemoveHost(host);

            if (partitioner != null)
                _cluster.Metadata.RebuildTokenMap(partitioner, tokenMap);

            _logger.Info("NodeList and TokenMap have been successfully refreshed!");
        }

        private RowSet Query(string cqlQuery)
        {
            var request = new QueryRequest(_controlConnectionProtocolVersion, cqlQuery, false, QueryProtocolOptions.Default);
            var task = _activeConnection.Value.Send(request);
            TaskHelper.WaitToComplete(task, 10000);
            if (!(task.Result is ResultResponse) && !(((ResultResponse)task.Result).Output is OutputRows))
            {
                throw new DriverInternalError("Expected rows " + task.Result);
            }
            return ((task.Result as ResultResponse).Output as OutputRows).RowSet;
        }

        private bool WaitForSchemaAgreement()
        {
            DateTimeOffset start = DateTimeOffset.Now;
            long elapsed = 0;
            while (elapsed < MaxSchemaAgreementWaitMs)
            {
                var versions = new HashSet<Guid>();
                {
                    var rowset = Query(CqlQueryTools.SelectSchemaPeers);
                    foreach (Row row in rowset.GetRows())
                    {
                        if (row.IsNull("rpc_address") || row.IsNull("schema_version"))
                            continue;

                        IPAddress address = row.GetValue<IPAddress>("rpc_address");
                        if (address.Equals(BindAllAddress) && !row.IsNull("peer"))
                            address = row.GetValue<IPAddress>("peer");

                        var originalAddress = new IPEndPoint(address, ProtocolOptions.DefaultPort);
                        var translatedAddress = TranslateAddress(originalAddress);

                        Host peer = _cluster.Metadata.GetHost(translatedAddress);
                        if (peer != null && peer.IsConsiderablyUp)
                            versions.Add(row.GetValue<Guid>("schema_version"));
                    }
                }

                {
                    var rowset = Query(CqlQueryTools.SelectSchemaLocal);
                    // Update cluster name, DC and rack for the one node we are connected to
                    foreach (Row localRow in rowset.GetRows())
                    {
                        if (!localRow.IsNull("schema_version"))
                        {
                            versions.Add(localRow.GetValue<Guid>("schema_version"));
                            break;
                        }
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

        internal void SubmitSchemaRefresh(string keyspace, string table)
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

        private ConcurrentDictionary<string, AtomicValue<KeyspaceMetadata>> SetupSchema()
        {
            ConcurrentDictionary<string, AtomicValue<KeyspaceMetadata>> ks = _keyspaces.Value;
            if (ks == null)
            {
                var newKeyspaces = new ConcurrentDictionary<string, AtomicValue<KeyspaceMetadata>>();
                var rows = Query(SelectKeyspaces);
                foreach (Row row in rows)
                {
                    var strKsName = row.GetValue<string>("keyspace_name");
                    string strClass = GetStrategyClass(row.GetValue<string>("strategy_class"));
                    var drblWrites = row.GetValue<bool>("durable_writes");
                    IDictionary<string, int> rplctOptions = Utils.ConvertStringToMapInt(row.GetValue<string>("strategy_options"));

                    var newMetadata = new KeyspaceMetadata(this, strKsName, drblWrites, strClass, rplctOptions);

                    newKeyspaces.TryAdd(strKsName, new AtomicValue<KeyspaceMetadata>(newMetadata));
                }
                return _keyspaces.Value = newKeyspaces;
            }
            return ks;
        }

        private void ResetKeyspace(string keyspace)
        {
            ConcurrentDictionary<string, AtomicValue<KeyspaceMetadata>> ks = _keyspaces.Value;
            if (ks != null)
            {
                AtomicValue<KeyspaceMetadata> value;
                if (ks.TryGetValue(keyspace, out value))
                    value.Value = null;
            }
        }

        private KeyspaceMetadata SetupKeyspace(string keyspace)
        {
            ConcurrentDictionary<string, AtomicValue<KeyspaceMetadata>> sc = SetupSchema();
            AtomicValue<KeyspaceMetadata> ksval;
            if (!sc.TryGetValue(keyspace, out ksval) || ksval.Value == null || ksval.Value.Tables.Value == null)
            {
                WaitForSchemaAgreement();
                ResetSchema();
                sc = SetupSchema();
                KeyspaceMetadata ks = null;

                {
                    var cqlQuery = String.Format(SelectKeyspaces + " WHERE keyspace_name='{0}';", keyspace);
                    var rows = Query(cqlQuery);
                    foreach (Row row in rows)
                    {
                        var strKsName = row.GetValue<string>("keyspace_name");
                        string strClass = GetStrategyClass(row.GetValue<string>("strategy_class"));
                        var drblWrites = row.GetValue<bool>("durable_writes");
                        IDictionary<string, int> rplctOptions = Utils.ConvertStringToMapInt(row.GetValue<string>("strategy_options"));

                        ks = new KeyspaceMetadata(this, strKsName, drblWrites, strClass, rplctOptions);
                    }
                    if (ks == null)
                        throw new InvalidOperationException();
                }

                {
                    var ktb = new ConcurrentDictionary<string, AtomicValue<TableMetadata>>();
                    var cqlQuery = String.Format(SelectColumnFamilies + " WHERE keyspace_name='{0}';", keyspace);
                    var rows = Query(cqlQuery);
                    foreach (Row row in rows)
                    {
                        ktb.TryAdd(row.GetValue<string>("columnfamily_name"), new AtomicValue<TableMetadata>(null));
                    }
                    ks.Tables.Value = ktb;
                    sc.TryAdd(ks.Name, new AtomicValue<KeyspaceMetadata>(ks));
                    return ks;
                }
            }
            return ksval.Value;
        }


        private void ResetTable(string keyspace, string table)
        {
            ConcurrentDictionary<string, AtomicValue<KeyspaceMetadata>> ks = _keyspaces.Value;

            if (ks != null)
            {
                AtomicValue<KeyspaceMetadata> value;
                if (ks.TryGetValue(keyspace, out value))
                {
                    if (value.Value != null)
                    {
                        ConcurrentDictionary<string, AtomicValue<TableMetadata>> kss = value.Value.Tables.Value;
                        if (kss != null)
                        {
                            AtomicValue<TableMetadata> tabval;
                            if (kss.TryGetValue(table, out tabval))
                                tabval.Value = null;
                        }
                    }
                }
            }
        }

        private TableMetadata SetupTable(string keyspace, string table)
        {
            bool wasc = false;
            RETRY:
            KeyspaceMetadata ks = SetupKeyspace(keyspace);
            ConcurrentDictionary<string, AtomicValue<TableMetadata>> tbl = ks.Tables.Value;
            if (tbl == null)
            {
                goto RETRY;
            }
            AtomicValue<TableMetadata> tblval;
            if (!tbl.TryGetValue(table, out tblval))
            {
                WaitForSchemaAgreement();
                ResetKeyspace(keyspace);
                if (wasc)
                    throw new IndexOutOfRangeException();
                wasc = true;
                goto RETRY;
            }

            if (tblval.Value == null)
            {
                TableMetadata m = GetTableMetadata(table, keyspace);
                tblval.Value = m;
                return m;
            }
            return tblval.Value;
        }

        internal void RefreshSchema(string keyspace, string table)
        {
            if (keyspace == null)
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
                ResetTable(keyspace, table);
                SetupTable(keyspace, table);
            }
        }

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

        /// <summary>
        /// Gets the definition of a User defined type
        /// </summary>
        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            var rs = Query(String.Format(SelectUdts + " WHERE keyspace_name='{0}' AND type_name = '{1}';", keyspace, typeName));
            var row = rs.FirstOrDefault();
            if (row == null)
            {
                return null;
            }
            var udt = new UdtColumnInfo(row.GetValue<string>("keyspace_name") + "." + row.GetValue<string>("type_name"));
            var fieldNames = row.GetValue<List<string>>("field_names");
            var fieldTypes = row.GetValue<List<string>>("field_types");
            if (fieldNames.Count != fieldTypes.Count)
            {
                var ex = new DriverInternalError("Field names and types for UDT do not match");
                _logger.Error(ex);
                throw ex;
            }
            for (var i = 0; i < fieldNames.Count; i++)
            {
                var field = TypeCodec.ParseDataType(fieldTypes[i]);
                field.Name = fieldNames[i];
                udt.Fields.Add(field);
            }
            return udt;
        }

        public string GetStrategyClass(string strClass)
        {
            if (strClass != null)
            {
                if (strClass.StartsWith("org.apache.cassandra.locator."))
                    strClass = strClass.Replace("org.apache.cassandra.locator.", "");
            }
            else
            {
                throw new ArgumentNullException("strClass");
            }

            return strClass;
        }

        private SortedDictionary<string, string> getCompactionStrategyOptions(Row row)
        {
            var result = new SortedDictionary<string, string> {{"class", row.GetValue<string>("compaction_strategy_class")}};
            foreach (KeyValuePair<string, string> entry in Utils.ConvertStringToMap(row.GetValue<string>("compaction_strategy_options")))
                result.Add(entry.Key, entry.Value);
            return result;
        }

        public TableMetadata GetTableMetadata(string tableName, string keyspaceName)
        {
            var cols = new Dictionary<string, TableColumn>();
            TableOptions options = null;
            {
                var cqlQuery = string.Format(SelectColumns + " WHERE columnfamily_name='{0}' AND keyspace_name='{1}';", tableName, keyspaceName);
                var rows = Query(cqlQuery);
                foreach (var row in rows)
                {
                    var dataType = TypeCodec.ParseDataType(row.GetValue<string>("validator"));
                    var dsc = new TableColumn
                    {
                        Name = row.GetValue<string>("column_name"),
                        Keyspace = row.GetValue<string>("keyspace_name"),
                        Table = row.GetValue<string>("columnfamily_name"),
                        TypeCode = dataType.TypeCode,
                        TypeInfo = dataType.TypeInfo,
                        SecondaryIndexName = row.GetValue<string>("index_name"),
                        SecondaryIndexType = row.GetValue<string>("index_type"),
                        KeyType =
                            row.GetValue<string>("index_name") != null
                                ? KeyType.SecondaryIndex
                                : KeyType.None,
                    };

                    cols.Add(dsc.Name, dsc);
                }
            }
            {
                var cqlQuery = string.Format(SelectColumnFamilies + " WHERE columnfamily_name='{0}' AND keyspace_name='{1}';", tableName, keyspaceName);
                var rows = Query(cqlQuery);
                var row = rows.First();
                var colNames = row.GetValue<string>("column_aliases");
                var rowKeys = colNames.Substring(1, colNames.Length - 2).Split(',');
                for (var i = 0; i < rowKeys.Length; i++)
                {
                    if (rowKeys[i].StartsWith("\""))
                    {
                        rowKeys[i] = rowKeys[i].Substring(1, rowKeys[i].Length - 2).Replace("\"\"", "\"");
                    }
                }
                //Needs cleanup
                if (rowKeys.Length > 0 && rowKeys[0] != string.Empty)
                {
                    bool isCompact = true;
                    var comparator = row.GetValue<string>("comparator");
                    //Remove reversed type
                    comparator = comparator.Replace(TypeCodec.ReversedTypeName, "");
                    if (comparator.StartsWith(TypeCodec.CompositeTypeName))
                    {
                        comparator = comparator.Replace(TypeCodec.CompositeTypeName, "");
                        isCompact = false;
                    }

                    var rg = new Regex(@"org\.apache\.cassandra\.db\.marshal\.\w+");
                    MatchCollection rowKeysTypes = rg.Matches(comparator);

                    int i = 0;
                    foreach (var keyName in rowKeys)
                    {
                        var dataType = TypeCodec.ParseDataType(rowKeysTypes[i].ToString());
                        var dsc = new TableColumn
                        {
                            Name = keyName,
                            Keyspace = row.GetValue<string>("keyspace_name"),
                            Table = row.GetValue<string>("columnfamily_name"),
                            TypeCode = dataType.TypeCode,
                            TypeInfo = dataType.TypeInfo,
                            KeyType = KeyType.Clustering,
                        };
                        cols[dsc.Name] = dsc;
                        i++;
                    }

                    options = new TableOptions
                    {
                        isCompactStorage = isCompact,
                        bfFpChance = row.GetValue<double>("bloom_filter_fp_chance"),
                        caching = row.GetValue<string>("caching"),
                        comment = row.GetValue<string>("comment"),
                        gcGrace = row.GetValue<int>("gc_grace_seconds"),
                        localReadRepair = row.GetValue<double>("local_read_repair_chance"),
                        readRepair = row.GetValue<double>("read_repair_chance"),
                        compactionOptions = getCompactionStrategyOptions(row),
                        compressionParams =
                            (SortedDictionary<string, string>) Utils.ConvertStringToMap(row.GetValue<string>("compression_parameters"))
                    };
                    //replicate_on_write column not present in C* >= 2.1
                    if (row.GetColumn("replicate_on_write") != null)
                    {
                        options.replicateOnWrite = row.GetValue<bool>("replicate_on_write");
                    }
                }
                //In Cassandra 1.2, the keys are not stored in the system.schema_columns table
                //But you can get it from system.schema_columnfamilies
                var keys = row.GetValue<string>("key_aliases")
                              .Replace("[", "")
                              .Replace("]", "")
                              .Split(',');
                var keyTypes = row.GetValue<string>("key_validator")
                                  .Replace("org.apache.cassandra.db.marshal.CompositeType", "")
                                  .Replace("(", "")
                                  .Replace(")", "")
                                  .Split(',');
                for (var i = 0; i < keys.Length; i++)
                {
                    var name = keys[i].Replace("\"", "").Trim();
                    var dataType = TypeCodec.ParseDataType(keyTypes[i].Trim());
                    cols[name] = new TableColumn()
                    {
                        Name = name,
                        Keyspace = row.GetValue<string>("keyspace_name"),
                        Table = row.GetValue<string>("columnfamily_name"),
                        TypeCode = dataType.TypeCode,
                        TypeInfo = dataType.TypeInfo,
                        KeyType = KeyType.Partition
                    };
                }
            }
            return new TableMetadata(tableName, cols.Values.ToArray(), options);
        }
    }
}