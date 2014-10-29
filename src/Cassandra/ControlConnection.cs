//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Cassandra
{
    internal class ControlConnection : IDisposable
    {
        private const string SelectPeers = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
        private const string SelectLocal = "SELECT * FROM system.local WHERE key='local'";
        private const CassandraEventType CassandraEventTypes = CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange;
        private static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);
        /// <summary>
        /// Protocol version used by the control connection
        /// </summary>
        private int _controlConnectionProtocolVersion = 2;

        private volatile Host _host;
        private volatile Connection _connection;

        private static readonly Logger _logger = new Logger(typeof (ControlConnection));
        private readonly IReconnectionPolicy _reconnectionPolicy = new ExponentialReconnectionPolicy(2*1000, 5*60*1000);
        private IReconnectionSchedule _reconnectionSchedule;
        private readonly Timer _reconnectionTimer;
        private readonly Session _session;
        private readonly BoolSwitch _shutdownSwitch = new BoolSwitch();
        private bool _isDisconnected;
        private readonly object _setupLock = new Object();
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

        private Metadata Metadata { get; set; }

        internal Host Host
        {
            get { return _host; }
            set { _host = value; }
        }

        /// <summary>
        /// The address of the endpoint used by the ControlConnection
        /// </summary>
        internal IPAddress BindAddress
        {
            get
            {
                if (_connection == null)
                {
                    return null;
                }
                return _connection.Address;
            }
        }

        internal ControlConnection(ICluster cluster, Metadata metadata, Configuration clusterConfig)
        {
            Metadata = metadata;
            _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            _reconnectionTimer = new Timer(ReconnectionClb, null, Timeout.Infinite, Timeout.Infinite);

            var config = GetConfiguration(clusterConfig);

            _session = new Session(cluster, config, null, _controlConnectionProtocolVersion);
        }

        /// <summary>
        /// Gets the ControlConnection settings based on the cluster configuration
        /// </summary>
        private Configuration GetConfiguration(Configuration clusterConfig)
        {
            var policies = new Policies(
                clusterConfig.Policies.LoadBalancingPolicy,
                new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000),
                Policies.DefaultRetryPolicy);
            var protocolOptions = new ProtocolOptions(clusterConfig.ProtocolOptions.Port, clusterConfig.ProtocolOptions.SslOptions);

            var poolingOptions = new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                .SetMaxConnectionsPerHost(HostDistance.Local, 1)
                .SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 0)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 127);
            var clientOptions = new ClientOptions(true, clusterConfig.ClientOptions.QueryAbortTimeout, null);

            return new Configuration
            (
                policies,
                protocolOptions,
                poolingOptions,
                clusterConfig.SocketOptions,
                clientOptions,
                clusterConfig.AuthProvider,
                clusterConfig.AuthInfoProvider,
                new QueryOptions()
            );
        }

        public void Dispose()
        {
            Shutdown();
        }

        internal void Init()
        {
            _session.Init(false);
            SetupControlConnection();
        }

        public void Shutdown(int timeoutMs = Timeout.Infinite)
        {
            if (_shutdownSwitch.TryTake())
            {
                _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _reconnectionTimer.Dispose();
                _session.WaitForAllPendingActions(timeoutMs);
                _session.Dispose();
            }
        }

        /// <summary>
        /// Gets the next connection and setup the event listener for the host and connection.
        /// Not thread-safe.
        /// </summary>
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
            //Only 1 thread at a time here if the caller is locking
            
            //Unsubscribe to previous events
            if (_connection != null)
            {
                _connection.CassandraEventResponse -= OnConnectionCassandraEvent;
            }
            if (_host != null)
            {
                _host.Down -= OnHostDown;
            }

            connection.CassandraEventResponse += OnConnectionCassandraEvent;
            _connection = connection;
            var host = handler.Host;
            host.Down += OnHostDown;
            _host = host;
            //Register to events on the connection
            var registerTask = _connection.Send(new RegisterForEventRequest(_controlConnectionProtocolVersion, CassandraEventTypes));
            TaskHelper.WaitToComplete(registerTask, 10000);
            if (!(registerTask.Result is ReadyResponse))
            {
                throw new DriverInternalError("Expected ReadyResponse, obtained " + registerTask.Result.GetType().Name);
            }
        }

        private void OnHostDown(Host h, DateTimeOffset nextUpTime)
        {
            h.Down -= OnHostDown;
            _logger.Warning("Host " + h.Address + " used by the ControlConnection DOWN");

            Task.Factory.StartNew(() => SetupControlConnection());
        }

        private void OnConnectionCassandraEvent(object sender, CassandraEventArgs e)
        {
            Task.Factory.StartNew(() =>
            {
                if (e is TopologyChangeEventArgs)
                {
                    var tce = e as TopologyChangeEventArgs;
                    if (tce.What == TopologyChangeEventArgs.Reason.NewNode)
                    {
                        SetupControlConnection(true);
                        Metadata.AddHost(tce.Address);
                        return;
                    }
                    if (tce.What == TopologyChangeEventArgs.Reason.RemovedNode)
                    {
                        Metadata.RemoveHost(tce.Address);
                        SetupControlConnection(_connection != null && !tce.Address.Equals(_connection.Address));
                        return;
                    }
                }
                if (e is StatusChangeEventArgs)
                {
                    var sce = e as StatusChangeEventArgs;
                    if (sce.What == StatusChangeEventArgs.Reason.Up)
                    {
                        Metadata.BringUpHost(sce.Address, this);
                        return;
                    }
                    if (sce.What == StatusChangeEventArgs.Reason.Down)
                    {
                        Metadata.SetDownHost(sce.Address, this);
                        return;
                    }
                }
                if (e is SchemaChangeEventArgs)
                {
                    var ssc = e as SchemaChangeEventArgs;
                    if (!String.IsNullOrEmpty(ssc.Table))
                    {
                        RefreshTable(ssc.Keyspace, ssc.Table);
                        return;
                    }
                    if (ssc.What == SchemaChangeEventArgs.Reason.Dropped)
                    {
                        Metadata.RemoveKeyspace(ssc.Keyspace);
                        return;
                    }
                    Metadata.RefreshSingleKeyspace(ssc.What == SchemaChangeEventArgs.Reason.Created, ssc.Keyspace);
                }
            });
        }

        private void RefreshTable(string keyspaceName, string tableName)
        {
            Metadata.RefreshTable(keyspaceName, tableName);
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
                    _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                    _logger.Info("ControlConnection is listening on " + _connection.Address.ToString() + ", using binary protocol version " + _controlConnectionProtocolVersion);
                }
                catch (NoHostAvailableException)
                {
                    _isDisconnected = true;
                    if (!_shutdownSwitch.IsTaken())
                    {
                        _logger.Error("ControlConnection is lost. Retrying.");
                        _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                    }
                }
                catch (SocketException)
                {
                    _isDisconnected = true;
                    if (!_shutdownSwitch.IsTaken())
                    {
                        _logger.Error("ControlConnection is lost. Retrying.");
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

        private void RefreshNodeListAndTokenMap()
        {
            _logger.Info("Refreshing NodeList and TokenMap.");
            var localRow = Query(SelectLocal).FirstOrDefault();
            var rsPeers = Query(SelectPeers);
            if (localRow == null)
            {
                _logger.Error("Local host metadata could not be retrieved");
                return;
            }
            var tokenMap = new Dictionary<Host, HashSet<string>>();
            var partitioner = localRow.GetValue<string>("partitioner");
            int protocolVersion;
            if (localRow.GetColumn("native_protocol_version") != null &&
                Int32.TryParse(localRow.GetValue<string>("native_protocol_version"), out protocolVersion))
            {
                //In Cassandra < 2
                //  there is no native protocol version column, it will get the default value
                _protocolVersion = protocolVersion;
            }
            UpdateLocalInfo(localRow, tokenMap);
            UpdatePeersInfo(rsPeers, tokenMap);
            Metadata.RefreshKeyspaces();
            Metadata.RebuildTokenMap(partitioner, tokenMap);
            _logger.Info("NodeList and TokenMap have been successfully refreshed!");
        }

        internal void UpdateLocalInfo(Row row, IDictionary<Host, HashSet<string>> tokenMap)
        {
            var localhost = _host;
            // Update cluster name, DC and rack for the one node we are connected to
            var clusterName = row.GetValue<string>("cluster_name");
            if (clusterName != null)
            {
                Metadata.ClusterName = clusterName;
            }
            localhost.SetLocationInfo(row.GetValue<string>("data_center"), row.GetValue<string>("rack"));
            var tokens = row.GetValue<IEnumerable<string>>("tokens") ?? new string[0];
            tokenMap.Add(localhost, new HashSet<string>(tokens));
        }

        internal void UpdatePeersInfo(IEnumerable<Row> rs, IDictionary<Host, HashSet<string>> tokenMap)
        {
            var foundPeers = new HashSet<IPAddress>();
            foreach (var row in rs)
            {
                var address = GetAddressForPeerHost(row);
                if (address == null)
                {
                    _logger.Error("No address found for host, ignoring it.");
                    continue;
                }
                foundPeers.Add(address);
                var host = Metadata.GetHost(address);
                if (host == null)
                {
                    host = Metadata.AddHost(address);
                }
                host.SetLocationInfo(row.GetValue<string>("data_center"), row.GetValue<string>("rack"));
                var tokens = row.GetValue<IEnumerable<string>>("tokens") ?? new string[0];
                tokenMap.Add(host, new HashSet<string>(tokens));
            }

            // Removes all those that seems to have been removed (since we lost the control connection or not valid contact point)
            foreach (var address in Metadata.AllReplicas())
            {
                if (!address.Equals(_host.Address) && !foundPeers.Contains(address))
                {
                    Metadata.RemoveHost(address);
                }
            }
        }

        /// <summary>
        /// Uses system.peers values to build the Address translator
        /// </summary>
        internal static IPAddress GetAddressForPeerHost(Row row)
        {
            var address = row.GetValue<IPAddress>("rpc_address");
            if (address == null)
            {
                return null;
            }
            if (BindAllAddress.Equals(address) && !row.IsNull("peer"))
            {
                address = row.GetValue<IPAddress>("peer");
                _logger.Warning(String.Format("Found host with 0.0.0.0 as rpc_address, using listen_address ({0}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", address));
            }
            return address;
        }

        /// <summary>
        /// Uses the active connection to execute a query
        /// </summary>
        public RowSet Query(string cqlQuery)
        {
            var request = new QueryRequest(_controlConnectionProtocolVersion, cqlQuery, false, QueryProtocolOptions.Default);
            var task = _connection.Send(request);
            TaskHelper.WaitToComplete(task, 10000);
            if (!(task.Result is ResultResponse) && !(((ResultResponse)task.Result).Output is OutputRows))
            {
                throw new DriverInternalError("Expected rows " + task.Result);
            }
            return ((task.Result as ResultResponse).Output as OutputRows).RowSet;
        }
    }
}
