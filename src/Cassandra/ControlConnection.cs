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
        private readonly Configuration _config;
        private readonly IReconnectionPolicy _reconnectionPolicy = new ExponentialReconnectionPolicy(2*1000, 5*60*1000);
        private IReconnectionSchedule _reconnectionSchedule;
        private readonly Timer _reconnectionTimer;
        private int _isShutdown = 0;
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
                return _controlConnectionProtocolVersion;
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

        internal ControlConnection(ICluster cluster, Metadata metadata)
        {
            Metadata = metadata;
            _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            _reconnectionTimer = new Timer(_ => Refresh(true), null, Timeout.Infinite, Timeout.Infinite);
            _config = cluster.Configuration;
        }

        public void Dispose()
        {
            Shutdown();
        }

        /// <summary>
        /// Tries to create a connection to any of the contact points and retrieve cluster metadata for the first time. Not thread-safe.
        /// </summary>
        /// <exception cref="NoHostAvailableException" />
        /// <exception cref="DriverInternalError" />
        internal void Init()
        {
            _logger.Info("Trying to connect the ControlConnection");
            Connect(true);
            try
            {
                SubscribeEventHandlers();
                RefreshNodeList();
                Metadata.RefreshKeyspaces(false);
            }
            catch (SocketException ex)
            {
                //There was a problem using the connection obtained
                //It is not usual but can happen
                _logger.Error("An error occurred when trying to retrieve the cluster metadata, retrying.", ex);
                //Retry one more time and throw if there is  problem
                Refresh(true, true);
            }
        }

        /// <summary>
        /// Tries to create the a connection to the cluster
        /// </summary>
        /// <exception cref="NoHostAvailableException" />
        /// <exception cref="DriverInternalError" />
        private void Connect(bool firstTime)
        {
            var triedHosts = new Dictionary<IPAddress, Exception>();
            IEnumerable<Host> hostIterator = Metadata.Hosts;
            if (!firstTime)
            {
                _logger.Info("Trying to reconnect the ControlConnection");
                //Use the load balancing policy to determine which host to use
                hostIterator = _config.Policies.LoadBalancingPolicy.NewQueryPlan(null, null);
            }
            foreach (var host in hostIterator)
            {
                var address = new IPEndPoint(host.Address, _config.ProtocolOptions.Port);
                var c = new Connection((byte)_controlConnectionProtocolVersion, address, _config);
                try
                {
                    c.Init();
                    _connection = c;
                    _host = host;
                    return;
                }
                catch (UnsupportedProtocolVersionException)
                {
                    _logger.Info(String.Format("Unsupported protocol version {0}, trying with a lower version", _controlConnectionProtocolVersion));
                    _controlConnectionProtocolVersion--;
                    c.Dispose();
                    if (_controlConnectionProtocolVersion < 1)
                    {
                        throw new DriverInternalError("Invalid protocol version");
                    }
                    //Retry using the new protocol version
                    Connect(firstTime);
                    return;
                }
                catch (Exception ex)
                {
                    //There was a socket exception or an authentication exception
                    triedHosts.Add(host.Address, ex);
                    c.Dispose();
                }
            }
            throw new NoHostAvailableException(triedHosts);
        }

        internal void Refresh(bool reconnect = false, bool throwExceptions = false)
        {
            lock (_refreshLock)
            {
                try
                {
                    if (reconnect)
                    {
                        Unsubscribe();
                        Connect(false);
                        SubscribeEventHandlers();
                    }
                    RefreshNodeList();
                    Metadata.RefreshKeyspaces(false);
                    _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                }
                catch (Exception ex)
                {
                    _logger.Error("There was an error when trying to refresh the ControlConnection", ex);
                    _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                    if (throwExceptions)
                    {
                        throw;
                    }
                }
            }
        }

        public void Shutdown()
        {
            if (Interlocked.Increment(ref _isShutdown) != 1)
            {
                //Only shutdown once
                return;
            }
            var c = _connection;
            if (c != null)
            {
                c.Dispose();
            }
            _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
            _reconnectionTimer.Dispose();
        }

        /// <summary>
        /// Gets the next connection and setup the event listener for the host and connection.
        /// Not thread-safe.
        /// </summary>
        private void SubscribeEventHandlers()
        {
            _host.Down += OnHostDown;
            _connection.CassandraEventResponse += OnConnectionCassandraEvent;
            //Register to events on the connection
            var registerTask = _connection.Send(new RegisterForEventRequest(_controlConnectionProtocolVersion, CassandraEventTypes));
            TaskHelper.WaitToComplete(registerTask, 10000);
            if (!(registerTask.Result is ReadyResponse))
            {
                throw new DriverInternalError("Expected ReadyResponse, obtained " + registerTask.Result.GetType().Name);
            }
        }

        private void Unsubscribe()
        {
            var c = _connection;
            var h = _host;
            if (c != null)
            {
                c.CassandraEventResponse -= OnConnectionCassandraEvent;
            }
            if (h != null)
            {
                h.Down -= OnHostDown;
            }
        }

        private void OnHostDown(Host h, DateTimeOffset nextUpTime)
        {
            h.Down -= OnHostDown;
            _logger.Warning("Host " + h.Address + " used by the ControlConnection DOWN");

            Task.Factory.StartNew(() => Refresh(true), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
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
                        Refresh(false);
                        return;
                    }
                    if (tce.What == TopologyChangeEventArgs.Reason.RemovedNode)
                    {
                        Refresh(false);
                        return;
                    }
                }
                if (e is StatusChangeEventArgs)
                {
                    //Translate address
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
                        Metadata.RefreshTable(ssc.Keyspace, ssc.Table);
                        return;
                    }
                    if (ssc.What == SchemaChangeEventArgs.Reason.Dropped)
                    {
                        Metadata.RemoveKeyspace(ssc.Keyspace);
                        return;
                    }
                    Metadata.RefreshSingleKeyspace(ssc.What == SchemaChangeEventArgs.Reason.Created, ssc.Keyspace);
                }
            }, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        private void RefreshNodeList()
        {
            _logger.Info("Refreshing node list");
            var localRow = Query(SelectLocal).FirstOrDefault();
            var rsPeers = Query(SelectPeers);
            if (localRow == null)
            {
                _logger.Error("Local host metadata could not be retrieved");
                return;
            }
            Metadata.Partitioner = localRow.GetValue<string>("partitioner");
            int protocolVersion;
            if (localRow.GetColumn("native_protocol_version") != null &&
                Int32.TryParse(localRow.GetValue<string>("native_protocol_version"), out protocolVersion))
            {
                //In Cassandra < 2
                //  there is no native protocol version column, it will get the default value
                _protocolVersion = protocolVersion;
            }
            UpdateLocalInfo(localRow);
            UpdatePeersInfo(rsPeers);
            _logger.Info("Node list retrieved successfully");
        }

        internal void UpdateLocalInfo(Row row)
        {
            var localhost = _host;
            // Update cluster name, DC and rack for the one node we are connected to
            var clusterName = row.GetValue<string>("cluster_name");
            if (clusterName != null)
            {
                Metadata.ClusterName = clusterName;
            }
            localhost.SetLocationInfo(row.GetValue<string>("data_center"), row.GetValue<string>("rack"));
            localhost.Tokens = row.GetValue<IEnumerable<string>>("tokens") ?? new string[0];
        }

        internal void UpdatePeersInfo(IEnumerable<Row> rs)
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
                host.Tokens = row.GetValue<IEnumerable<string>>("tokens") ?? new string[0];
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
        public RowSet Query(string cqlQuery, bool retry = false)
        {
            var request = new QueryRequest(_controlConnectionProtocolVersion, cqlQuery, false, QueryProtocolOptions.Default);
            var task = _connection.Send(request);
            try
            {
                TaskHelper.WaitToComplete(task, 10000);
            }
            catch (SocketException ex)
            {
                const string message = "There was an error while executing on the host {0} the query '{1}'";
                _logger.Error(String.Format(message, cqlQuery, _connection.Address), ex);
                if (retry)
                {
                    //Try to connect to another host
                    Refresh(reconnect:true, throwExceptions:true);
                    //Try to execute again without retry
                    return Query(cqlQuery, false);
                }
                else
                {
                    throw;
                }
            }
            if (!(task.Result is ResultResponse) && !(((ResultResponse)task.Result).Output is OutputRows))
            {
                throw new DriverInternalError("Expected rows " + task.Result);
            }
            return (((ResultResponse)task.Result).Output as OutputRows).RowSet;
        }
    }
}
