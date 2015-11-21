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
using Cassandra.Tasks;
using Cassandra.Requests;
using Cassandra.Responses;

namespace Cassandra
{
    internal class ControlConnection : IDisposable
    {
        private const string SelectPeers = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
        private const string SelectLocal = "SELECT * FROM system.local WHERE key='local'";
        private const CassandraEventType CassandraEventTypes = CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange;
        private static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);

        private volatile Host _host;
        private volatile Connection _connection;
        // ReSharper disable once InconsistentNaming
        private static readonly Logger _logger = new Logger(typeof (ControlConnection));
        private readonly Configuration _config;
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private IReconnectionSchedule _reconnectionSchedule;
        private readonly Timer _reconnectionTimer;
        private int _isShutdown;
        private int _refreshCounter;
        private Task<bool> _reconnectTask;

        /// <summary>
        /// Gets the recommended binary protocol version to be used for this cluster.
        /// </summary>
        internal byte ProtocolVersion { get; private set; }

        private Metadata Metadata { get; set; }

        internal Host Host
        {
            get { return _host; }
            set { _host = value; }
        }

        /// <summary>
        /// The address of the endpoint used by the ControlConnection
        /// </summary>
        internal IPEndPoint BindAddress
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

        internal ControlConnection(byte initialProtocolVersion, Configuration config, Metadata metadata)
        {
            Metadata = metadata;
            _reconnectionPolicy = config.Policies.ReconnectionPolicy;
            _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            _reconnectionTimer = new Timer(_ => Reconnect(CancellationToken.None).Wait(), null, Timeout.Infinite, Timeout.Infinite);
            _config = config;
            ProtocolVersion = initialProtocolVersion;
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
        internal async Task InitAsync()
        {
            _logger.Info("Trying to connect the ControlConnection");
            //Only abort when twice the time for ConnectTimeout per host passed
            var timeout = _config.SocketOptions.ConnectTimeoutMillis * 2 * Metadata.Hosts.Count;
            await Connect(true, new CancellationTokenSource(timeout).Token);
            try
            {
                SubscribeEventHandlers();
                await RefreshNodeListAsync();
                await Metadata.RefreshKeyspacesAsync(false);
            }
            catch (SocketException ex)
            {
                //There was a problem using the connection obtained
                //It is not usual but can happen
                _logger.Error("An error occurred when trying to retrieve the cluster metadata, retrying.", ex);
                //Retry one more time and throw if there is problem
                
                await Reconnect(new CancellationTokenSource(_config.SocketOptions.ConnectTimeoutMillis).Token);
            }
        }

        /// <summary>
        /// Tries to create the a connection to the cluster
        /// </summary>
        /// <exception cref="NoHostAvailableException" />
        /// <exception cref="DriverInternalError" />
        private Task<bool> Connect(bool firstTime, CancellationToken cancellationToken)
        {
            IEnumerable<Host> hosts = Metadata.Hosts;
            if (!firstTime)
            {
                _logger.Info("Trying to reconnect the ControlConnection");
                //Use the load balancing policy to determine which host to use
                hosts = _config.Policies.LoadBalancingPolicy.NewQueryPlan(null, null);
            }
            return IterateAndConnect(hosts.GetEnumerator(), new Dictionary<IPEndPoint, Exception>(), cancellationToken);
        }

        private async Task<bool> IterateAndConnect(IEnumerator<Host> hostsEnumerator, Dictionary<IPEndPoint, Exception> triedHosts, CancellationToken cancellationToken)
        {
            var available = hostsEnumerator.MoveNext();
            if (!available)
            {
                throw new NoHostAvailableException(triedHosts);
            }
            var host = hostsEnumerator.Current;
            var connection = new Connection(ProtocolVersion, host.Address, _config);

            try
            {
                await connection.Open(cancellationToken);
                _connection = connection;
                _host = host;
                _logger.Info("Connection established to {0}", connection.Address);
                return true;
            }
            catch (UnsupportedProtocolVersionException)
            {
                //Use the protocol version used to parse the response message
                var nextVersion = connection.ProtocolVersion;
                if (nextVersion >= ProtocolVersion)
                {
                    //Processor could reorder instructions in such way that the connection protocol version is not up to date.
                    nextVersion = (byte)(ProtocolVersion - 1);
                }
                _logger.Info(String.Format("Unsupported protocol version {0}, trying with version {1}", ProtocolVersion, nextVersion));
                ProtocolVersion = nextVersion;
                connection.Dispose();
                if (ProtocolVersion < 1)
                {
                    throw new DriverInternalError("Invalid protocol version");
                }
                //Retry using the new protocol version
                return await Connect(true, cancellationToken);
            }
            catch (Exception ex)
            {
                //There was a socket exception or an authentication exception
                triedHosts.Add(host.Address, ex);
                connection.Dispose();
                return await IterateAndConnect(hostsEnumerator, triedHosts, cancellationToken);
            }
        }

        internal async Task<bool> Reconnect(CancellationToken cancellationToken)
        {
            //If there is another thread reconnecting, use the same task
            var tcs = new TaskCompletionSource<bool>();
            var currentTask = Interlocked.CompareExchange(ref _reconnectTask, tcs.Task, null);
            if (currentTask != null)
            {
                return await currentTask;
            }
            Unsubscribe();

            try
            {
                await Connect(false, cancellationToken);
            }
            catch (Exception ex)
            {
                await Interlocked.Exchange(ref _reconnectTask, null);
                tcs.TrySetException(ex);
                var delay = _reconnectionSchedule.NextDelayMs();
                _reconnectionTimer.Change(delay, Timeout.Infinite);
                _logger.Error("ControlConnection was not able to reconnect: " + ex);
            }

            try
            {
                await RefreshNodeListAsync();
                await Metadata.RefreshKeyspacesAsync(false);
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                tcs.TrySetResult(true);
                await Interlocked.Exchange(ref _reconnectTask, null);
                _logger.Info("ControlConnection reconnected to host {0}", _host.Address);
            }
            catch (Exception ex)
            {
                await Interlocked.Exchange(ref _reconnectTask, null);
                _logger.Error("There was an error when trying to refresh the ControlConnection", ex);
                _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                tcs.TrySetException(ex);
            }
            return await tcs.Task;
        }

        internal async Task Refresh()
        {
            if (Interlocked.Increment(ref _refreshCounter) != 1)
            {
                //Only one refresh at a time
                Interlocked.Decrement(ref _refreshCounter);
                return;
            }
            var reconnect = false;
            try
            {
                await RefreshNodeListAsync();
                await Metadata.RefreshKeyspacesAsync(false);
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            }
            catch (SocketException ex)
            {
                _logger.Error("There was a SocketException when trying to refresh the ControlConnection", ex);
                reconnect = true;
            }
            catch (Exception ex)
            {
                _logger.Error("There was an error when trying to refresh the ControlConnection", ex);
            }
            finally
            {
                Interlocked.Decrement(ref _refreshCounter);
            }
            if (reconnect)
            {
                await Reconnect(CancellationToken.None);
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
            var registerTask = _connection.Send(new RegisterForEventRequest(ProtocolVersion, CassandraEventTypes));
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

        private void OnHostDown(Host h, long reconnectionDelay)
        {
            h.Down -= OnHostDown;
            _logger.Warning("Host {0} used by the ControlConnection DOWN", h.Address);
            Task.Factory.StartNew(() => Reconnect(CancellationToken.None), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        private void OnConnectionCassandraEvent(object sender, CassandraEventArgs e)
        {
            //This event is invoked from a worker thread (not a IO thread)
            if (e is TopologyChangeEventArgs)
            {
                var tce = (TopologyChangeEventArgs)e;
                if (tce.What == TopologyChangeEventArgs.Reason.NewNode || tce.What == TopologyChangeEventArgs.Reason.RemovedNode)
                {
                    Refresh().Wait();
                    return;
                }
            }
            if (e is StatusChangeEventArgs)
            {
                var sce = (StatusChangeEventArgs)e;
                //The address in the Cassandra event message needs to be translated
                var address = TranslateAddress(sce.Address);
                _logger.Info("Received Node status change event: host {0} is {1}", address, sce.What.ToString().ToUpper());
                if (sce.What == StatusChangeEventArgs.Reason.Up)
                {
                    Metadata.BringUpHost(address, this);
                    return;
                }
                if (sce.What == StatusChangeEventArgs.Reason.Down)
                {
                    Metadata.SetDownHost(address, this);
                    return;
                }
            }
            if (e is SchemaChangeEventArgs)
            {
                var ssc = (SchemaChangeEventArgs)e;
                if (!String.IsNullOrEmpty(ssc.Table))
                {
                    Metadata.RefreshTable(ssc.Keyspace, ssc.Table);
                    return;
                }
                if (ssc.FunctionName != null)
                {
                    Metadata.ClearFunction(ssc.Keyspace, ssc.FunctionName, ssc.Signature);
                    return;
                }
                if (ssc.AggregateName != null)
                {
                    Metadata.ClearAggregate(ssc.Keyspace, ssc.AggregateName, ssc.Signature);
                    return;
                }
                if (ssc.Type != null)
                {
                    return;
                }
                if (ssc.What == SchemaChangeEventArgs.Reason.Dropped)
                {
                    Metadata.RemoveKeyspace(ssc.Keyspace);
                    return;
                }
                Metadata.RefreshSingleKeyspace(ssc.What == SchemaChangeEventArgs.Reason.Created, ssc.Keyspace);
            }
        }

        private IPEndPoint TranslateAddress(IPEndPoint value)
        {
            return _config.AddressTranslator.Translate(value);
        }

        private async Task RefreshNodeListAsync()
        {
            _logger.Info("Refreshing node list");
            var localRow = (await QueryAsync(SelectLocal)).FirstOrDefault();
            var rsPeers = await QueryAsync(SelectPeers);
            if (localRow == null)
            {
                _logger.Error("Local host metadata could not be retrieved");
                return;
            }
            Metadata.Partitioner = localRow.GetValue<string>("partitioner");
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
            var foundPeers = new HashSet<IPEndPoint>();
            foreach (var row in rs)
            {
                var address = GetAddressForPeerHost(row, _config.AddressTranslator, _config.ProtocolOptions.Port);
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
        internal static IPEndPoint GetAddressForPeerHost(Row row, IAddressTranslator translator, int port)
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

            return translator.Translate(new IPEndPoint(address, port));
        }

        public RowSet Query(string cqlQuery, bool retry = false)
        {
            return QueryAsync(cqlQuery, retry).WaitToComplete();
        }

        /// <summary>
        /// Uses the active connection to execute a query
        /// </summary>
        public async Task<RowSet> QueryAsync(string cqlQuery, bool retry = false)
        {
            var request = new QueryRequest(ProtocolVersion, cqlQuery, false, QueryProtocolOptions.Default);
            Response response;
            try
            {
                response = await _connection.Send(request, new CancellationTokenSource(10000).Token);
            }
            catch (SocketException ex)
            {
                const string message = "There was an error while executing on the host {0} the query '{1}'";
                _logger.Error(string.Format(message, cqlQuery, _connection.Address), ex);
                if (retry)
                {
                    //Try to connect to another host
                    await Reconnect(new CancellationTokenSource(_config.SocketOptions.ConnectTimeoutMillis).Token);
                    //Try to execute again without retry
                    return Query(cqlQuery, false);
                }
                throw;
            }
            return GetRowSet(response);
        }

        /// <summary>
        /// Validates that the result contains a RowSet and returns it.
        /// </summary>
        /// <exception cref="NullReferenceException" />
        /// <exception cref="DriverInternalError" />
        public static RowSet GetRowSet(Response response)
        {
            if (response == null)
            {
                throw new NullReferenceException("Response can not be null");
            }
            if (!(response is ResultResponse))
            {
                throw new DriverInternalError("Expected rows, obtained " + response.GetType().FullName);
            }
            var result = (ResultResponse) response;
            if (!(result.Output is OutputRows))
            {
                throw new DriverInternalError("Expected rows output, obtained " + result.Output.GetType().FullName);
            }
            return ((OutputRows) result.Output).RowSet;
        }
    }
}
