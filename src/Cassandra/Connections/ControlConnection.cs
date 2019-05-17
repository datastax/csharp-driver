//
//      Copyright DataStax Inc.
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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.ProtocolEvents;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.Tasks;

namespace Cassandra.Connections
{
    internal class ControlConnection : IControlConnection
    {
        private const string SelectPeers = "SELECT * FROM system.peers";
        private const string SelectLocal = "SELECT * FROM system.local WHERE key='local'";
        private const CassandraEventType CassandraEventTypes = CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange;
        private static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);

        private readonly Metadata _metadata;
        private volatile Host _host;
        private volatile IConnection _connection;

        // ReSharper disable once InconsistentNaming
        private static readonly Logger _logger = new Logger(typeof(ControlConnection));

        private readonly Configuration _config;
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private IReconnectionSchedule _reconnectionSchedule;
        private readonly Timer _reconnectionTimer;
        private long _isShutdown;
        private int _refreshFlag;
        private Task<bool> _reconnectTask;
        private readonly Serializer _serializer;
        internal const int MetadataAbortTimeout = 5 * 60000;
        private readonly IProtocolEventDebouncer _eventDebouncer;

        /// <summary>
        /// Gets the binary protocol version to be used for this cluster.
        /// </summary>
        public ProtocolVersion ProtocolVersion => _serializer.ProtocolVersion;

        public Host Host
        {
            get => _host;
            set => _host = value;
        }

        public IPEndPoint Address => _connection?.Address;

        public IPEndPoint LocalAddress => _connection?.LocalAddress;

        public Serializer Serializer => _serializer;

        internal ControlConnection(
            IProtocolEventDebouncer eventDebouncer, 
            ProtocolVersion initialProtocolVersion, 
            Configuration config, 
            Metadata metadata)
        {
            _metadata = metadata;
            _reconnectionPolicy = config.Policies.ReconnectionPolicy;
            _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            _reconnectionTimer = new Timer(ReconnectEventHandler, null, Timeout.Infinite, Timeout.Infinite);
            _config = config;
            _serializer = new Serializer(initialProtocolVersion, config.TypeSerializers);
            _eventDebouncer = eventDebouncer;
        }

        public void Dispose()
        {
            Shutdown();
        }

        /// <summary>
        /// Tries to create a connection to any of the contact points and retrieve cluster metadata for the first time.
        /// Not thread-safe.
        /// </summary>
        /// <exception cref="NoHostAvailableException" />
        /// <exception cref="TimeoutException" />
        /// <exception cref="DriverInternalError" />
        public async Task Init()
        {
            ControlConnection._logger.Info("Trying to connect the ControlConnection");
            await Connect(true).ConfigureAwait(false);
        }

        /// <summary>
        /// Iterates through the query plan or hosts and tries to create a connection.
        /// Once a connection is made, topology metadata is refreshed and the ControlConnection is subscribed to Host
        /// and Connection events.
        /// </summary>
        /// <param name="isInitializing">
        /// Determines whether the ControlConnection is connecting for the first time as part of the initialization.
        /// </param>
        /// <exception cref="NoHostAvailableException" />
        /// <exception cref="DriverInternalError" />
        private async Task Connect(bool isInitializing)
        {
            var hosts = !isInitializing ?
                _config.DefaultRequestOptions.LoadBalancingPolicy.NewQueryPlan(null, null) : GetHostEnumerable();
            var triedHosts = new Dictionary<IPEndPoint, Exception>();

            foreach (var host in hosts)
            {
                var connection = _config.ConnectionFactory.Create(_serializer, host.Address, _config);
                try
                {
                    var version = _serializer.ProtocolVersion;
                    try
                    {
                        await connection.Open().ConfigureAwait(false);
                    }
                    catch (UnsupportedProtocolVersionException ex)
                    {
                        var nextVersion = _serializer.ProtocolVersion;
                        connection = await ChangeProtocolVersion(nextVersion, connection, ex, version)
                            .ConfigureAwait(false);
                    }

                    ControlConnection._logger.Info($"Connection established to {connection.Address} using protocol " +
                                 $"version {_serializer.ProtocolVersion:D}");
                    _connection = connection;
                    _host = host;

                    await RefreshNodeList().ConfigureAwait(false);

                    var commonVersion = ProtocolVersion.GetHighestCommon(_metadata.Hosts);
                    if (commonVersion != _serializer.ProtocolVersion)
                    {
                        // Current connection will be closed and reopened
                        connection = await ChangeProtocolVersion(commonVersion, connection).ConfigureAwait(false);
                        _connection = connection;
                    }

                    await SubscribeToServerEvents(connection).ConfigureAwait(false);
                    await _metadata.RebuildTokenMapAsync(false, _config.MetadataSyncOptions.MetadataSyncEnabled).ConfigureAwait(false);

                    host.Down += OnHostDown;
                    return;
                }
                catch (Exception ex)
                {
                    // There was a socket or authentication exception or an unexpected error
                    // NOTE: A host may appear twice iterating by design, see GetHostEnumerable()
                    triedHosts[host.Address] = ex;
                    connection.Dispose();
                }
            }
            throw new NoHostAvailableException(triedHosts);
        }

        private async Task<IConnection> ChangeProtocolVersion(ProtocolVersion nextVersion, IConnection previousConnection,
                                                 UnsupportedProtocolVersionException ex = null,
                                                 ProtocolVersion? previousVersion = null)
        {
            if (!nextVersion.IsSupported() || nextVersion == previousVersion)
            {
                nextVersion = nextVersion.GetLowerSupported();
            }

            if (nextVersion == 0)
            {
                if (ex != null)
                {
                    // We have downgraded the version until is 0 and none of those are supported
                    throw ex;
                }

                // There was no exception leading to the downgrade, signal internal error
                throw new DriverInternalError("Connection was unable to STARTUP using protocol version 0");
            }

            ControlConnection._logger.Info(ex != null
                ? $"{ex.Message}, trying with version {nextVersion:D}"
                : $"Changing protocol version to {nextVersion:D}");

            _serializer.ProtocolVersion = nextVersion;

            previousConnection.Dispose();

            var c = _config.ConnectionFactory.Create(_serializer, previousConnection.Address, _config);
            await c.Open().ConfigureAwait(false);
            return c;
        }

        private async void ReconnectEventHandler(object state)
        {
            try
            {
                await Reconnect().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ControlConnection._logger.Error("An exception was thrown when reconnecting the control connection.", ex);
            }
        }

        internal async Task<bool> Reconnect()
        {
            var tcs = new TaskCompletionSource<bool>();
            var currentTask = Interlocked.CompareExchange(ref _reconnectTask, tcs.Task, null);
            if (currentTask != null)
            {
                // If there is another thread reconnecting, use the same task
                return await currentTask.ConfigureAwait(false);
            }
            Unsubscribe();
            var oldConnection = _connection;
            try
            {
                ControlConnection._logger.Info("Trying to reconnect the ControlConnection");
                await Connect(false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // It failed to reconnect, schedule the timer for next reconnection and let go.
                var _ = Interlocked.Exchange(ref _reconnectTask, null);
                tcs.TrySetException(ex);
                var delay = _reconnectionSchedule.NextDelayMs();
                ControlConnection._logger.Error("ControlConnection was not able to reconnect: " + ex);
                try
                {
                    _reconnectionTimer.Change((int)delay, Timeout.Infinite);
                }
                catch (ObjectDisposedException)
                {
                    //Control connection is being disposed
                }

                // It will throw the same exception that it was set in the TCS
                throw;
            }
            finally
            {
                if (_connection != oldConnection)
                {
                    oldConnection.Dispose();
                }
            }

            if (Interlocked.Read(ref _isShutdown) > 0L)
            {
                return false;
            }
            try
            {
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                tcs.TrySetResult(true);
                var _ = Interlocked.Exchange(ref _reconnectTask, null);
                ControlConnection._logger.Info("ControlConnection reconnected to host {0}", _host.Address);
            }
            catch (Exception ex)
            {
                var _ = Interlocked.Exchange(ref _reconnectTask, null);
                ControlConnection._logger.Error("There was an error when trying to refresh the ControlConnection", ex);
                tcs.TrySetException(ex);
                try
                {
                    _reconnectionTimer.Change((int)_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                }
                catch (ObjectDisposedException)
                {
                    //Control connection is being disposed
                }
            }
            return await tcs.Task.ConfigureAwait(false);
        }

        private async Task Refresh()
        {
            if (Interlocked.CompareExchange(ref _refreshFlag, 1, 0) != 0)
            {
                // Only one refresh at a time
                return;
            }
            var reconnect = false;
            try
            {
                await RefreshNodeList().ConfigureAwait(false);
                await _metadata.RebuildTokenMapAsync(false, _config.MetadataSyncOptions.MetadataSyncEnabled).ConfigureAwait(false);
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            }
            catch (SocketException ex)
            {
                ControlConnection._logger.Error("There was a SocketException when trying to refresh the ControlConnection", ex);
                reconnect = true;
            }
            catch (Exception ex)
            {
                ControlConnection._logger.Error("There was an error when trying to refresh the ControlConnection", ex);
            }
            finally
            {
                Interlocked.Exchange(ref _refreshFlag, 0);
            }
            if (reconnect)
            {
                await Reconnect().ConfigureAwait(false);
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
                ControlConnection._logger.Info("Shutting down control connection to {0}", c.Address);
                c.Dispose();
            }
            _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
            _reconnectionTimer.Dispose();
        }

        /// <summary>
        /// Gets the next connection and setup the event listener for the host and connection.
        /// </summary>
        /// <exception cref="SocketException" />
        /// <exception cref="DriverInternalError" />
        private async Task SubscribeToServerEvents(IConnection connection)
        {
            connection.CassandraEventResponse += OnConnectionCassandraEvent;
            // Register to events on the connection
            var response = await connection.Send(new RegisterForEventRequest(ControlConnection.CassandraEventTypes))
                                            .ConfigureAwait(false);
            if (!(response is ReadyResponse))
            {
                throw new DriverInternalError("Expected ReadyResponse, obtained " + response?.GetType().Name);
            }
        }

        /// <summary>
        /// Unsubscribe from the current host 'Down' event.
        /// </summary>
        private void Unsubscribe()
        {
            var h = _host;
            if (h != null)
            {
                h.Down -= OnHostDown;
            }
        }

        private void OnHostDown(Host h)
        {
            h.Down -= OnHostDown;
            ControlConnection._logger.Warning("Host {0} used by the ControlConnection DOWN", h.Address);
            // Queue reconnection to occur in the background
            Task.Run(Reconnect).Forget();
        }

        private async void OnConnectionCassandraEvent(object sender, CassandraEventArgs e)
        {
            try
            {
                //This event is invoked from a worker thread (not a IO thread)
                if (e is TopologyChangeEventArgs tce)
                {
                    if (tce.What == TopologyChangeEventArgs.Reason.NewNode || tce.What == TopologyChangeEventArgs.Reason.RemovedNode)
                    {
                        // Start refresh
                        await ScheduleHostsRefreshAsync().ConfigureAwait(false);
                        return;
                    }
                }

                if (e is StatusChangeEventArgs args)
                {
                    HandleStatusChangeEvent(args);
                    return;
                }

                if (e is SchemaChangeEventArgs ssc)
                {
                    await HandleSchemaChangeEvent(ssc, false).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                ControlConnection._logger.Error("Exception thrown while handling cassandra event.", ex);
            }
        }

        /// <inheritdoc />
        public Task HandleSchemaChangeEvent(SchemaChangeEventArgs ssc, bool processNow)
        {
            if (!_config.MetadataSyncOptions.MetadataSyncEnabled)
            {
                return TaskHelper.Completed;
            }

            Func<Task> handler;
            if (!string.IsNullOrEmpty(ssc.Table))
            {
                handler = () =>
                {
                    //Can be either a table or a view
                    _metadata.ClearTable(ssc.Keyspace, ssc.Table);
                    _metadata.ClearView(ssc.Keyspace, ssc.Table);
                    return TaskHelper.Completed;
                };
            }
            else if (ssc.FunctionName != null)
            {
                handler = TaskHelper.ActionToAsync(() => _metadata.ClearFunction(ssc.Keyspace, ssc.FunctionName, ssc.Signature));
            }
            else if (ssc.AggregateName != null)
            {
                handler = TaskHelper.ActionToAsync(() => _metadata.ClearAggregate(ssc.Keyspace, ssc.AggregateName, ssc.Signature));
            }
            else if (ssc.Type != null)
            {
                return TaskHelper.Completed;
            }
            else if (ssc.What == SchemaChangeEventArgs.Reason.Dropped)
            {
                handler = TaskHelper.ActionToAsync(() => _metadata.RemoveKeyspace(ssc.Keyspace));
            }
            else
            {
                return ScheduleKeyspaceRefreshAsync(ssc.Keyspace, processNow);
            }
            
            return ScheduleObjectRefreshAsync(ssc.Keyspace, processNow, handler);
        }

        private void HandleStatusChangeEvent(StatusChangeEventArgs e)
        {
            //The address in the Cassandra event message needs to be translated
            var address = TranslateAddress(e.Address);
            ControlConnection._logger.Info("Received Node status change event: host {0} is {1}", address, e.What.ToString().ToUpper());
            Host host;
            if (!_metadata.Hosts.TryGet(address, out host))
            {
                ControlConnection._logger.Info("Received status change event for host {0} but it was not found", address);
                return;
            }
            var distance = Cluster.RetrieveDistance(host, _config.DefaultRequestOptions.LoadBalancingPolicy);
            if (distance != HostDistance.Ignored)
            {
                // We should not consider events for status changes
                // We should trust the pools.
                return;
            }
            if (e.What == StatusChangeEventArgs.Reason.Up)
            {
                host.BringUpIfDown();
                return;
            }
            host.SetDown();
        }

        private IPEndPoint TranslateAddress(IPEndPoint value)
        {
            return _config.AddressTranslator.Translate(value);
        }

        private async Task RefreshNodeList()
        {
            ControlConnection._logger.Info("Refreshing node list");
            var queriesRs = await Task.WhenAll(QueryAsync(ControlConnection.SelectLocal), QueryAsync(ControlConnection.SelectPeers))
                                      .ConfigureAwait(false);
            var localRow = queriesRs[0].FirstOrDefault();
            var rsPeers = queriesRs[1];

            if (localRow == null)
            {
                ControlConnection._logger.Error("Local host metadata could not be retrieved");
                throw new DriverInternalError("Local host metadata could not be retrieved");
            }

            _metadata.Partitioner = localRow.GetValue<string>("partitioner");
            UpdateLocalInfo(localRow);
            UpdatePeersInfo(rsPeers);
            ControlConnection._logger.Info("Node list retrieved successfully");
        }

        internal void UpdateLocalInfo(Row row)
        {
            var localhost = _host;
            // Update cluster name, DC and rack for the one node we are connected to
            var clusterName = row.GetValue<string>("cluster_name");
            if (clusterName != null)
            {
                _metadata.ClusterName = clusterName;
            }
            localhost.SetInfo(row);
            _metadata.SetCassandraVersion(localhost.CassandraVersion);
        }

        internal void UpdatePeersInfo(IEnumerable<Row> rs)
        {
            var foundPeers = new HashSet<IPEndPoint>();
            foreach (var row in rs)
            {
                var address = ControlConnection.GetAddressForPeerHost(row, _config.AddressTranslator, _config.ProtocolOptions.Port);
                if (address == null)
                {
                    ControlConnection._logger.Error("No address found for host, ignoring it.");
                    continue;
                }
                foundPeers.Add(address);
                var host = _metadata.GetHost(address) ?? _metadata.AddHost(address);
                host.SetInfo(row);
            }

            // Removes all those that seems to have been removed (since we lost the control connection or not valid contact point)
            foreach (var address in _metadata.AllReplicas())
            {
                if (!address.Equals(_host.Address) && !foundPeers.Contains(address))
                {
                    _metadata.RemoveHost(address);
                }
            }
        }

        /// <summary>
        /// Uses system.peers values to build the Address translator
        /// </summary>
        private static IPEndPoint GetAddressForPeerHost(Row row, IAddressTranslator translator, int port)
        {
            var address = row.GetValue<IPAddress>("rpc_address");
            if (address == null)
            {
                return null;
            }
            if (ControlConnection.BindAllAddress.Equals(address) && !row.IsNull("peer"))
            {
                address = row.GetValue<IPAddress>("peer");
                ControlConnection._logger.Warning(String.Format("Found host with 0.0.0.0 as rpc_address, using listen_address ({0}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", address));
            }

            return translator.Translate(new IPEndPoint(address, port));
        }

        /// <summary>
        /// Uses the active connection to execute a query
        /// </summary>
        public IEnumerable<Row> Query(string cqlQuery, bool retry = false)
        {
            return TaskHelper.WaitToComplete(QueryAsync(cqlQuery, retry), ControlConnection.MetadataAbortTimeout);
        }

        public async Task<IEnumerable<Row>> QueryAsync(string cqlQuery, bool retry = false)
        {
            return ControlConnection.GetRowSet(await SendQueryRequestAsync(cqlQuery, retry, QueryProtocolOptions.Default).ConfigureAwait(false));
        }

        public async Task<Response> SendQueryRequestAsync(string cqlQuery, bool retry, QueryProtocolOptions queryProtocolOptions)
        {
            var request = new QueryRequest(ProtocolVersion, cqlQuery, false, queryProtocolOptions);
            Response response;
            try
            {
                response = await _connection.Send(request).ConfigureAwait(false);
            }
            catch (SocketException ex)
            {
                ControlConnection._logger.Error(
                    $"There was an error while executing on the host {cqlQuery} the query '{_connection.Address}'", ex);
                if (!retry)
                {
                    throw;
                }
                // Try reconnect
                await Reconnect().ConfigureAwait(false);
                // Query with retry set to false
                return await SendQueryRequestAsync(cqlQuery, false, queryProtocolOptions).ConfigureAwait(false);
            }
            return response;
        }

        /// <summary>
        /// Validates that the result contains a RowSet and returns it.
        /// </summary>
        /// <exception cref="NullReferenceException" />
        /// <exception cref="DriverInternalError" />
        public static IEnumerable<Row> GetRowSet(Response response)
        {
            if (response == null)
            {
                throw new NullReferenceException("Response can not be null");
            }
            if (!(response is ResultResponse))
            {
                throw new DriverInternalError("Expected rows, obtained " + response.GetType().FullName);
            }
            var result = (ResultResponse)response;
            if (!(result.Output is OutputRows))
            {
                throw new DriverInternalError("Expected rows output, obtained " + result.Output.GetType().FullName);
            }
            return ((OutputRows)result.Output).RowSet;
        }

        /// <summary>
        /// An iterator designed for the underlying collection to change
        /// </summary>
        private IEnumerable<Host> GetHostEnumerable()
        {
            var index = 0;
            var hosts = _metadata.Hosts.ToArray();
            while (index < hosts.Length)
            {
                yield return hosts[index++];
                // Check that the collection changed
                var newHosts = _metadata.Hosts.ToCollection();
                if (newHosts.Count != hosts.Length)
                {
                    index = 0;
                    hosts = newHosts.ToArray();
                }
            }
        }

        /// <inheritdoc />
        public async Task HandleKeyspaceRefreshLaterAsync(string keyspace)
        {
            var @event = new KeyspaceProtocolEvent(true, keyspace, async () =>
            {
                await _metadata.RefreshSingleKeyspace(keyspace).ConfigureAwait(false);
            });
            await _eventDebouncer.HandleEventAsync(@event, false).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task ScheduleKeyspaceRefreshAsync(string keyspace, bool processNow)
        {
            var @event = new KeyspaceProtocolEvent(true, keyspace, () => _metadata.RefreshSingleKeyspace(keyspace));
            return processNow 
                ? _eventDebouncer.HandleEventAsync(@event, true) 
                : _eventDebouncer.ScheduleEventAsync(@event, false);
        }
        
        private Task ScheduleObjectRefreshAsync(string keyspace, bool processNow, Func<Task> handler)
        {
            var @event = new KeyspaceProtocolEvent(false, keyspace, handler);
            return processNow 
                ? _eventDebouncer.HandleEventAsync(@event, true) 
                : _eventDebouncer.ScheduleEventAsync(@event, false);
        }

        private Task ScheduleHostsRefreshAsync()
        {
            return _eventDebouncer.ScheduleEventAsync(new ProtocolEvent(Refresh), false);
        }

        /// <inheritdoc />
        public Task ScheduleAllKeyspacesRefreshAsync(bool processNow)
        {
            var @event = new ProtocolEvent(() => _metadata.RebuildTokenMapAsync(false, true));
            return processNow 
                ? _eventDebouncer.HandleEventAsync(@event, true) 
                : _eventDebouncer.ScheduleEventAsync(@event, false);
        }
    }
}