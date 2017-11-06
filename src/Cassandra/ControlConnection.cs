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
using Cassandra.Serialization;

namespace Cassandra
{
    internal class ControlConnection : IMetadataQueryProvider, IDisposable
    {
        private const string SelectPeers = "SELECT peer, data_center, rack, tokens, rpc_address, release_version FROM system.peers";
        private const string SelectLocal = "SELECT * FROM system.local WHERE key='local'";
        private const CassandraEventType CassandraEventTypes = CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange;
        private static readonly IPAddress BindAllAddress = new IPAddress(new byte[4]);

        private readonly Metadata _metadata;
        private volatile Host _host;
        private volatile Connection _connection;
        // ReSharper disable once InconsistentNaming
        private static readonly Logger _logger = new Logger(typeof (ControlConnection));
        private readonly Configuration _config;
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private IReconnectionSchedule _reconnectionSchedule;
        private readonly Timer _reconnectionTimer;
        private long _isShutdown;
        private int _refreshCounter;
        private Task<bool> _reconnectTask;
        private readonly Serializer _serializer;
        internal const int MetadataAbortTimeout = 5 * 60000;

        /// <summary>
        /// Gets the binary protocol version to be used for this cluster.
        /// </summary>
        public ProtocolVersion ProtocolVersion 
        {
            get { return _serializer.ProtocolVersion; }
        }

        internal Host Host
        {
            get { return _host; }
            set { _host = value; }
        }

        public IPEndPoint Address
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

        public Serializer Serializer
        {
            get { return _serializer; }
        }

        internal ControlConnection(ProtocolVersion initialProtocolVersion, Configuration config, Metadata metadata)
        {
            _metadata = metadata;
            _reconnectionPolicy = config.Policies.ReconnectionPolicy;
            _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            _reconnectionTimer = new Timer(_ => Reconnect().Forget(), null, Timeout.Infinite, Timeout.Infinite);
            _config = config;
            _serializer = new Serializer(initialProtocolVersion, config.TypeSerializers);
        }

        public void Dispose()
        {
            Shutdown();
        }

        /// <summary>
        /// Tries to create a connection to any of the contact points and retrieve cluster metadata for the first time. Not thread-safe.
        /// </summary>
        /// <exception cref="NoHostAvailableException" />
        /// <exception cref="TimeoutException" />
        /// <exception cref="DriverInternalError" />
        internal async Task Init()
        {
            _logger.Info("Trying to connect the ControlConnection");
            await Connect(true).ConfigureAwait(false);
            SubscribeEventHandlers();
            var obtainingMetadataFailed = false;
            try
            {
                await RefreshNodeList().ConfigureAwait(false);
                await _metadata.RefreshKeyspaces(false).ConfigureAwait(false);
            }
            catch (SocketException ex)
            {
                _logger.Error("An error occurred when trying to retrieve the cluster metadata, retrying.", ex);
                // Can't be awaited on catch
                obtainingMetadataFailed = true;
            }
            if (obtainingMetadataFailed)
            {
                // There was a problem using the connection obtained, it is not usual but can happen
                // Retry one more time and throw if there is problem
                await Reconnect().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Tries to create the a connection to the cluster
        /// </summary>
        /// <exception cref="NoHostAvailableException" />
        /// <exception cref="DriverInternalError" />
        private Task<bool> Connect(bool firstTime)
        {
            IEnumerable<Host> hosts = _metadata.Hosts;
            if (!firstTime)
            {
                _logger.Info("Trying to reconnect the ControlConnection");
                //Use the load balancing policy to determine which host to use
                hosts = _config.Policies.LoadBalancingPolicy.NewQueryPlan(null, null);
            }
            return IterateAndConnect(hosts.GetEnumerator(), new Dictionary<IPEndPoint, Exception>());
        }

        private async Task<bool> IterateAndConnect(IEnumerator<Host> hostsEnumerator, Dictionary<IPEndPoint, Exception> triedHosts)
        {
            var available = hostsEnumerator.MoveNext();
            if (!available)
            {
                throw new NoHostAvailableException(triedHosts);
            }
            var host = hostsEnumerator.Current ?? throw new NullReferenceException("Host should not be null");
            var c = new Connection(_serializer, host.Address, _config);
            // Use a task to workaround "no awaiting in catch"
            Task<bool> nextTask;
            try
            {
                await c.Open().ConfigureAwait(false);
                _connection = c;
                _host = host;
                _logger.Info("Connection established to {0}", c.Address);
                return true;
            }
            catch (UnsupportedProtocolVersionException ex)
            {
                // The server can respond with a message using a lower protocol version supported by the server
                // or using the same version as the one provided
                var nextVersion = _serializer.ProtocolVersion;
                if (nextVersion == ex.ProtocolVersion || !nextVersion.IsSupported())
                {
                    nextVersion = nextVersion.GetLowerSupported();
                }
                if (nextVersion == 0)
                {
                    throw new DriverInternalError("Connection was unable to STARTUP using protocol version " + 
                        ex.ProtocolVersion);
                }
                _serializer.ProtocolVersion = nextVersion;
                _logger.Info("{0}, trying with version {1:D}", ex.Message, nextVersion);
                c.Dispose();
                if (!nextVersion.IsSupported())
                {
                    throw new DriverInternalError("Invalid protocol version " + nextVersion);
                }
                //Retry using the new protocol version
                nextTask = Connect(true);
            }
            catch (Exception ex)
            {
                //There was a socket exception or an authentication exception
                triedHosts.Add(host.Address, ex);
                c.Dispose();
                nextTask = IterateAndConnect(hostsEnumerator, triedHosts);
            }
            return await nextTask.ConfigureAwait(false);
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
            try
            {
                await Connect(false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // It failed to reconnect, schedule the timer for next reconnection and let go.
                Interlocked.Exchange(ref _reconnectTask, null).Forget();
                tcs.TrySetException(ex);
                var delay = _reconnectionSchedule.NextDelayMs();
                _logger.Error("ControlConnection was not able to reconnect: " + ex);
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

            if (Interlocked.Read(ref _isShutdown) > 0L)
            {
                return false;
            }
            try
            {
                await RefreshNodeList().ConfigureAwait(false);
                TaskHelper.WaitToComplete(_metadata.RefreshKeyspaces(false), MetadataAbortTimeout);
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                tcs.TrySetResult(true);
                Interlocked.Exchange(ref _reconnectTask, null).Forget();
                _logger.Info("ControlConnection reconnected to host {0}", _host.Address);
            }
            catch (Exception ex)
            {
                Interlocked.Exchange(ref _reconnectTask, null).Forget();
                _logger.Error("There was an error when trying to refresh the ControlConnection", ex);
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
            if (Interlocked.Increment(ref _refreshCounter) != 1)
            {
                //Only one refresh at a time
                Interlocked.Decrement(ref _refreshCounter);
                return;
            }
            var reconnect = false;
            try
            {
                await RefreshNodeList().ConfigureAwait(false);
                await _metadata.RefreshKeyspaces(false).ConfigureAwait(false);
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
                _logger.Info("Shutting down control connection to {0}", c.Address);
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
            var registerTask = _connection.Send(new RegisterForEventRequest(CassandraEventTypes));
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

        private void OnHostDown(Host h)
        {
            h.Down -= OnHostDown;
            _logger.Warning("Host {0} used by the ControlConnection DOWN", h.Address);
            Task.Factory.StartNew(() => Reconnect(), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        private void OnConnectionCassandraEvent(object sender, CassandraEventArgs e)
        {
            //This event is invoked from a worker thread (not a IO thread)
            if (e is TopologyChangeEventArgs)
            {
                var tce = (TopologyChangeEventArgs)e;
                if (tce.What == TopologyChangeEventArgs.Reason.NewNode || tce.What == TopologyChangeEventArgs.Reason.RemovedNode)
                {
                    // Start refresh
                    Refresh().Forget();
                    return;
                }
            }
            if (e is StatusChangeEventArgs)
            {
                HandleStatusChangeEvent((StatusChangeEventArgs) e);
                return;
            }
            if (e is SchemaChangeEventArgs)
            {
                var ssc = (SchemaChangeEventArgs)e;
                if (!string.IsNullOrEmpty(ssc.Table))
                {
                    //Can be either a table or a view
                    _metadata.RefreshTable(ssc.Keyspace, ssc.Table);
                    _metadata.RefreshView(ssc.Keyspace, ssc.Table);
                    return;
                }
                if (ssc.FunctionName != null)
                {
                    _metadata.ClearFunction(ssc.Keyspace, ssc.FunctionName, ssc.Signature);
                    return;
                }
                if (ssc.AggregateName != null)
                {
                    _metadata.ClearAggregate(ssc.Keyspace, ssc.AggregateName, ssc.Signature);
                    return;
                }
                if (ssc.Type != null)
                {
                    return;
                }
                if (ssc.What == SchemaChangeEventArgs.Reason.Dropped)
                {
                    _metadata.RemoveKeyspace(ssc.Keyspace);
                    return;
                }
                _metadata.RefreshSingleKeyspace(ssc.What == SchemaChangeEventArgs.Reason.Created, ssc.Keyspace);
            }
        }

        private void HandleStatusChangeEvent(StatusChangeEventArgs e)
        {
            //The address in the Cassandra event message needs to be translated
            var address = TranslateAddress(e.Address);
            _logger.Info("Received Node status change event: host {0} is {1}", address, e.What.ToString().ToUpper());
            Host host;
            if (!_metadata.Hosts.TryGet(address, out host))
            {
                _logger.Info("Received status change event for host {0} but it was not found", address);
                return;
            }
            var distance = Cluster.RetrieveDistance(host, _config.Policies.LoadBalancingPolicy);
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
            _logger.Info("Refreshing node list");
            var rsLocal = await QueryAsync(SelectLocal).ConfigureAwait(false);
            var localRow = rsLocal.FirstOrDefault();
            var rsPeers = await QueryAsync(SelectPeers).ConfigureAwait(false);
            if (localRow == null)
            {
                _logger.Error("Local host metadata could not be retrieved");
                return;
            }
            _metadata.Partitioner = localRow.GetValue<string>("partitioner");
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
                _metadata.ClusterName = clusterName;
            }
            localhost.SetLocationInfo(row.GetValue<string>("data_center"), row.GetValue<string>("rack"));
            SetCassandraVersion(localhost, row);
            localhost.Tokens = row.GetValue<IEnumerable<string>>("tokens") ?? new string[0];
            _metadata.SetCassandraVersion(localhost.CassandraVersion);
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
                var host = _metadata.GetHost(address);
                if (host == null)
                {
                    host = _metadata.AddHost(address);
                }
                host.SetLocationInfo(row.GetValue<string>("data_center"), row.GetValue<string>("rack"));
                SetCassandraVersion(host, row);
                host.Tokens = row.GetValue<IEnumerable<string>>("tokens") ?? new string[0];
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

        internal static void SetCassandraVersion(Host host, Row row)
        {
            try
            {
                var releaseVersion = row.GetValue<string>("release_version");
                if (releaseVersion != null)
                {
                    host.CassandraVersion = Version.Parse(releaseVersion.Split('-')[0]);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("There was an error while trying to retrieve the Cassandra version", ex);
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
            if (BindAllAddress.Equals(address) && !row.IsNull("peer"))
            {
                address = row.GetValue<IPAddress>("peer");
                _logger.Warning(String.Format("Found host with 0.0.0.0 as rpc_address, using listen_address ({0}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", address));
            }

            return translator.Translate(new IPEndPoint(address, port));
        }

        /// <summary>
        /// Uses the active connection to execute a query
        /// </summary>
        public IEnumerable<Row> Query(string cqlQuery, bool retry = false)
        {
            return TaskHelper.WaitToComplete(QueryAsync(cqlQuery, retry), MetadataAbortTimeout);
        }

        public async Task<IEnumerable<Row>> QueryAsync(string cqlQuery, bool retry = false)
        {
            var request = new QueryRequest(ProtocolVersion, cqlQuery, false, QueryProtocolOptions.Default);
            Response response;
            try
            {
                response = await _connection.Send(request).ConfigureAwait(false);
            }
            catch (SocketException ex)
            {
                _logger.Error(
                    $"There was an error while executing on the host {cqlQuery} the query '{_connection.Address}'", ex);
                if (!retry)
                {
                    throw;
                }
                // Try reconnect
                await Reconnect().ConfigureAwait(false);
                // Query with retry set to false
                return await QueryAsync(cqlQuery).ConfigureAwait(false);
            }
            return GetRowSet(response);
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
            var result = (ResultResponse) response;
            if (!(result.Output is OutputRows))
            {
                throw new DriverInternalError("Expected rows output, obtained " + result.Output.GetType().FullName);
            }
            return ((OutputRows) result.Output).RowSet;
        }
    }

    /// <summary>
    /// Represents an object that can execute metadata queries
    /// </summary>
    internal interface IMetadataQueryProvider
    {
        ProtocolVersion ProtocolVersion { get; }

        /// <summary>
        /// The address of the endpoint used by the ControlConnection
        /// </summary>
        IPEndPoint Address { get; }

        Serializer Serializer { get; }

        Task<IEnumerable<Row>> QueryAsync(string cqlQuery, bool retry = false);

        IEnumerable<Row> Query(string cqlQuery, bool retry = false);
    }
}
