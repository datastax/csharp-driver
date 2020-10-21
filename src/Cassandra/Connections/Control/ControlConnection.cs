//
//      Copyright (C) DataStax Inc.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.ProtocolEvents;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra.Connections.Control
{
    internal class ControlConnection : IControlConnection
    {
        private readonly IInternalCluster _cluster;
        private readonly Metadata _metadata;
        private volatile Host _host;
        private volatile IConnectionEndPoint _currentConnectionEndPoint;
        private volatile IConnection _connection;

        internal static readonly Logger Logger = new Logger(typeof(ControlConnection));

        private readonly Configuration _config;
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private IReconnectionSchedule _reconnectionSchedule;
        private readonly Timer _reconnectionTimer;
        private long _isShutdown;
        private int _refreshFlag;
        private Task<bool> _reconnectTask;
        private readonly ISerializerManager _serializer;
        private readonly IProtocolEventDebouncer _eventDebouncer;
        private readonly IEnumerable<IContactPoint> _contactPoints;
        private readonly ITopologyRefresher _topologyRefresher;
        private readonly ISupportedOptionsInitializer _supportedOptionsInitializer;

        private bool IsShutdown => Interlocked.Read(ref _isShutdown) > 0L;

        /// <summary>
        /// Gets the binary protocol version to be used for this cluster.
        /// </summary>
        public ProtocolVersion ProtocolVersion => _serializer.CurrentProtocolVersion;

        /// <inheritdoc />
        public Host Host
        {
            get => _host;
            internal set => _host = value;
        }

        public IConnectionEndPoint EndPoint => _connection?.EndPoint;

        public IPEndPoint LocalAddress => _connection?.LocalAddress;

        public ISerializerManager Serializer => _serializer;

        internal ControlConnection(
            IInternalCluster cluster,
            IProtocolEventDebouncer eventDebouncer,
            ProtocolVersion initialProtocolVersion,
            Configuration config,
            Metadata metadata,
            IEnumerable<IContactPoint> contactPoints)
        {
            _cluster = cluster;
            _metadata = metadata;
            _reconnectionPolicy = config.Policies.ReconnectionPolicy;
            _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            _reconnectionTimer = new Timer(ReconnectEventHandler, null, Timeout.Infinite, Timeout.Infinite);
            _config = config;
            _serializer = new SerializerManager(initialProtocolVersion, config.TypeSerializers);
            _eventDebouncer = eventDebouncer;
            _contactPoints = contactPoints;
            _topologyRefresher = config.TopologyRefresherFactory.Create(metadata, config);
            _supportedOptionsInitializer = config.SupportedOptionsInitializerFactory.Create(metadata);

            if (!_config.KeepContactPointsUnresolved)
            {
                TaskHelper.WaitToComplete(InitialContactPointResolutionAsync());
            }
        }

        public void Dispose()
        {
            Shutdown();
        }

        /// <inheritdoc />
        public async Task InitAsync()
        {
            ControlConnection.Logger.Info("Trying to connect the ControlConnection");
            await Connect(true).ConfigureAwait(false);
        }

        /// <summary>
        /// Resolves the contact points to a read only list of <see cref="IConnectionEndPoint"/> which will be used
        /// during initialization. Also sets <see cref="Metadata.SetResolvedContactPoints"/>.
        /// </summary>
        private async Task InitialContactPointResolutionAsync()
        {
            var tasksDictionary = _contactPoints.ToDictionary(c => c, c => c.GetConnectionEndPointsAsync(true));

            await Task.WhenAll(tasksDictionary.Values).ConfigureAwait(false);

            var resolvedContactPoints = tasksDictionary.ToDictionary(t => t.Key, t => t.Value.Result);

            _metadata.SetResolvedContactPoints(resolvedContactPoints);

            if (!resolvedContactPoints.Any(kvp => kvp.Value.Any()))
            {
                var hostNames = tasksDictionary.Where(kvp => kvp.Key.CanBeResolved).Select(kvp => kvp.Key.StringRepresentation);
                throw new NoHostAvailableException($"No host name could be resolved, attempted: {string.Join(", ", hostNames)}");
            }
        }

        private bool TotalConnectivityLoss()
        {
            var currentHosts = _metadata.AllHosts();
            return currentHosts.Count(h => h.IsUp) == 0 || currentHosts.All(h => !_cluster.AnyOpenConnections(h));
        }

        private async Task<IEnumerable<IConnectionEndPoint>> ResolveContactPoint(IContactPoint contactPoint, bool isInitializing)
        {
            var connectivityLoss = !isInitializing && TotalConnectivityLoss();
            if (connectivityLoss && contactPoint.CanBeResolved)
            {
                ControlConnection.Logger.Warning(
                    "Total connectivity loss detected due to the fact that there are no open connections, " +
                    "re-resolving the following contact point: {0}", contactPoint.StringRepresentation);
            }

            var endpoints = await contactPoint.GetConnectionEndPointsAsync(
                _config.KeepContactPointsUnresolved || connectivityLoss).ConfigureAwait(false);
            return _metadata.UpdateResolvedContactPoint(contactPoint, endpoints);
        }

        private async Task<IEnumerable<IConnectionEndPoint>> ResolveHostContactPointOrConnectionEndpointAsync(
            ConcurrentDictionary<IContactPoint, object> attemptedContactPoints, Host host, bool isInitializing)
        {
            if (host.ContactPoint != null && attemptedContactPoints.TryAdd(host.ContactPoint, null))
            {
                return await ResolveContactPoint(host.ContactPoint, isInitializing).ConfigureAwait(false);
            }

            var endpoint =
                await _config
                      .EndPointResolver
                      .GetConnectionEndPointAsync(host, TotalConnectivityLoss())
                      .ConfigureAwait(false);
            return new List<IConnectionEndPoint> { endpoint };
        }

        private IEnumerable<Task<IEnumerable<IConnectionEndPoint>>> ContactPointResolutionTasksEnumerable(
            ConcurrentDictionary<IContactPoint, object> attemptedContactPoints, bool isInitializing)
        {
            foreach (var contactPoint in _contactPoints)
            {
                if (attemptedContactPoints.TryAdd(contactPoint, null))
                {
                    yield return ResolveContactPoint(contactPoint, isInitializing);
                }
            }
        }

        private IEnumerable<Task<IEnumerable<IConnectionEndPoint>>> AllHostsEndPointResolutionTasksEnumerable(
            ConcurrentDictionary<IContactPoint, object> attemptedContactPoints,
            ConcurrentDictionary<Host, object> attemptedHosts,
            bool isInitializing)
        {
            foreach (var host in GetHostEnumerable())
            {
                if (attemptedHosts.TryAdd(host, null))
                {
                    if (!IsHostValid(host, isInitializing))
                    {
                        continue;
                    }

                    yield return ResolveHostContactPointOrConnectionEndpointAsync(attemptedContactPoints, host, isInitializing);
                }
            }
        }

        private IEnumerable<Task<IEnumerable<IConnectionEndPoint>>> DefaultLbpHostsEnumerable(
            ConcurrentDictionary<IContactPoint, object> attemptedContactPoints,
            ConcurrentDictionary<Host, object> attemptedHosts,
            bool isInitializing)
        {
            foreach (var host in _config.DefaultRequestOptions.LoadBalancingPolicy.NewQueryPlan(null, null))
            {
                if (attemptedHosts.TryAdd(host, null))
                {
                    if (!IsHostValid(host, isInitializing))
                    {
                        continue;
                    }

                    yield return ResolveHostContactPointOrConnectionEndpointAsync(attemptedContactPoints, host, isInitializing);
                }
            }
        }

        private bool IsHostValid(Host host, bool initializing)
        {
            if (initializing)
            {
                return true;
            }

            if (_cluster.RetrieveAndSetDistance(host) == HostDistance.Ignored)
            {
                ControlConnection.Logger.Verbose("Skipping {0} because it is ignored.", host.Address.ToString());
                return false;
            }

            if (!host.IsUp)
            {
                ControlConnection.Logger.Verbose("Skipping {0} because it is not UP.", host.Address.ToString());
                return false;
            }

            return true;
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
            // lazy iterator of endpoints to try for the control connection
            IEnumerable<Task<IEnumerable<IConnectionEndPoint>>> endPointResolutionTasksLazyIterator =
                Enumerable.Empty<Task<IEnumerable<IConnectionEndPoint>>>();

            var attemptedContactPoints = new ConcurrentDictionary<IContactPoint, object>();
            var attemptedHosts = new ConcurrentDictionary<Host, object>();

            // start with endpoints from the default LBP if it is already initialized
            if (!isInitializing)
            {
                endPointResolutionTasksLazyIterator = DefaultLbpHostsEnumerable(attemptedContactPoints, attemptedHosts, isInitializing);
            }

            // add contact points next
            endPointResolutionTasksLazyIterator = endPointResolutionTasksLazyIterator.Concat(
                ContactPointResolutionTasksEnumerable(attemptedContactPoints, isInitializing));

            // finally add all hosts iterator, this will contain already tried hosts but we will check for it with the concurrent dictionary
            if (isInitializing)
            {
                endPointResolutionTasksLazyIterator = endPointResolutionTasksLazyIterator.Concat(
                    AllHostsEndPointResolutionTasksEnumerable(attemptedContactPoints, attemptedHosts, isInitializing));
            }

            var triedHosts = new Dictionary<IPEndPoint, Exception>();
            foreach (var endPointResolutionTask in endPointResolutionTasksLazyIterator)
            {
                var endPoints = await endPointResolutionTask.ConfigureAwait(false);
                foreach (var endPoint in endPoints)
                {
                    ControlConnection.Logger.Verbose("Attempting to connect to {0}.", endPoint.EndpointFriendlyName);
                    var connection = _config.ConnectionFactory.CreateUnobserved(_serializer.GetCurrentSerializer(), endPoint, _config);
                    try
                    {
                        var version = _serializer.CurrentProtocolVersion;
                        try
                        {
                            await connection.Open().ConfigureAwait(false);
                        }
                        catch (UnsupportedProtocolVersionException ex)
                        {
                            if (!isInitializing)
                            {
                                // The version of the protocol is not supported on this host
                                // Most likely, we are using a higher protocol version than the host supports
                                ControlConnection.Logger.Warning("Host {0} does not support protocol version {1}. You should use a fixed protocol " +
                                                            "version during rolling upgrades of the cluster. " +
                                                            "Skipping this host on the current attempt to open the control connection.", endPoint.EndpointFriendlyName, ex.ProtocolVersion);
                                throw;
                            }

                            connection =
                                await _config.ProtocolVersionNegotiator.ChangeProtocolVersion(
                                                 _config,
                                                 _serializer,
                                                 ex.ResponseProtocolVersion,
                                                 connection,
                                                 ex,
                                                 version)
                                             .ConfigureAwait(false);
                        }

                        _connection = connection;

                        //// We haven't used a CAS operation, so it's possible that the control connection is
                        //// being closed while a reconnection attempt is happening, we should dispose it in that case.
                        if (IsShutdown)
                        {
                            ControlConnection.Logger.Info(
                                "Connection established to {0} successfully but the Control Connection was being disposed, " +
                                "closing the connection.",
                                connection.EndPoint.EndpointFriendlyName);
                            throw new ObjectDisposedException("Connection established successfully but the Control Connection was being disposed.");
                        }

                        ControlConnection.Logger.Info(
                            "Connection established to {0} using protocol version {1}.",
                            connection.EndPoint.EndpointFriendlyName,
                            _serializer.CurrentProtocolVersion.ToString("D"));

                        if (isInitializing)
                        {
                            await _supportedOptionsInitializer.ApplySupportedOptionsAsync(connection).ConfigureAwait(false);
                        }

                        var currentHost = await _topologyRefresher.RefreshNodeListAsync(
                            endPoint, connection, _serializer.GetCurrentSerializer()).ConfigureAwait(false);

                        SetCurrentConnection(currentHost, endPoint);

                        if (isInitializing)
                        {
                            await _config.ProtocolVersionNegotiator.NegotiateVersionAsync(
                                _config, _metadata, connection, _serializer).ConfigureAwait(false);
                        }

                        await _config.ServerEventsSubscriber.SubscribeToServerEvents(connection, OnConnectionCassandraEvent).ConfigureAwait(false);
                        await _metadata.RebuildTokenMapAsync(false, _config.MetadataSyncOptions.MetadataSyncEnabled).ConfigureAwait(false);

                        _host.Down += OnHostDown;

                        return;
                    }
                    catch (Exception ex)
                    {
                        connection.Dispose();

                        if (ex is ObjectDisposedException)
                        {
                            throw;
                        }

                        if (IsShutdown)
                        {
                            throw new ObjectDisposedException("Control Connection has been disposed.", ex);
                        }

                        ControlConnection.Logger.Info("Failed to connect to {0}. Exception: {1}", endPoint.EndpointFriendlyName, ex.ToString());

                        // There was a socket or authentication exception or an unexpected error
                        triedHosts[endPoint.GetHostIpEndPointWithFallback()] = ex;
                    }
                }
            }
            throw new NoHostAvailableException(triedHosts);
        }

        private async void ReconnectEventHandler(object state)
        {
            try
            {
                await Reconnect().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ControlConnection.Logger.Error("An exception was thrown when reconnecting the control connection.", ex);
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
                ControlConnection.Logger.Info("Trying to reconnect the ControlConnection");
                await Connect(false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // It failed to reconnect, schedule the timer for next reconnection and let go.
                var _ = Interlocked.Exchange(ref _reconnectTask, null);
                tcs.TrySetException(ex);
                var delay = _reconnectionSchedule.NextDelayMs();
                ControlConnection.Logger.Error("ControlConnection was not able to reconnect: " + ex);
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

            if (IsShutdown)
            {
                tcs.TrySetResult(false);
                return false;
            }
            try
            {
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                tcs.TrySetResult(true);
                var _ = Interlocked.Exchange(ref _reconnectTask, null);
                ControlConnection.Logger.Info("ControlConnection reconnected to host {0}", _host.Address);
            }
            catch (Exception ex)
            {
                var _ = Interlocked.Exchange(ref _reconnectTask, null);
                ControlConnection.Logger.Error("There was an error when trying to refresh the ControlConnection", ex);
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
                var currentEndPoint = _currentConnectionEndPoint;
                var currentHost = await _topologyRefresher.RefreshNodeListAsync(
                    currentEndPoint, _connection, _serializer.GetCurrentSerializer()).ConfigureAwait(false);
                
                SetCurrentConnection(currentHost, currentEndPoint);

                await _metadata.RebuildTokenMapAsync(false, _config.MetadataSyncOptions.MetadataSyncEnabled).ConfigureAwait(false);
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            }
            catch (SocketException ex)
            {
                ControlConnection.Logger.Error("There was a SocketException when trying to refresh the ControlConnection", ex);
                reconnect = true;
            }
            catch (Exception ex)
            {
                ControlConnection.Logger.Error("There was an error when trying to refresh the ControlConnection", ex);
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
                ControlConnection.Logger.Info("Shutting down control connection to {0}", c.EndPoint.EndpointFriendlyName);
                c.Dispose();
            }
            _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
            _reconnectionTimer.Dispose();
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
            ControlConnection.Logger.Warning("Host {0} used by the ControlConnection DOWN", h.Address);
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
                ControlConnection.Logger.Error("Exception thrown while handling cassandra event.", ex);
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
            ControlConnection.Logger.Info("Received Node status change event: host {0} is {1}", address, e.What.ToString().ToUpper());
            if (!_metadata.Hosts.TryGet(address, out var host))
            {
                ControlConnection.Logger.Info("Received status change event for host {0} but it was not found", address);
                return;
            }
            var distance = _cluster.RetrieveAndSetDistance(host);
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

        private void SetCurrentConnection(Host host, IConnectionEndPoint endPoint)
        {
            _host = host;
            _currentConnectionEndPoint = endPoint;
            _metadata.SetCassandraVersion(host.CassandraVersion);
        }

        /// <summary>
        /// Uses the active connection to execute a query
        /// </summary>
        public IEnumerable<IRow> Query(string cqlQuery, bool retry = false)
        {
            return TaskHelper.WaitToComplete(QueryAsync(cqlQuery, retry), _config.SocketOptions.MetadataAbortTimeout);
        }

        public async Task<IEnumerable<IRow>> QueryAsync(string cqlQuery, bool retry = false)
        {
            return _config.MetadataRequestHandler.GetRowSet(
                await SendQueryRequestAsync(cqlQuery, retry, QueryProtocolOptions.Default).ConfigureAwait(false));
        }

        public async Task<Response> SendQueryRequestAsync(string cqlQuery, bool retry, QueryProtocolOptions queryProtocolOptions)
        {
            Response response;
            try
            {
                response = await _config.MetadataRequestHandler.SendMetadataRequestAsync(
                    _connection, _serializer.GetCurrentSerializer(), cqlQuery, queryProtocolOptions).ConfigureAwait(false);
            }
            catch (SocketException)
            {
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

        /// <inheritdoc />
        public Task<Response> UnsafeSendQueryRequestAsync(string cqlQuery, QueryProtocolOptions queryProtocolOptions)
        {
            return _config.MetadataRequestHandler.UnsafeSendQueryRequestAsync(
                _connection, _serializer.GetCurrentSerializer(), cqlQuery, queryProtocolOptions);
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