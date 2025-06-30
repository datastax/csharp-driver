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

using Cassandra.Collections;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Observers.Abstractions;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra.Requests
{
    /// <inheritdoc />
    internal class RequestHandler : IRequestHandler
    {
        private static readonly Logger Logger = new Logger(typeof(Session));
        public const long StateInit = 0;
        public const long StateCompleted = 1;

        private readonly IRequest _request;
        private readonly IInternalSession _session;
        private readonly IRequestResultHandler _requestResultHandler;
        private long _state;
        private readonly IEnumerator<HostShard> _queryPlan;
        private readonly object _queryPlanLock = new object();
        private readonly ICollection<IRequestExecution> _running = new CopyOnWriteList<IRequestExecution>();
        private ISpeculativeExecutionPlan _executionPlan;
        private volatile HashedWheelTimer.ITimeout _nextExecutionTimeout;
        private readonly IRequestObserver _requestObserver;
        private readonly SessionRequestInfo _sessionRequestInfo;
        public IExtendedRetryPolicy RetryPolicy { get; }
        public ISerializer Serializer { get; }
        public IStatement Statement { get; }
        public IRequestOptions RequestOptions { get; }

        private readonly Dictionary<Guid, NodeRequestInfo> _nodeExecutions = new Dictionary<Guid, NodeRequestInfo>(1);
        private readonly object _nodeExecutionLock = new object();
        private bool _nodeExecutionsCleared = false;

        /// <summary>
        /// Creates a new instance using a request, the statement and the execution profile.
        /// </summary>
        public RequestHandler(
            IInternalSession session, ISerializer serializer, IRequest request, SessionRequestInfo sessionRequestInfo, IRequestOptions requestOptions, IRequestObserver requestObserver)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _requestObserver = requestObserver;
            _sessionRequestInfo = sessionRequestInfo;
            _requestResultHandler = new TcsMetricsRequestResultHandler(_requestObserver);
            _request = request;
            Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            Statement = sessionRequestInfo.Statement;
            RequestOptions = requestOptions ?? throw new ArgumentNullException(nameof(requestOptions));

            RetryPolicy = RequestOptions.RetryPolicy;

            if (sessionRequestInfo.Statement?.RetryPolicy != null)
            {
                RetryPolicy = sessionRequestInfo.Statement.RetryPolicy.Wrap(RetryPolicy);
            }

            _queryPlan = RequestHandler.GetQueryPlan(session, sessionRequestInfo.Statement, RequestOptions.LoadBalancingPolicy).GetEnumerator();
        }

        /// <summary>
        /// Creates a new instance using the statement to build the request.
        /// Statement can not be null.
        /// </summary>
        public RequestHandler(IInternalSession session, ISerializer serializer, SessionRequestInfo sessionRequestInfo, IRequestOptions requestOptions, IRequestObserver requestObserver)
            : this(session, serializer, RequestHandler.GetRequest(sessionRequestInfo.Statement, serializer, requestOptions), sessionRequestInfo, requestOptions, requestObserver)
        {
        }

        /// <summary>
        /// Creates a new instance with no request, suitable for getting a connection.
        /// </summary>
        public RequestHandler(IInternalSession session, ISerializer serializer, SessionRequestInfo sessionRequestInfo, IRequestObserver requestObserver)
            : this(session, serializer, null, sessionRequestInfo, session.Cluster.Configuration.DefaultRequestOptions, requestObserver)
        {
        }

        /// <summary>
        /// Gets a query plan as determined by the load-balancing policy.
        /// In the special case when a Host is provided at Statement level, it will return a query plan with a single
        /// host.
        /// </summary>
        private static IEnumerable<HostShard> GetQueryPlan(ISession session, IStatement statement, ILoadBalancingPolicy lbp)
        {
            // Single host iteration
            var host = (statement as Statement)?.Host;

            return host == null
                ? lbp.NewQueryPlan(session.Keyspace, statement)
                : Enumerable.Repeat(new HostShard(host, -1), 1);
        }

        /// <inheritdoc />
        public IRequest BuildRequest()
        {
            return RequestHandler.GetRequest(Statement, Serializer, RequestOptions);
        }

        public bool OnNewNodeExecution(NodeRequestInfo nodeRequestInfo)
        {
            lock (_nodeExecutionLock)
            {
                if (!_nodeExecutionsCleared)
                {
                    try
                    {
                        _nodeExecutions.Add(nodeRequestInfo.ExecutionId, nodeRequestInfo);
                        return true;
                    }
                    catch
                    {
                        return false;
                    }
                }
            }

            return false;
        }

        public bool SetNodeExecutionCompleted(Guid executionId)
        {
            lock (_nodeExecutionLock)
            {
                if (_nodeExecutions.Count > 0)
                {
                    return _nodeExecutions.Remove(executionId);
                }
            }

            return false;
        }

        /// <summary>
        /// Gets the Request to send to a cassandra node based on the statement type
        /// </summary>
        internal static IRequest GetRequest(IStatement statement, ISerializer serializer, IRequestOptions requestOptions)
        {
            ICqlRequest request = null;
            if (statement.IsIdempotent == null)
            {
                statement.SetIdempotence(requestOptions.DefaultIdempotence);
            }

            if (statement is RegularStatement s1)
            {
                s1.Serializer = serializer;
                var options = QueryProtocolOptions.CreateFromQuery(serializer.ProtocolVersion, s1, requestOptions, null, null);
                options.ValueNames = s1.QueryValueNames;
                request = new QueryRequest(serializer, s1.QueryString, options, s1.IsTracing, s1.OutgoingPayload);
            }

            if (statement is BoundStatement s2)
            {
                // set skip metadata only when result metadata id is supported because of CASSANDRA-10786
                var skipMetadata =
                    serializer.ProtocolVersion.SupportsResultMetadataId()
                    && s2.PreparedStatement.ResultMetadata.ContainsColumnDefinitions();

                var options = QueryProtocolOptions.CreateFromQuery(serializer.ProtocolVersion, s2, requestOptions, skipMetadata, s2.PreparedStatement.Variables);
                request = new ExecuteRequest(
                    serializer,
                    s2.PreparedStatement.Id,
                    s2.PreparedStatement.ResultMetadata,
                    options,
                    s2.IsTracing,
                    s2.OutgoingPayload,
                    false);
            }

            if (statement is BatchStatement s)
            {
                s.Serializer = serializer;
                var consistency = requestOptions.ConsistencyLevel;
                if (s.ConsistencyLevel.HasValue)
                {
                    consistency = s.ConsistencyLevel.Value;
                }
                request = new BatchRequest(serializer, s.OutgoingPayload, s, consistency, requestOptions);
            }

            if (request == null)
            {
                throw new NotSupportedException("Statement of type " + statement.GetType().FullName + " not supported");
            }
            return request;
        }

        /// <inheritdoc />
        public Task<bool> SetCompletedAsync(Exception ex, RowSet result = null)
        {
            return SetCompletedAsync(ex, result, null);
        }

        /// <inheritdoc />
        public Task<bool> SetCompletedAsync(RowSet result, Func<Task> action)
        {
            return SetCompletedAsync(null, result, action);
        }

        /// <summary>
        /// Marks this instance as completed.
        /// If ex is not null, sets the exception.
        /// If action is not null, it invokes it using the default task scheduler.
        /// </summary>
        private async Task<bool> SetCompletedAsync(Exception ex, RowSet result, Func<Task> action)
        {
            var finishedNow = Interlocked.CompareExchange(ref _state, RequestHandler.StateCompleted, RequestHandler.StateInit) == RequestHandler.StateInit;
            if (!finishedNow)
            {
                return false;
            }

            //Cancel the current timer
            //When the next execution timer is being scheduled at the *same time*
            //the timer is not going to be cancelled, in that case, this instance is going to stay alive a little longer
            _nextExecutionTimeout?.Cancel();
            foreach (var execution in _running)
            {
                execution.Cancel();
            }
            if (ex != null)
            {
                await _requestResultHandler.TrySetExceptionAsync(ex, _sessionRequestInfo).ConfigureAwait(false);
                return true;
            }
            if (action != null)
            {
                //Create a new Task using the default scheduler, invoke the action and set the result
                Task.Run(async () =>
                {
                    try
                    {
                        await action().ConfigureAwait(false);
                    }
                    catch (Exception actionEx)
                    {
                        await ClearNodeExecutionsAsync().ConfigureAwait(false);
                        await _requestResultHandler.TrySetExceptionAsync(actionEx, _sessionRequestInfo).ConfigureAwait(false);
                        return;
                    }

                    await ClearNodeExecutionsAsync().ConfigureAwait(false);
                    await _requestResultHandler.TrySetResultAsync(result, _sessionRequestInfo).ConfigureAwait(false);
                }, CancellationToken.None).Forget();
                return true;
            }

            await ClearNodeExecutionsAsync().ConfigureAwait(false);
            await _requestResultHandler.TrySetResultAsync(result, _sessionRequestInfo).ConfigureAwait(false);
            return true;
        }

        private async Task ClearNodeExecutionsAsync()
        {
            IEnumerable<KeyValuePair<Guid, NodeRequestInfo>> executions;
            lock (_nodeExecutionLock)
            {
                _nodeExecutionsCleared = true;
                if (_nodeExecutions.Count > 0)
                {
                    executions = _nodeExecutions.ToArray();
                    _nodeExecutions.Clear();
                }
                else
                {
                    return;
                }
            }
            foreach (var kvp in executions)
            {
                await _requestObserver.OnNodeRequestAbortedAsync(_sessionRequestInfo, kvp.Value).ConfigureAwait(false);
            }
        }

        public Task SetNoMoreHostsAsync(NoHostAvailableException ex, IRequestExecution execution)
        {
            //An execution ended with a NoHostAvailableException (retrying or starting).
            //If there is a running execution, do not yield it to the user
            _running.Remove(execution);
            if (_running.Count > 0)
            {
                RequestHandler.Logger.Info("Could not obtain an available host for speculative execution");
                return TaskHelper.Completed;
            }
            return SetCompletedAsync(ex);
        }

        public bool HasCompleted()
        {
            return Interlocked.Read(ref _state) == RequestHandler.StateCompleted;
        }

        private HostShard GetNextHost()
        {
            // Lock to handle multiple threads from multiple executions to get a new host
            lock (_queryPlanLock)
            {
                if (_queryPlan.MoveNext())
                {
                    return _queryPlan.Current;
                }
            }
            return null;
        }

        /// <inheritdoc />
        public ValidHost GetNextValidHost(Dictionary<IPEndPoint, Exception> triedHosts)
        {
            HostShard hostShard;
            while ((hostShard = GetNextHost()) != null && !_session.IsDisposed)
            {
                triedHosts[hostShard.Host.Address] = null;
                if (!TryValidateHost(hostShard.Host, out var validHost))
                {
                    continue;
                }

                return validHost;
            }

            throw new NoHostAvailableException(triedHosts);
        }

        /// <summary>
        /// Checks if the host is a valid candidate for the purpose of obtaining a connection.
        /// This method obtains the <see cref="HostDistance"/> from the load balancing policy.
        /// </summary>
        /// <param name="host">Host to check.</param>
        /// <param name="validHost">Output parameter that will contain the <see cref="ValidHost"/> instance.</param>
        /// <returns><code>true</code> if the host is valid and <code>false</code> if not valid
        /// (see documentation of <see cref="ValidHost.New"/>)</returns>
        private bool TryValidateHost(Host host, out ValidHost validHost)
        {
            var distance = _session.InternalCluster.RetrieveAndSetDistance(host);
            validHost = ValidHost.New(host, distance);
            return validHost != null;
        }

        /// <inheritdoc />
        public async Task<IConnection> GetNextConnectionAsync(Dictionary<IPEndPoint, Exception> triedHosts)
        {
            HostShard hostShard;
            // While there is an available host
            while ((hostShard = GetNextHost()) != null)
            {
                var c = await ValidateHostAndGetConnectionAsync(hostShard, triedHosts).ConfigureAwait(false);
                if (c == null)
                {
                    continue;
                }
                return c;
            }

            throw new NoHostAvailableException(triedHosts);
        }

        /// <inheritdoc />
        public async Task<IConnection> ValidateHostAndGetConnectionAsync(HostShard hostShard, Dictionary<IPEndPoint, Exception> triedHosts)
        {
            if (_session.IsDisposed)
            {
                throw new NoHostAvailableException(triedHosts);
            }

            triedHosts[hostShard.Host.Address] = null;
            if (!TryValidateHost(hostShard.Host, out var validHost))
            {
                return null;
            }

            var c = await GetConnectionToValidHostAsync(validHost, triedHosts, hostShard.Shard).ConfigureAwait(false);
            return c;
        }

        /// <inheritdoc />
        public Task<IConnection> GetConnectionToValidHostAsync(ValidHost validHost, IDictionary<IPEndPoint, Exception> triedHosts, int shardID = -1)
        {
            return RequestHandler.GetConnectionFromHostAsync(validHost.Host, validHost.Distance, _session, triedHosts, Statement != null ? Statement.RoutingKey : null, shardID);
        }

        /// <summary>
        /// Gets a connection from a host or null if its not possible, filling the triedHosts map with the failures.
        /// </summary>
        /// <param name="host">Host to which a connection will be obtained.</param>
        /// <param name="distance">Output parameter that will contain the <see cref="HostDistance"/> associated with
        /// <paramref name="host"/>. It is retrieved from the current <see cref="ILoadBalancingPolicy"/>.</param>
        /// <param name="session">Session from where a connection will be obtained (or created).</param>
        /// <param name="triedHosts">Hosts for which there were attempts to connect and send the request.</param>
        /// <param name="routingKey">Routing key to use for the next host.</param>
        /// <param name="shardID">Shard to use.</param>
        /// <exception cref="InvalidQueryException">When the keyspace is not valid</exception>
        internal static Task<IConnection> GetConnectionFromHostAsync(
            Host host, HostDistance distance, IInternalSession session, IDictionary<IPEndPoint, Exception> triedHosts, RoutingKey routingKey = null, int shardID = -1)
        {
            return GetConnectionFromHostInternalAsync(host, distance, session, triedHosts, true, routingKey, shardID);
        }

        private static async Task<IConnection> GetConnectionFromHostInternalAsync(
            Host host, HostDistance distance, IInternalSession session, IDictionary<IPEndPoint, Exception> triedHosts, bool retry, RoutingKey routingKey, int shardID = -1)
        {
            var hostPool = session.GetOrCreateConnectionPool(host, distance);

            try
            {
                return await hostPool.GetConnectionFromHostAsync(triedHosts, () => session.Keyspace, routingKey, shardID).ConfigureAwait(false);
            }
            catch (SocketException)
            {
                if (retry)
                {
                    // A socket exception on the current connection does not mean that all the pool is closed:
                    // Retry on the same host
                    return await RequestHandler.GetConnectionFromHostInternalAsync(host, distance, session, triedHosts, false, routingKey).ConfigureAwait(false);
                }

                throw;
            }
        }

        public async Task<RowSet> SendAsync()
        {
            if (_request == null)
            {
                await _requestResultHandler.TrySetExceptionAsync(new DriverException("request can not be null"), _sessionRequestInfo).ConfigureAwait(false);
                return await _requestResultHandler.Task.ConfigureAwait(false);
            }

            await StartNewExecutionAsync().ConfigureAwait(false);
            return await _requestResultHandler.Task.ConfigureAwait(false);
        }

        /// <summary>
        /// Starts a new execution and adds it to the executions collection
        /// </summary>
        private async Task StartNewExecutionAsync()
        {
            try
            {
                var execution = _session.Cluster.Configuration.RequestExecutionFactory.Create(this, _session, _request, _requestObserver, _sessionRequestInfo);
                var lastHost = execution.Start(false);
                _running.Add(execution);
                ScheduleNext(lastHost);
            }
            catch (NoHostAvailableException ex)
            {
                if (_running.Count == 0)
                {
                    //Its the sending of the first execution
                    //There isn't any host available, yield it to the user
                    await SetCompletedAsync(ex).ConfigureAwait(false);
                }
                //Let's wait for the other executions
            }
            catch (Exception ex)
            {
                //There was an Exception before sending: a protocol error or the keyspace does not exists
                await SetCompletedAsync(ex).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Schedules the next delayed execution
        /// </summary>
        private void ScheduleNext(Host currentHost)
        {
            if (Statement == null || Statement.IsIdempotent == false)
            {
                //its not idempotent, we should not schedule an speculative execution
                return;
            }
            if (_executionPlan == null)
            {
                _executionPlan = RequestOptions.SpeculativeExecutionPolicy.NewPlan(_session.Keyspace, Statement);
            }
            var delay = _executionPlan.NextExecution(currentHost);
            if (delay <= 0)
            {
                return;
            }
            //There is one live timer at a time.
            _nextExecutionTimeout = _session.Cluster.Configuration.Timer.NewTimeout(_ =>
            {
                // Start the speculative execution outside the IO thread
                Task.Run(() =>
                {
                    if (HasCompleted())
                    {
                        return TaskHelper.Completed;
                    }

                    RequestHandler.Logger.Info("Starting new speculative execution after {0} ms. Last used host: {1}", delay, currentHost.Address);
                    _requestObserver.OnSpeculativeExecution(currentHost, delay);
                    return StartNewExecutionAsync();
                }, CancellationToken.None);
            }, null, delay);
        }

        public static async Task<Tuple<SessionRequestInfo, IRequestObserver>> CreateRequestObserver(IInternalSession session, IStatement statement)
        {
            var requestTrackingInfo = new SessionRequestInfo(statement, session.Keyspace);
            var observer = session.ObserverFactory.CreateRequestObserver();
            await observer.OnRequestStartAsync(requestTrackingInfo).ConfigureAwait(false);
            return new Tuple<SessionRequestInfo, IRequestObserver>(requestTrackingInfo, observer);
        }
    }
}