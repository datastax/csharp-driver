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
        private readonly IEnumerator<Host> _queryPlan;
        private readonly object _queryPlanLock = new object();
        private readonly ICollection<IRequestExecution> _running = new CopyOnWriteList<IRequestExecution>();
        private ISpeculativeExecutionPlan _executionPlan;
        private volatile HashedWheelTimer.ITimeout _nextExecutionTimeout;
        private readonly IRequestObserver _requestObserver;
        public IExtendedRetryPolicy RetryPolicy { get; }
        public ISerializer Serializer { get; }
        public IStatement Statement { get; }
        public IRequestOptions RequestOptions { get; }

        /// <summary>
        /// Creates a new instance using a request, the statement and the execution profile.
        /// </summary>
        public RequestHandler(
            IInternalSession session, ISerializer serializer, IRequest request, IStatement statement, IRequestOptions requestOptions)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _requestObserver = session.ObserverFactory.CreateRequestObserver();
            _requestResultHandler = new TcsMetricsRequestResultHandler(_requestObserver);
            _request = request;
            Serializer = serializer ?? throw new ArgumentNullException(nameof(session));
            Statement = statement;
            RequestOptions = requestOptions ?? throw new ArgumentNullException(nameof(requestOptions));

            RetryPolicy = RequestOptions.RetryPolicy;

            if (statement?.RetryPolicy != null)
            {
                RetryPolicy = statement.RetryPolicy.Wrap(RetryPolicy);
            }

            _queryPlan = RequestHandler.GetQueryPlan(session, statement, RequestOptions.LoadBalancingPolicy).GetEnumerator();
        }

        /// <summary>
        /// Creates a new instance using the statement to build the request.
        /// Statement can not be null.
        /// </summary>
        public RequestHandler(IInternalSession session, ISerializer serializer, IStatement statement, IRequestOptions requestOptions)
            : this(session, serializer, RequestHandler.GetRequest(statement, serializer, requestOptions), statement, requestOptions)
        {
        }

        /// <summary>
        /// Creates a new instance with no request, suitable for getting a connection.
        /// </summary>
        public RequestHandler(IInternalSession session, ISerializer serializer)
            : this(session, serializer, null, null, session.Cluster.Configuration.DefaultRequestOptions)
        {
        }

        /// <summary>
        /// Gets a query plan as determined by the load-balancing policy.
        /// In the special case when a Host is provided at Statement level, it will return a query plan with a single
        /// host.
        /// </summary>
        private static IEnumerable<Host> GetQueryPlan(ISession session, IStatement statement, ILoadBalancingPolicy lbp)
        {
            // Single host iteration
            var host = (statement as Statement)?.Host;

            return host == null
                ? lbp.NewQueryPlan(session.Keyspace, statement)
                : Enumerable.Repeat(host, 1);
        }

        /// <inheritdoc />
        public IRequest BuildRequest()
        {
            return RequestHandler.GetRequest(Statement, Serializer, RequestOptions);
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
                var options = QueryProtocolOptions.CreateFromQuery(serializer.ProtocolVersion, s1, requestOptions, null);
                options.ValueNames = s1.QueryValueNames;
                request = new QueryRequest(serializer, s1.QueryString, options, s1.IsTracing, s1.OutgoingPayload);
            }

            if (statement is BoundStatement s2)
            {
                // set skip metadata only when result metadata id is supported because of CASSANDRA-10786
                var skipMetadata = 
                    serializer.ProtocolVersion.SupportsResultMetadataId() 
                    && s2.PreparedStatement.ResultMetadata.ContainsColumnDefinitions();

                var options = QueryProtocolOptions.CreateFromQuery(serializer.ProtocolVersion, s2, requestOptions, skipMetadata);
                request = new ExecuteRequest(
                    serializer, 
                    s2.PreparedStatement.Id, 
                    null,
                    s2.PreparedStatement.ResultMetadata, 
                    options,
                    s2.IsTracing, 
                    s2.OutgoingPayload);
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
        public bool SetCompleted(Exception ex, RowSet result = null)
        {
            return SetCompleted(ex, result, null);
        }

        /// <inheritdoc />
        public bool SetCompleted(RowSet result, Action action)
        {
            return SetCompleted(null, result, action);
        }

        /// <summary>
        /// Marks this instance as completed.
        /// If ex is not null, sets the exception.
        /// If action is not null, it invokes it using the default task scheduler.
        /// </summary>
        private bool SetCompleted(Exception ex, RowSet result, Action action)
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
                _requestResultHandler.TrySetException(ex);
                return true;
            }
            if (action != null)
            {
                //Create a new Task using the default scheduler, invoke the action and set the result
                Task.Factory.StartNew(() =>
                {
                    try
                    {
                        action();
                        _requestResultHandler.TrySetResult(result);
                    }
                    catch (Exception actionEx)
                    {
                        _requestResultHandler.TrySetException(actionEx);
                    }
                });
                return true;
            }
            _requestResultHandler.TrySetResult(result);
            return true;
        }

        public void SetNoMoreHosts(NoHostAvailableException ex, IRequestExecution execution)
        {
            //An execution ended with a NoHostAvailableException (retrying or starting).
            //If there is a running execution, do not yield it to the user
            _running.Remove(execution);
            if (_running.Count > 0)
            {
                RequestHandler.Logger.Info("Could not obtain an available host for speculative execution");
                return;
            }
            SetCompleted(ex);
        }

        public bool HasCompleted()
        {
            return Interlocked.Read(ref _state) == RequestHandler.StateCompleted;
        }

        private Host GetNextHost()
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
            Host host;
            while ((host = GetNextHost()) != null && !_session.IsDisposed)
            {
                triedHosts[host.Address] = null;
                if (!TryValidateHost(host, out var validHost))
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
            Host host;
            // While there is an available host
            while ((host = GetNextHost()) != null)
            {
                var c = await ValidateHostAndGetConnectionAsync(host, triedHosts).ConfigureAwait(false);
                if (c == null)
                {
                    continue;
                }
                return c;
            }

            throw new NoHostAvailableException(triedHosts);
        }

        /// <inheritdoc />
        public async Task<IConnection> ValidateHostAndGetConnectionAsync(Host host, Dictionary<IPEndPoint, Exception> triedHosts)
        {
            if (_session.IsDisposed)
            {
                throw new NoHostAvailableException(triedHosts);
            }

            triedHosts[host.Address] = null;
            if (!TryValidateHost(host, out var validHost))
            {
                return null;
            }

            var c = await GetConnectionToValidHostAsync(validHost, triedHosts).ConfigureAwait(false);
            return c;
        }

        /// <inheritdoc />
        public Task<IConnection> GetConnectionToValidHostAsync(ValidHost validHost, IDictionary<IPEndPoint, Exception> triedHosts)
        {
            return RequestHandler.GetConnectionFromHostAsync(validHost.Host, validHost.Distance, _session, triedHosts);
        }

        /// <summary>
        /// Gets a connection from a host or null if its not possible, filling the triedHosts map with the failures.
        /// </summary>
        /// <param name="host">Host to which a connection will be obtained.</param>
        /// <param name="distance">Output parameter that will contain the <see cref="HostDistance"/> associated with
        /// <paramref name="host"/>. It is retrieved from the current <see cref="ILoadBalancingPolicy"/>.</param>
        /// <param name="session">Session from where a connection will be obtained (or created).</param>
        /// <param name="triedHosts">Hosts for which there were attempts to connect and send the request.</param>
        /// <exception cref="InvalidQueryException">When the keyspace is not valid</exception>
        internal static async Task<IConnection> GetConnectionFromHostAsync(
            Host host, HostDistance distance, IInternalSession session, IDictionary<IPEndPoint, Exception> triedHosts)
        {
            var hostPool = session.GetOrCreateConnectionPool(host, distance);
            
            try
            {
                return await hostPool.GetConnectionFromHostAsync(triedHosts, () => session.Keyspace).ConfigureAwait(false);
            }
            catch (SocketException)
            {
                // A socket exception on the current connection does not mean that all the pool is closed:
                // Retry on the same host
                return await RequestHandler.GetConnectionFromHostAsync(host, distance, session, triedHosts).ConfigureAwait(false);
            }
        }

        public Task<RowSet> SendAsync()
        {
            if (_request == null)
            {
                _requestResultHandler.TrySetException(new DriverException("request can not be null"));
                return _requestResultHandler.Task;
            }

            StartNewExecution();
            return _requestResultHandler.Task;
        }

        /// <summary>
        /// Starts a new execution and adds it to the executions collection
        /// </summary>
        private void StartNewExecution()
        {
            try
            {
                var execution = _session.Cluster.Configuration.RequestExecutionFactory.Create(this, _session, _request, _requestObserver);
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
                    SetCompleted(ex);
                }
                //Let's wait for the other executions
            }
            catch (Exception ex)
            {
                //There was an Exception before sending: a protocol error or the keyspace does not exists
                SetCompleted(ex);
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
                        return;
                    }

                    RequestHandler.Logger.Info("Starting new speculative execution after {0} ms. Last used host: {1}", delay, currentHost.Address);
                    _requestObserver.OnSpeculativeExecution(currentHost, delay);
                    StartNewExecution();
                });
            }, null, delay);
        }
    }
}