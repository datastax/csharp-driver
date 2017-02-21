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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Collections;
using Cassandra.Serialization;
using Cassandra.Tasks;

namespace Cassandra.Requests
{
    /// <summary>
    /// Handles request executions, each execution handles retry and failover.
    /// </summary>
    internal class RequestHandler<T> where T : class
    {
        // ReSharper disable once StaticMemberInGenericType
        private static readonly Logger Logger = new Logger(typeof(Session));
        public const long StateInit = 0;
        public const long StateCompleted = 1;

        private readonly IRequest _request;
        private readonly ISession _session;
        private readonly TaskCompletionSource<T> _tcs;
        private long _state;
        private readonly IEnumerator<Host> _queryPlan;
        private readonly object _queryPlanLock = new object();
        private readonly ICollection<RequestExecution<T>> _running = new CopyOnWriteList<RequestExecution<T>>();
        private ISpeculativeExecutionPlan _executionPlan;
        private volatile Host _host;
        private volatile HashedWheelTimer.ITimeout _nextExecutionTimeout;

        public Policies Policies { get; private set; }
        public IRetryPolicy RetryPolicy { get; private set; }
        public Serializer Serializer { get; private set; }
        public IStatement Statement { get; private set; }

        /// <summary>
        /// Creates a new instance using a request and the statement.
        /// </summary>
        public RequestHandler(ISession session, Serializer serializer, IRequest request, IStatement statement)
        {
            if (session == null)
            {
                throw new ArgumentNullException("session");
            }
            if (serializer == null)
            {
                throw new ArgumentNullException("session");
            }
            _tcs = new TaskCompletionSource<T>();
            _session = session;
            _request = request;
            Serializer = serializer;
            Statement = statement;
            Policies = _session.Cluster.Configuration.Policies;
            RetryPolicy = session.Cluster.Configuration.Policies.RetryPolicy;
            if (statement != null)
            {
                if (statement.RetryPolicy != null)
                {
                    RetryPolicy = statement.RetryPolicy;   
                }
            }
            _queryPlan = Policies.LoadBalancingPolicy.NewQueryPlan(_session.Keyspace, statement).GetEnumerator();
        }

        /// <summary>
        /// Creates a new instance using a request with no statement.
        /// </summary>
        public RequestHandler(ISession session, Serializer serializer, IRequest request)
            : this(session, serializer, request, null)
        {

        }

        /// <summary>
        /// Creates a new instance using the statement to build the request.
        /// Statement can not be null.
        /// </summary>
        public RequestHandler(ISession session, Serializer serializer, IStatement statement)
            : this(session, serializer, GetRequest(statement, serializer, session.Cluster.Configuration), statement)
        {

        }

        /// <summary>
        /// Creates a new instance with no request, suitable for getting a connection.
        /// </summary>
        public RequestHandler(ISession session, Serializer serializer)
            : this(session, serializer, null, null)
        {

        }

        /// <summary>
        /// Gets the Request to send to a cassandra node based on the statement type
        /// </summary>
        internal static IRequest GetRequest(IStatement statement, Serializer serializer, Configuration config)
        {
            ICqlRequest request = null;
            if (statement.IsIdempotent == null)
            {
                statement.SetIdempotence(config.QueryOptions.GetDefaultIdempotence());
            }
            if (statement is RegularStatement)
            {
                var s = (RegularStatement)statement;
                s.Serializer = serializer;
                var options = QueryProtocolOptions.CreateFromQuery(s, config.QueryOptions);
                options.ValueNames = s.QueryValueNames;
                request = new QueryRequest(serializer.ProtocolVersion, s.QueryString, s.IsTracing, options);
            }
            if (statement is BoundStatement)
            {
                var s = (BoundStatement)statement;
                var options = QueryProtocolOptions.CreateFromQuery(s, config.QueryOptions);
                request = new ExecuteRequest(serializer.ProtocolVersion, s.PreparedStatement.Id, null, s.IsTracing, options);
            }
            if (statement is BatchStatement)
            {
                var s = (BatchStatement)statement;
                s.Serializer = serializer;
                var consistency = config.QueryOptions.GetConsistencyLevel();
                if (s.ConsistencyLevel != null)
                {
                    consistency = s.ConsistencyLevel.Value;
                }
                request = new BatchRequest(serializer.ProtocolVersion, s, consistency);
            }
            if (request == null)
            {
                throw new NotSupportedException("Statement of type " + statement.GetType().FullName + " not supported");   
            }
            //Set the outgoing payload for the request
            request.Payload = statement.OutgoingPayload;
            return request;
        }

        /// <summary>
        /// Marks this instance as completed (if not already) and sets the exception or result
        /// </summary>
        public bool SetCompleted(Exception ex, T result = null)
        {
            return SetCompleted(ex, result, null);
        }

        /// <summary>
        /// Marks this instance as completed (if not already) and in a new Task using the default scheduler, it invokes the action and sets the result
        /// </summary>
        public bool SetCompleted(T result, Action action)
        {
            return SetCompleted(null, result, action);
        }

        /// <summary>
        /// Marks this instance as completed.
        /// If ex is not null, sets the exception.
        /// If action is not null, it invokes it using the default task scheduler.
        /// </summary>
        private bool SetCompleted(Exception ex, T result, Action action)
        {
            var finishedNow = Interlocked.CompareExchange(ref _state, StateCompleted, StateInit) == StateInit;
            if (!finishedNow)
            {
                return false;
            }
            //Cancel the current timer
            //When the next execution timer is being scheduled at the *same time*
            //the timer is not going to be cancelled, in that case, this instance is going to stay alive a little longer
            if (_nextExecutionTimeout != null)
            {
                _nextExecutionTimeout.Cancel();
            }
            foreach (var execution in _running)
            {
                execution.Cancel();
            }
            if (ex != null)
            {
                _tcs.TrySetException(ex);
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
                        _tcs.TrySetResult(result);
                    }
                    catch (Exception actionEx)
                    {
                        _tcs.TrySetException(actionEx);
                    }
                });
                return true;
            }
            _tcs.TrySetResult(result);
            return true;
        }

        public void SetNoMoreHosts(NoHostAvailableException ex, RequestExecution<T> execution)
        {
            //An execution ended with a NoHostAvailableException (retrying or starting).
            //If there is a running execution, do not yield it to the user
            _running.Remove(execution);
            if (_running.Count > 0)
            {
                Logger.Info("Could not obtain an available host for speculative execution");
                return;
            }
            SetCompleted(ex);
        }

        public bool HasCompleted()
        {
            return Interlocked.Read(ref _state) == StateCompleted;
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

        /// <summary>
        /// Gets a connection from the next host according to the load balancing policy
        /// </summary>
        /// <exception cref="InvalidQueryException">When the keyspace is not valid</exception>
        /// <exception cref="UnsupportedProtocolVersionException">When the protocol version is not supported in the host</exception>
        /// <exception cref="NoHostAvailableException"></exception>
        internal async Task<Connection> GetNextConnection(Dictionary<IPEndPoint, Exception> triedHosts)
        {
            Host host;
            // Use the concrete implementation in this method
            var session = (Session) _session;
            Connection c = null;
            // While there is an available host
            while ((host = GetNextHost()) != null && !session.IsDisposed)
            {
                _host = host;
                triedHosts[host.Address] = null;
                // Retrieve the distance from the load balancing and setting it
                // at host level
                var distance = Cluster.RetrieveDistance(host, Policies.LoadBalancingPolicy);
                if (distance == HostDistance.Ignored)
                {
                    // We should not use an ignored host
                    continue;
                }
                if (!_host.IsUp)
                {
                    // The host is not considered UP by the driver.
                    // We could have filtered earlier by hosts that are considered UP, but we must check the host
                    // distance first.
                    continue;
                }
                var hostPool = session.GetOrCreateConnectionPool(host, distance);
                try
                {
                    c = await hostPool.BorrowConnection().ConfigureAwait(false);
                }
                catch (UnsupportedProtocolVersionException ex)
                {
                    // The version of the protocol is not supported on this host
                    // Most likely, we are using a higher protocol version than the host supports
                    Logger.Error("Host {0} does not support protocol version {1}. You should use a fixed protocol " +
                                 "version during rolling upgrades of the cluster. Setting the host as DOWN to " +
                                 "avoid hitting this node as part of the query plan for a while", host.Address, ex.ProtocolVersion);
                    triedHosts[host.Address] = ex;
                    session.MarkAsDownAndScheduleReconnection(host, hostPool);
                }
                catch (Exception ex)
                {
                    // Probably a SocketException/AuthenticationException, move along
                    Logger.Error(ex);
                    triedHosts[host.Address] = ex;
                }
                if (c == null)
                {
                    continue;
                }
                await c.SetKeyspace(_session.Keyspace).ConfigureAwait(false);
                break;
            }

            if (c == null)
            {
                throw new NoHostAvailableException(triedHosts);
            }
            return c;
        }

        public Task<T> Send()
        {
            if (_request == null)
            {
                _tcs.TrySetException(new DriverException("request can not be null"));
                return _tcs.Task;
            }
            StartNewExecution();
            return _tcs.Task;
        }

        /// <summary>
        /// Starts a new execution and adds it to the executions collection
        /// </summary>
        private void StartNewExecution()
        {
            try
            {
                var execution = new RequestExecution<T>(this, _session, _request);
                execution.Start();
                _running.Add(execution);
                ScheduleNext();
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
        private void ScheduleNext()
        {
            if (Statement == null || Statement.IsIdempotent == false)
            {
                //its not idempotent, we should not schedule an speculative execution
                return;
            }
            if (_executionPlan == null)
            {
                _executionPlan = Policies.SpeculativeExecutionPolicy.NewPlan(_session.Keyspace, Statement);
            }
            var delay = _executionPlan.NextExecution(_host);
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
                    Logger.Info("Starting new speculative execution after {0}, last used host {1}", delay, _host.Address);
                    StartNewExecution();
                });
            }, null, delay);
        }
    }
}
