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
using Cassandra.Tasks;

namespace Cassandra.Requests
{
    /// <summary>
    /// Handles request executions, each execution handles retry and failover.
    /// </summary>
    internal class RequestHandler<T> where T : class
    {
        // ReSharper disable once StaticMemberInGenericType
        private readonly static Logger Logger = new Logger(typeof(Session));
        public const int StateInit = 0;
        public const int StateCompleted = 1;

        private readonly IRequest _request;
        private readonly ISession _session;
        private readonly TaskCompletionSource<T> _tcs;
        private int _state;
        private readonly IEnumerator<Host> _queryPlan;
        private readonly object _queryPlanLock = new object();
        private readonly ICollection<RequestExecution<T>> _running = new CopyOnWriteList<RequestExecution<T>>();
        private ISpeculativeExecutionPlan _executionPlan;
        private volatile Host _host;
        private volatile HashedWheelTimer.ITimeout _nextExecutionTimeout;

        public Policies Policies { get; private set; }
        public IRetryPolicy RetryPolicy { get; private set; }
        public IStatement Statement { get; private set; }

        /// <summary>
        /// Creates a new instance using a request and the statement.
        /// </summary>
        public RequestHandler(ISession session, IRequest request, IStatement statement)
        {
            _tcs = new TaskCompletionSource<T>();
            _session = session;
            _request = request;
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
        public RequestHandler(ISession session, IRequest request)
            : this(session, request, null)
        {

        }

        /// <summary>
        /// Creates a new instance using the statement to build the request.
        /// Statement can not be null.
        /// </summary>
        public RequestHandler(ISession session, IStatement statement)
            : this(session, GetRequest(statement, session.BinaryProtocolVersion, session.Cluster.Configuration), statement)
        {

        }

        /// <summary>
        /// Creates a new instance with no request, suitable for getting a connection.
        /// </summary>
        public RequestHandler(ISession session)
            : this(session, null, null)
        {

        }

        /// <summary>
        /// Gets the Request to send to a cassandra node based on the statement type
        /// </summary>
        internal static IRequest GetRequest(IStatement statement, int protocolVersion, Configuration config)
        {
            if (statement == null)
            {
                throw new ArgumentNullException("statement");
            }
            if (statement is RegularStatement)
            {
                var s = (RegularStatement)statement;
                s.ProtocolVersion = protocolVersion;
                var options = QueryProtocolOptions.CreateFromQuery(s, config.QueryOptions);
                options.ValueNames = s.QueryValueNames;
                return new QueryRequest(protocolVersion, s.QueryString, s.IsTracing, options);
            }
            if (statement is BoundStatement)
            {
                var s = (BoundStatement)statement;
                var options = QueryProtocolOptions.CreateFromQuery(s, config.QueryOptions);
                return new ExecuteRequest(protocolVersion, s.PreparedStatement.Id, null, s.IsTracing, options);
            }
            if (statement is BatchStatement)
            {
                var s = (BatchStatement)statement;
                s.ProtocolVersion = protocolVersion;
                var consistency = config.QueryOptions.GetConsistencyLevel();
                if (s.ConsistencyLevel != null)
                {
                    consistency = s.ConsistencyLevel.Value;
                }
                return new BatchRequest(protocolVersion, s, consistency);
            }
            throw new NotSupportedException("Statement of type " + statement.GetType().FullName + " not supported");
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
                return;
            }
            SetCompleted(ex);
        }

        public bool HasCompleted()
        {
            return Thread.VolatileRead(ref _state) == StateCompleted;
        }

        private Host GetNextHost()
        {
            Host host = null;
            //Lock to handle multiple threads from multiple executions to get a new host
            lock (_queryPlanLock)
            {
                while (_queryPlan.MoveNext())
                {
                    var h = _queryPlan.Current;
                    if (h.IsConsiderablyUp)
                    {
                        host = h;
                        break;
                    }
                }   
            }
            return host;
        }

        /// <summary>
        /// Gets a connection from the next host according to the load balancing policy
        /// </summary>
        /// <exception cref="InvalidQueryException">When the keyspace is not valid</exception>
        /// <exception cref="UnsupportedProtocolVersionException">When the protocol version is not supported in the host</exception>
        /// <exception cref="NoHostAvailableException"></exception>
        internal Connection GetNextConnection(Dictionary<IPEndPoint, Exception> triedHosts)
        {
            Host host;
            while ((host = GetNextHost()) != null)
            {
                _host = host;
                triedHosts[host.Address] = null;
                try
                {
                    var distance = Policies.LoadBalancingPolicy.Distance(host);
                    //Use the concrete session here
                    var hostPool = ((Session)_session).GetOrCreateConnectionPool(host, distance);
                    var connection = hostPool.BorrowConnection();
                    if (connection == null)
                    {
                        //The load balancing policy did not allow to connect to this node
                        continue;
                    }
                    connection.Keyspace = _session.Keyspace;
                    return connection;
                }
                catch (SocketException ex)
                {
                    host.SetDown();
                    triedHosts[host.Address] = ex;
                }
                catch (InvalidQueryException)
                {
                    //The keyspace does not exist
                    throw;
                }
                catch (UnsupportedProtocolVersionException)
                {
                    //The version of the protocol is not supported
                    throw;
                }
                catch (Exception ex)
                {
                    //We didn't expect this but move to next host
                    Logger.Error(ex);
                    triedHosts[host.Address] = ex;
                }
            }
            throw new NoHostAvailableException(triedHosts);
        }

        /// <summary>
        /// Sets a host down by the provided connection
        /// </summary>
        public void SetHostDown(Connection connection)
        {
            Host host;
            //Trying to avoid referencing the parent host in the connection or having a host reference in the RequestExecution{T} class
            if (!_session.Cluster.Metadata.Hosts.TryGet(connection.Address, out host))
            {
                return;
            }
            host.SetDown();
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
            if (Statement == null || !(Statement.IsIdempotent ?? _session.Cluster.Configuration.QueryOptions.GetDefaultIdempotence()))
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
            _nextExecutionTimeout = _session.Cluster.Configuration.Timer.NewTimeout(() =>
            {
                if (HasCompleted())
                {
                    return;
                }
                StartNewExecution();
            }, delay);
        }
    }
}
