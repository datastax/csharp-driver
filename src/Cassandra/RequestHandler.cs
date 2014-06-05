//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Text;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Handles a request to cassandra, dealing with host failover and retries on error
    /// </summary>
    internal class RequestHandler<T>
    {
        private static Logger _logger = new Logger(typeof(RequestHandler<object>));
        private static IRetryPolicy DefaultRetryPolicy = new DefaultRetryPolicy();
        private TaskCompletionSource<T> _tcs;
        private Session _session;
        private IRequest _request;
        private IStatement _statement;
        private IRetryPolicy _retryPolicy;
        private int _retryCount = 0;

        public RequestHandler(Session session, IRequest request, IStatement statement)
        {
            _tcs = new TaskCompletionSource<T>();
            _session = session;
            _request = request;
            _statement = statement;
            _retryPolicy = DefaultRetryPolicy;
            if (statement != null && statement.RetryPolicy != null)
            {
                _retryPolicy = statement.RetryPolicy;
            }
        }

        internal Connection GetNextConnection(IStatement statement)
        {
            var triedHosts = new Dictionary<IPAddress, List<Exception>>();
            var hostEnumerable = _session.Policies.LoadBalancingPolicy.NewQueryPlan(statement);
            //hostEnumerable GetEnumerator will return a NEW enumerator, making this call thread safe
            foreach (var host in hostEnumerable)
            {
                if (!host.IsConsiderablyUp)
                {
                    continue;
                }
                var distance = _session.Policies.LoadBalancingPolicy.Distance(host);
                var hostPool = _session.GetConnectionPool(host, distance);
                try
                {
                    var connection = hostPool.BorrowConnection(_session.Keyspace);
                    return connection;
                }
                catch (SocketException ex)
                {
                    _session.SetHostDown(host);
                    triedHosts.Add(host.Address, new List<Exception> { ex });
                }
                catch (Exception ex)
                {
                    triedHosts.Add(host.Address, new List<Exception> { ex });
                }
            }
            //TODO: Remove list of exceptions per host
            throw new NoHostAvailableException(triedHosts);
        }

        /// <summary>
        /// Gets the retry decision based on the exception from Cassandra
        /// </summary>
        public RetryDecision GetRetryDecision(Exception ex)
        {
            RetryDecision decision = RetryDecision.Rethrow();
            if (ex is ReadTimeoutException)
            {
                var e = ex as ReadTimeoutException;
                decision = _retryPolicy.OnReadTimeout(_statement, e.ConsistencyLevel, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, e.WasDataRetrieved, _retryCount);
            }
            else if (ex is WriteTimeoutException)
            {
                var e = ex as WriteTimeoutException;
                decision = _retryPolicy.OnWriteTimeout(_statement, e.ConsistencyLevel, e.WriteType, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, _retryCount);
            }
            else if (ex is UnavailableException)
            {
                var e = ex as UnavailableException;
                decision = _retryPolicy.OnUnavailable(_statement, e.Consistency, e.RequiredReplicas, e.AliveReplicas, _retryCount);
            }
            return decision;
        }

        public void HandleResult(Exception ex, AbstractResponse response)
        {
            if (ex != null)
            {
                HandleException(ex);
                return;
            }
            if (typeof(T) == typeof(RowSet))
            {
                HandleRowSetResult(response);
            }
        }

        /// <summary>
        /// Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
        /// </summary>
        private void HandleException(Exception ex)
        {
            //TODO: Failover
            var decision = GetRetryDecision(ex);
            switch (decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Rethrow:
                    _tcs.TrySetException(ex);
                    break;
                case RetryDecision.RetryDecisionType.Ignore:
                    if (typeof(T).IsAssignableFrom(typeof(RowSet)))
                    {
                        _tcs.TrySetResult((T)(object)new RowSet());
                    }
                    else
                    {
                        _tcs.TrySetResult(default(T));
                    }
                    break;
                case RetryDecision.RetryDecisionType.Retry:
                    Retry(decision.RetryConsistencyLevel);
                    break;
            }
        }

        private void HandleRowSetResult(AbstractResponse response)
        {
            try
            {
                if (response == null)
                {
                    throw new DriverInternalError("Response can not be null");
                }
                if (!(response is ResultResponse))
                {
                    throw new DriverInternalError("Excepted ResultResponse, obtained " + response.GetType().FullName);
                }
                var output = ((ResultResponse)response).Output;
                RowSet rs;
                if (output is OutputRows)
                {
                    rs = ((OutputRows)output).RowSet;
                }
                else
                {
                    rs = new RowSet();
                }
                if (output.TraceId != null)
                {
                    rs.Info.SetQueryTrace(new QueryTrace(output.TraceId.Value, _session));
                }
                //Info.SetTriedHosts(TriedHosts);
                //rowset.Info.SetAchievedConsistency
                if (rs.PagingState != null)
                {
                    rs.FetchNextPage = (pagingState) =>
                    {
                        if (_session.IsDisposed)
                        {
                            _logger.Warning("Trying to page results using a Session already disposed.");
                            return new RowSet();
                        }
                        _statement.SetPagingState(pagingState);
                        return _session.Execute(_statement);
                    };
                }
                _tcs.TrySetResult((T)(object)rs);
            }
            catch (Exception ex)
            {
                _tcs.TrySetException(ex);
            }
        }

        public virtual void Retry(ConsistencyLevel? consistency)
        {
            _retryCount++;
            var connection = GetNextConnection(_statement);
            if (consistency != null && _request is ICqlRequest)
            {
                //Set the new consistency to be used for the new request
                ((ICqlRequest)_request).Consistency = consistency.Value;
            }
            connection.Send(_request, HandleResult);
        }

        public Task<T> Send()
        {
            try
            {
                var connection = GetNextConnection(_statement);
                connection.Send(_request, HandleResult);
            }
            catch (Exception ex)
            {
                _tcs.TrySetException(ex);
            }
            return _tcs.Task;
        }
    }
}
