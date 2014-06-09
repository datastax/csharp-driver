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
        private static Logger _logger = new Logger(typeof(Session));
        private static IRetryPolicy DefaultRetryPolicy = new DefaultRetryPolicy();

        private Connection _connection;
        private Host _currentHost;
        private IRequest _request;
        private IRetryPolicy _retryPolicy;
        private Session _session;
        private IStatement _statement;
        private int _retryCount = 0;
        private TaskCompletionSource<T> _tcs;
        private Dictionary<IPAddress, Exception> _triedHosts = new Dictionary<IPAddress, Exception>();

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
            var hostEnumerable = _session.Policies.LoadBalancingPolicy.NewQueryPlan(statement);
            //hostEnumerable GetEnumerator will return a NEW enumerator, making this call thread safe
            foreach (var host in hostEnumerable)
            {
                if (!host.IsConsiderablyUp)
                {
                    continue;
                }
                _currentHost = host;
                _triedHosts.Add(host.Address, null);
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
                    _triedHosts[host.Address] = ex;
                }
                catch (Exception ex)
                {
                    _logger.Error(ex);
                    _triedHosts[host.Address] = ex;
                }
            }
            throw new NoHostAvailableException(_triedHosts);
        }

        /// <summary>
        /// Gets the retry decision based on the exception from Cassandra
        /// </summary>
        public RetryDecision GetRetryDecision(Exception ex)
        {
            RetryDecision decision = RetryDecision.Rethrow();
            if (ex is SocketException)
            {
                decision = RetryDecision.Retry(null);
            }
            else if (ex is OverloadedException || ex is IsBootstrappingException || ex is TruncateException)
            {
                decision = RetryDecision.Retry(null);
            }
            else if (ex is ReadTimeoutException)
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

        /// <summary>
        /// Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
        /// </summary>
        private void HandleException(Exception ex)
        {
            if (ex is PreparedQueryNotFoundException && (_statement is BoundStatement || _statement is BatchStatement))
            {
                PrepareAndRetry(((PreparedQueryNotFoundException)ex).UnknownId);
                return;
            }
            if (ex is SocketException)
            {
                _session.SetHostDown(_currentHost);
            }
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

        /// <summary>
        /// Creates the prepared statement and transitions the task to completed
        /// </summary>
        private void HandlePreparedResult(AbstractResponse response)
        {
            ValidateResult(response);
            var output = ((ResultResponse)response).Output;
            if (!(output is OutputPrepared))
            {
                throw new DriverInternalError("Expected prepared response, obtained " + output.GetType().FullName);
            }
            if (!(_request is PrepareRequest))
            {
                throw new DriverInternalError("Obtained PREPARED response for " + _request.GetType().FullName + " request");
            }
            var prepared = (OutputPrepared)output;
            var statement = new PreparedStatement(prepared.Metadata, prepared.QueryId, ((PrepareRequest)_request).Query, prepared.ResultMetadata);
            _tcs.TrySetResult((T)(object)statement);
        }

        /// <summary>
        /// Gets the resulting RowSet and transitions the task to completed.
        /// </summary>
        private void HandleRowSetResult(AbstractResponse response)
        {
            ValidateResult(response);
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
            rs.Info.SetTriedHosts(_triedHosts.Keys.ToList());
            if (_request is ICqlRequest)
            {
                rs.Info.SetAchievedConsistency(((ICqlRequest)_request).Consistency);
            }
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

        private void PrepareAndRetry(byte[] id)
        {
            _logger.Info(String.Format("Query {0} is not prepared on {1}, preparing before retrying executing.", id, _currentHost));
            BoundStatement boundStatement = null;
            if (_statement is BoundStatement)
            {
                boundStatement = (BoundStatement)_statement;
            }
            else if (_statement is BatchStatement)
            {
                var batch = (BatchStatement)_statement;
                Func<Statement, bool> search = s => s is BoundStatement && ((BoundStatement)s).PreparedStatement.Id.SequenceEqual(id);
                boundStatement = (BoundStatement)batch.Queries.FirstOrDefault(search);
            }
            if (boundStatement == null)
            {
                throw new DriverInternalError("Expected Bound or batch statement");
            }
            var request = new PrepareRequest(boundStatement.PreparedStatement.Cql);
            _connection.Send(request, ResponseReprepareHandler);
        }

        /// <summary>
        /// Generic handler for all the responses
        /// </summary>
        public void ResponseHandler(Exception ex, AbstractResponse response)
        {
            try
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
                else if (typeof(T) == typeof(PreparedStatement))
                {
                    HandlePreparedResult(response);
                }
            }
            catch (Exception handlerException)
            {
                _tcs.TrySetException(handlerException);
            }
        }

        /// <summary>
        /// Handles the response of a (re)prepare request and retries to execute on the same connection
        /// </summary>
        private void ResponseReprepareHandler(Exception ex, AbstractResponse response)
        {
            try
            {
                if (ex != null)
                {
                    HandleException(ex);
                    return;
                }
                ValidateResult(response);
                var output = ((ResultResponse)response).Output;
                if (!(output is OutputPrepared))
                {
                    throw new DriverInternalError("Expected prepared response, obtained " + output.GetType().FullName);
                }
                _connection.Send(_request, ResponseHandler);
            }
            catch (Exception exception)
            {
                _tcs.TrySetException(exception);
            }
        }

        public virtual void Retry(ConsistencyLevel? consistency)
        {
            _retryCount++;
            if (consistency != null && _request is ICqlRequest)
            {
                //Set the new consistency to be used for the new request
                ((ICqlRequest)_request).Consistency = consistency.Value;
            }
            TrySend();
        }

        public Task<T> Send()
        {
            TrySend();
            return _tcs.Task;
        }

        public void TrySend()
        {
            try
            {
                _connection = GetNextConnection(_statement);
                _connection.Send(_request, ResponseHandler);
            }
            catch (Exception ex)
            {
                //There was an Exception before sending (probably no host is available).
                //This will mark the Task as faulted.
                _tcs.TrySetException(ex);
            }
        }

        private void ValidateResult(AbstractResponse response)
        {
            if (response == null)
            {
                throw new DriverInternalError("Response can not be null");
            }
            if (!(response is ResultResponse))
            {
                throw new DriverInternalError("Excepted ResultResponse, obtained " + response.GetType().FullName);
            }
        }
    }
}
