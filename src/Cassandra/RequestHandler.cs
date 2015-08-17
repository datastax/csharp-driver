﻿//
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
using System.Threading.Tasks;
using Cassandra.Tasks;
// ReSharper disable StaticMemberInGenericType

namespace Cassandra
{
    /// <summary>
    /// Handles a request to cassandra, dealing with host failover and retries on error
    /// </summary>
    internal class RequestHandler<T>
    {
        // ReSharper disable once InconsistentNaming
        private readonly static Logger _logger = new Logger(typeof(Session));

        private Connection _connection;
        private readonly IRequest _request;
        private readonly IRetryPolicy _retryPolicy;
        private readonly Session _session;
        private readonly IStatement _statement;
        private readonly bool _autoPage = true;
        private readonly bool _retryOnTimeout = true;
        private int _retryCount;
        private readonly TaskCompletionSource<T> _tcs;
        private readonly Dictionary<IPEndPoint, Exception> _triedHosts = new Dictionary<IPEndPoint, Exception>();

        public Host Host { get; private set; }

        /// <summary>
        /// Creates a new instance of the RequestHandler that deals with host failover and retries on error
        /// </summary>
        public RequestHandler(Session session, IRequest request, IStatement statement)
        {
            _tcs = new TaskCompletionSource<T>();
            _session = session;
            _request = request;
            _statement = statement;
            _retryPolicy = Policies.DefaultRetryPolicy;
            //Don't use Argument null check to allow unit test
            if (session != null && session.Policies != null && session.Policies.RetryPolicy != null)
            {
                _retryPolicy = session.Policies.RetryPolicy;
                if (session.Configuration != null)
                {
                    _retryOnTimeout = session.Configuration.QueryOptions.RetryOnTimeout;
                }
            }
            if (statement != null)
            {
                if (statement.RetryPolicy != null)
                {
                    _retryPolicy = statement.RetryPolicy;   
                }
                _autoPage = statement.AutoPage;
            }
        }

        /// <summary>
        /// Fills the common properties of the RowSet
        /// </summary>
        private RowSet FillRowSet(RowSet rs, ResultResponse response)
        {
            if (response != null)
            {
                if (response.Output.TraceId != null)
                {
                    rs.Info.SetQueryTrace(new QueryTrace(response.Output.TraceId.Value, _session));   
                }
                if (response.Warnings != null)
                {
                    rs.Info.Warnings = response.Warnings;
                    //Log the warnings
                    for (var i = 0; i < response.Warnings.Length; i++)
                    {
                        var query = "BATCH";
                        if (_request is QueryRequest)
                        {
                            query = ((QueryRequest) _request).Query;
                        }
                        else if (_statement is BoundStatement)
                        {
                            query = ((BoundStatement) _statement).PreparedStatement.Cql;
                        }
                        _logger.Warning("Received warning ({0} of {1}): \"{2}\" for \"{3}\"", i + 1, response.Warnings.Length, response.Warnings[i], query);
                    }
                }
                rs.Info.IncomingPayload = response.CustomPayload;
            }
            rs.Info.SetTriedHosts(_triedHosts.Keys.ToList());
            if (_request is ICqlRequest)
            {
                rs.Info.SetAchievedConsistency(((ICqlRequest)_request).Consistency);
            }
            rs.AutoPage = _autoPage;
            if (_autoPage && rs.PagingState != null && _request is IQueryRequest && typeof(T) == typeof(RowSet))
            {
                //Automatic paging is enabled and there are following result pages
                //Set the Handler for fetching the next page.
                rs.FetchNextPage = pagingState =>
                {
                    if (_session.IsDisposed)
                    {
                        _logger.Warning("Trying to page results using a Session already disposed.");
                        return new RowSet();
                    }
                    ((IQueryRequest)_request).PagingState = pagingState;
                    var task = new RequestHandler<RowSet>(_session, _request, _statement).Send();
                    TaskHelper.WaitToComplete(task, _session.Configuration.ClientOptions.QueryAbortTimeout);
                    return (RowSet)(object)task.Result;
                };
            }
            return rs;
        }

        /// <summary>
        /// Gets a connection from the next host according to the load balancing policy
        /// </summary>
        /// <exception cref="NoHostAvailableException"/>
        /// <exception cref="InvalidQueryException">When keyspace does not exist</exception>
        /// <exception cref="UnsupportedProtocolVersionException"/>
        internal Connection GetNextConnection(IStatement statement)
        {
            var hostEnumerable = _session.Policies.LoadBalancingPolicy.NewQueryPlan(_session.Keyspace, statement);
            //hostEnumerable GetEnumerator will return a NEW enumerator, making this call thread safe
            foreach (var host in hostEnumerable)
            {
                if (!host.IsConsiderablyUp)
                {
                    continue;
                }
                Host = host;
                _triedHosts[host.Address] = null;
                try
                {
                    var distance = _session.Policies.LoadBalancingPolicy.Distance(host);
                    var hostPool = _session.GetOrCreateConnectionPool(host, distance);
                    var connection = hostPool.BorrowConnection();
                    if (connection == null)
                    {
                        continue;
                    }
                    connection.Keyspace = _session.Keyspace;
                    return connection;
                }
                catch (SocketException ex)
                {
                    host.SetDown();
                    _triedHosts[host.Address] = ex;
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
                    _logger.Error(ex);
                    _triedHosts[host.Address] = ex;
                }
            }
            Host = null;
            throw new NoHostAvailableException(_triedHosts);
        }

        /// <summary>
        /// Gets the retry decision based on the exception from Cassandra
        /// </summary>
        public RetryDecision GetRetryDecision(Exception ex)
        {
            var decision = RetryDecision.Rethrow();
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
                var e = (ReadTimeoutException) ex;
                decision = _retryPolicy.OnReadTimeout(_statement, e.ConsistencyLevel, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, e.WasDataRetrieved, _retryCount);
            }
            else if (ex is WriteTimeoutException)
            {
                var e = (WriteTimeoutException) ex;
                decision = _retryPolicy.OnWriteTimeout(_statement, e.ConsistencyLevel, e.WriteType, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, _retryCount);
            }
            else if (ex is UnavailableException)
            {
                var e = (UnavailableException) ex;
                decision = _retryPolicy.OnUnavailable(_statement, e.Consistency, e.RequiredReplicas, e.AliveReplicas, _retryCount);
            }
            return decision;
        }

        /// <summary>
        /// Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
        /// </summary>
        private void HandleException(Exception ex)
        {
            _logger.Info("RequestHandler received exception {0}", ex.ToString());
            if (ex is PreparedQueryNotFoundException && (_statement is BoundStatement || _statement is BatchStatement))
            {
                PrepareAndRetry(((PreparedQueryNotFoundException)ex).UnknownId);
                return;
            }
            if (ex is OperationTimedOutException)
            {
                OnTimeout(ex);
                return;
            }
            if (ex is SocketException)
            {
                _logger.Verbose("Socket error " + ((SocketException)ex).SocketErrorCode);
                Host.SetDown();
            }
            var decision = GetRetryDecision(ex);
            switch (decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Rethrow:
                    _tcs.TrySetException(ex);
                    break;
                case RetryDecision.RetryDecisionType.Ignore:
                    //The error was ignored by the RetryPolicy
                    //Try to give a decent response
                    if (typeof(T).IsAssignableFrom(typeof(RowSet)))
                    {
                        var rs = new RowSet();
                        _tcs.TrySetResult((T)(object) FillRowSet(rs, null));
                    }
                    else
                    {
                        _tcs.TrySetResult(default(T));
                    }
                    break;
                case RetryDecision.RetryDecisionType.Retry:
                    //Retry the Request using the new consistency level
                    Retry(decision.RetryConsistencyLevel);
                    break;
            }
        }

        /// <summary>
        /// It handles the steps required when there is a client-level read timeout.
        /// It is invoked by a thread from the default TaskScheduler
        /// </summary>
        private void OnTimeout(Exception ex)
        {
            _logger.Warning(ex.Message);
            if (_session == null || Host == null || _connection == null)
            {
                _logger.Error("Session, Host and Connection must not be null");
                return;
            }
            var pool = _session.GetExistingPool(Host);
            pool.CheckHealth(_connection);
            if (_retryOnTimeout || _request is PrepareRequest)
            {
                TrySend();
                return;
            }
            _tcs.TrySetException(ex);
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
            var statement = new PreparedStatement(prepared.Metadata, prepared.QueryId, ((PrepareRequest) _request).Query, _connection.Keyspace, _session.BinaryProtocolVersion)
            {
                IncomingPayload = ((ResultResponse) response).CustomPayload
            };
            _tcs.TrySetResult((T)(object)statement);
        }

        /// <summary>
        /// Gets the resulting RowSet and transitions the task to completed.
        /// </summary>
        private void HandleRowSetResult(AbstractResponse response)
        {
            ValidateResult(response);
            var resultResponse = (ResultResponse)response;
            if (resultResponse.Output is OutputSchemaChange)
            {
                //Schema changes need to do blocking operations
                HandleSchemaChange(resultResponse);
                return;
            }
            RowSet rs;
            if (resultResponse.Output is OutputRows)
            {
                rs = ((OutputRows)resultResponse.Output).RowSet;
            }
            else
            {
                if (resultResponse.Output is OutputSetKeyspace)
                {
                    _session.Keyspace = ((OutputSetKeyspace)resultResponse.Output).Value;
                }
                rs = new RowSet();
            }
            _tcs.TrySetResult((T)(object)FillRowSet(rs, resultResponse));
        }

        private void HandleSchemaChange(ResultResponse response)
        {
            //Use one of the worker/executor threads to wait on the schema change
            Task.Factory.StartNew(() =>
            {
                _session.Cluster.Metadata.WaitForSchemaAgreement(_connection);
                //Set the result from the worker thread
                _tcs.TrySetResult((T)(object)FillRowSet(new RowSet(), response));
            });
        }

        private void PrepareAndRetry(byte[] id)
        {
            _logger.Info(String.Format("Query {0} is not prepared on {1}, preparing before retrying executing.", BitConverter.ToString(id), Host.Address));
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
            var request = new PrepareRequest(_request.ProtocolVersion, boundStatement.PreparedStatement.Cql);
            if (boundStatement.PreparedStatement.Keyspace != null && _session.Keyspace != boundStatement.PreparedStatement.Keyspace)
            {
                _logger.Warning(String.Format("The statement was prepared using another keyspace, changing the keyspace temporarily to" +
                                              " {0} and back to {1}. Use keyspace and table identifiers in your queries and avoid switching keyspaces.",
                                              boundStatement.PreparedStatement.Keyspace, _session.Keyspace));
                //Use the current task scheduler to avoid blocking on a io worker thread
                Task.Factory.StartNew(() =>
                {
                    //Change the keyspace is a blocking operation
                    _connection.Keyspace = boundStatement.PreparedStatement.Keyspace;
                    _connection.Send(request, ResponseReprepareHandler);
                });
                return;
            }
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
            _logger.Info("Retrying request: {0}", _request.GetType().Name);
            TrySend();
        }

        public Task<T> Send()
        {
            TrySend();
            return _tcs.Task;
        }

        private void TrySend()
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
                HandleException(ex);
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
