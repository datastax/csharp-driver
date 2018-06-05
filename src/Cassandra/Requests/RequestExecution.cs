﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Responses;
using Cassandra.Tasks;

namespace Cassandra.Requests
{
    internal class RequestExecution
    {
        private static readonly Logger Logger = new Logger(typeof(RequestExecution));
        private readonly RequestHandler _parent;
        private readonly ISession _session;
        private readonly IRequest _request;
        private readonly Dictionary<IPEndPoint, Exception> _triedHosts = new Dictionary<IPEndPoint, Exception>();
        private volatile Connection _connection;
        private volatile int _retryCount;
        private volatile OperationState _operation;

        public RequestExecution(RequestHandler parent, ISession session, IRequest request)
        {
            _parent = parent;
            _session = session;
            _request = request;
        }

        public void Cancel()
        {
            if (_operation == null)
            {
                //The request has not been sent yet
                return;
            }
            //_operation can not be assigned to null, so it is safe to use the reference
            _operation.Cancel();
        }

        /// <summary>
        /// Starts a new execution using the current request
        /// </summary>
        /// <param name="useCurrentHost"></param>
        public void Start(bool useCurrentHost = false)
        {
            if (!useCurrentHost)
            {
                //Get a new connection from the next host
                _parent.GetNextConnection(_triedHosts).ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        t.Exception.Handle(_ => true);
                        HandleResponse(t.Exception.InnerException, null);
                        return;
                    }
                    _connection = t.Result;
                    Send(_request, HandleResponse);
                }, TaskContinuationOptions.ExecuteSynchronously);
                return;
            }
            if (_connection == null)
            {
                throw new DriverInternalError("No current connection set");
            }
            Send(_request, HandleResponse);
        }

        private void TryStartNew(bool useCurrentHost)
        {
            try
            {
                Start(useCurrentHost);
            }
            catch (Exception ex)
            {
                //There was an Exception before sending (probably no host is available).
                //This will mark the Task as faulted.
                HandleException(ex);
            }
        }

        /// <summary>
        /// Sends a new request using the active connection
        /// </summary>
        private void Send(IRequest request, Action<Exception, Response> callback)
        {
            var timeoutMillis = Timeout.Infinite;
            if (_parent.Statement != null)
            {
                timeoutMillis = _parent.Statement.ReadTimeoutMillis;
            }
            _operation = _connection.Send(request, callback, timeoutMillis);
        }

        public void HandleResponse(Exception ex, Response response)
        {
            if (_parent.HasCompleted())
            {
                //Do nothing else, another execution finished already set the response
                return;
            }
            try
            {
                if (ex != null)
                {
                    HandleException(ex);
                    return;
                }
                HandleRowSetResult(response);
            }
            catch (Exception handlerException)
            {
                _parent.SetCompleted(handlerException);
            }
        }

        public void Retry(ConsistencyLevel? consistency, bool useCurrentHost)
        {
            _retryCount++;
            if (consistency != null && _request is ICqlRequest)
            {
                //Set the new consistency to be used for the new request
                ((ICqlRequest)_request).Consistency = consistency.Value;
            }
            Logger.Info("Retrying request: {0}", _request.GetType().Name);
            TryStartNew(useCurrentHost);
        }

        private void HandleRowSetResult(Response response)
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
                    ((Session)_session).Keyspace = ((OutputSetKeyspace)resultResponse.Output).Value;
                }
                rs = RowSet.Empty();
            }
            _parent.SetCompleted(null, FillRowSet(rs, resultResponse));
        }

        private void HandleSchemaChange(ResultResponse response)
        {
            var result = FillRowSet(new RowSet(), response);
            //Wait for the schema change before returning the result
            _parent.SetCompleted(result, () => _session.Cluster.Metadata.WaitForSchemaAgreement(_connection));
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
                            query = ((QueryRequest)_request).Query;
                        }
                        else if (_parent.Statement is BoundStatement)
                        {
                            query = ((BoundStatement)_parent.Statement).PreparedStatement.Cql;
                        }
                        Logger.Warning("Received warning ({0} of {1}): \"{2}\" for \"{3}\"", i + 1, response.Warnings.Length, response.Warnings[i], query);
                    }
                }
                rs.Info.IncomingPayload = response.CustomPayload;
            }
            rs.Info.SetTriedHosts(_triedHosts.Keys.ToList());
            if (_request is ICqlRequest)
            {
                rs.Info.SetAchievedConsistency(((ICqlRequest)_request).Consistency);
            }
            SetAutoPage(rs, _session, _parent.Statement);
            return rs;
        }

        private void SetAutoPage(RowSet rs, ISession session, IStatement statement)
        {
            rs.AutoPage = statement != null && statement.AutoPage;
            if (rs.AutoPage && rs.PagingState != null && _request is IQueryRequest)
            {
                // Automatic paging is enabled and there are following result pages
                rs.SetFetchNextPageHandler(pagingState =>
                {
                    if (_session.IsDisposed)
                    {
                        Logger.Warning("Trying to page results using a Session already disposed.");
                        return Task.FromResult(RowSet.Empty());
                    }

                    var request = (IQueryRequest) RequestHandler.GetRequest(statement, _parent.Serializer,
                        session.Cluster.Configuration);
                    request.PagingState = pagingState;
                    return new RequestHandler(session, _parent.Serializer, request, statement).Send();
                }, _session.Cluster.Configuration.ClientOptions.QueryAbortTimeout);
            }
        }

        /// <summary>
        /// Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
        /// </summary>
        private void HandleException(Exception ex)
        {
            Logger.Info("RequestHandler received exception {0}", ex.ToString());
            if (ex is PreparedQueryNotFoundException &&
                (_parent.Statement is BoundStatement || _parent.Statement is BatchStatement))
            {
                PrepareAndRetry(((PreparedQueryNotFoundException)ex).UnknownId);
                return;
            }
            if (ex is NoHostAvailableException)
            {
                //A NoHostAvailableException when trying to retrieve
                _parent.SetNoMoreHosts((NoHostAvailableException)ex, this);
                return;
            }
            var c = _connection;
            if (c != null)
            {
                _triedHosts[c.Address] = ex;
            }
            if (ex is OperationTimedOutException)
            {
                Logger.Warning(ex.Message);
                var connection = _connection;
                if (connection == null)
                {
                    Logger.Error("Host and Connection must not be null");
                }
                else
                {
                    // Checks how many timed out operations are in the connection
                    ((Session)_session).CheckHealth(connection);
                }
            }
            var decision = GetRetryDecision(
                ex, _parent.RetryPolicy, _parent.Statement, _session.Cluster.Configuration, _retryCount);
            switch (decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Rethrow:
                    _parent.SetCompleted(ex);
                    break;
                case RetryDecision.RetryDecisionType.Ignore:
                    // The error was ignored by the RetryPolicy, return an empty rowset
                    _parent.SetCompleted(null, FillRowSet(RowSet.Empty(), null));
                    break;
                case RetryDecision.RetryDecisionType.Retry:
                    //Retry the Request using the new consistency level
                    Retry(decision.RetryConsistencyLevel, decision.UseCurrentHost);
                    break;
            }
        }

        /// <summary>
        /// Gets the retry decision based on the exception from Cassandra
        /// </summary>
        public static RetryDecision GetRetryDecision(Exception ex, IExtendedRetryPolicy policy, IStatement statement,
                                                     Configuration config, int retryCount)
        {
            if (ex is SocketException)
            {
                Logger.Verbose("Socket error " + ((SocketException)ex).SocketErrorCode);
                return policy.OnRequestError(statement, config, ex, retryCount);
            }
            if (ex is OverloadedException || ex is IsBootstrappingException || ex is TruncateException)
            {
                return policy.OnRequestError(statement, config, ex, retryCount);
            }
            if (ex is ReadTimeoutException)
            {
                var e = (ReadTimeoutException)ex;
                return policy.OnReadTimeout(statement, e.ConsistencyLevel, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, e.WasDataRetrieved, retryCount);
            }
            if (ex is WriteTimeoutException)
            {
                var e = (WriteTimeoutException)ex;
                return policy.OnWriteTimeout(statement, e.ConsistencyLevel, e.WriteType, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, retryCount);
            }
            if (ex is UnavailableException)
            {
                var e = (UnavailableException)ex;
                return policy.OnUnavailable(statement, e.Consistency, e.RequiredReplicas, e.AliveReplicas, retryCount);
            }
            if (ex is OperationTimedOutException)
            {
                if (statement == null)
                {
                    // For PREPARE requests, retry on next host
                    return RetryDecision.Retry(null, false);
                }
                // Delegate on retry policy
                return policy.OnRequestError(statement, config, ex, retryCount);
            }
            // Any other Exception just throw it
            return RetryDecision.Rethrow();
        }

        /// <summary>
        /// Sends a prepare request before retrying the statement
        /// </summary>
        private void PrepareAndRetry(byte[] id)
        {
            Logger.Info(String.Format("Query {0} is not prepared on {1}, preparing before retrying executing.", BitConverter.ToString(id), _connection.Address));
            BoundStatement boundStatement = null;
            if (_parent.Statement is BoundStatement)
            {
                boundStatement = (BoundStatement)_parent.Statement;
            }
            else if (_parent.Statement is BatchStatement)
            {
                var batch = (BatchStatement)_parent.Statement;

                bool SearchBoundStatement(Statement s) =>
                    s is BoundStatement && ((BoundStatement) s).PreparedStatement.Id.SequenceEqual(id);
                boundStatement = (BoundStatement)batch.Queries.FirstOrDefault(SearchBoundStatement);
            }
            if (boundStatement == null)
            {
                throw new DriverInternalError("Expected Bound or batch statement");
            }
            var request = new PrepareRequest(boundStatement.PreparedStatement.Cql);
            if (boundStatement.PreparedStatement.Keyspace != null && _session.Keyspace != boundStatement.PreparedStatement.Keyspace)
            {
                Logger.Warning(String.Format("The statement was prepared using another keyspace, changing the keyspace temporarily to" +
                                              " {0} and back to {1}. Use keyspace and table identifiers in your queries and avoid switching keyspaces.",
                                              boundStatement.PreparedStatement.Keyspace, _session.Keyspace));

                _connection
                    .SetKeyspace(boundStatement.PreparedStatement.Keyspace)
                    .ContinueSync(_ =>
                    {
                        Send(request, ReprepareResponseHandler);
                        return true;
                    });
                return;
            }
            Send(request, ReprepareResponseHandler);
        }

        /// <summary>
        /// Handles the response of a (re)prepare request and retries to execute on the same connection
        /// </summary>
        private void ReprepareResponseHandler(Exception ex, Response response)
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
                Send(_request, HandleResponse);
            }
            catch (Exception exception)
            {
                //There was an issue while sending
                _parent.SetCompleted(exception);
            }
        }

        private static void ValidateResult(Response response)
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
