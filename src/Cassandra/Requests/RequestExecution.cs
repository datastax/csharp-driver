//
//       Copyright DataStax, Inc.
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
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Observers.Abstractions;
using Cassandra.Responses;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra.Requests
{
    internal class RequestExecution : IRequestExecution
    {
        private static readonly Logger Logger = new Logger(typeof(RequestExecution));
        private readonly IRequestHandler _parent;
        private readonly IInternalSession _session;
        private readonly IRequest _request;
        private readonly Dictionary<IPEndPoint, Exception> _triedHosts = new Dictionary<IPEndPoint, Exception>();
        private volatile IConnection _connection;
        private volatile int _retryCount;
        private volatile OperationState _operation;
        private readonly IRequestObserver _requestObserver;
        private readonly RequestTrackingInfo _requestTrackingInfo;

        /// <summary>
        /// Host that was queried last in this execution. It can be null in case there was no attempt to send the request yet.
        /// </summary>
        private volatile Host _host;
        
        public RequestExecution(IRequestHandler parent, IInternalSession session, IRequest request, IRequestObserver requestObserver, RequestTrackingInfo requestTrackingInfo)
        {
            _parent = parent;
            _session = session;
            _request = request;
            _host = null;
            _requestObserver = requestObserver;
            _requestTrackingInfo = requestTrackingInfo;
        }

        public void Cancel()
        {
            // if null then the request has not been sent yet
            _operation?.Cancel();
        }

        /// <inheritdoc />
        public async Task<Host> StartAsync(bool currentHostRetry)
        {
            if (currentHostRetry && _host != null)
            {
                await _requestObserver.OnNodeStartAsync(_host, _requestTrackingInfo).ConfigureAwait(false);

                SendToCurrentHostAsync().Forget();
                return _host;
            }

            // fail fast: try to choose a host before leaving this thread
            var validHost = _parent.GetNextValidHost(_triedHosts);

            await _requestObserver.OnNodeStartAsync(validHost.Host, _requestTrackingInfo).ConfigureAwait(false);

            SendToNextHostAsync(validHost).Forget();            
            return validHost.Host;
        }

        /// <summary>
        /// Gets a new connection to the current host and send the request with it. Useful for retries on the same host.
        /// </summary>
        private async Task SendToCurrentHostAsync()
        {
            var host = _host;
            try
            {
                // host needs to be re-validated using the load balancing policy
                _connection = await _parent.ValidateHostAndGetConnectionAsync(host, _triedHosts).ConfigureAwait(false);
                if (_connection != null)
                {
                    Send(_request, host, HandleResponseAsync);
                    return;
                }

                RequestExecution.Logger.Warning("RequestHandler could not obtain a connection while attempting to retry with the current host.");
            }
            catch (Exception ex)
            {
                RequestExecution.Logger.Warning("RequestHandler received exception while attempting to retry with the current host: {0}", ex.ToString());
            }

            await RetryExecutionAsync(false, host).ConfigureAwait(false);
        }

        /// <summary>
        /// Attempts to obtain a connection to the provided <paramref name="validHost"/> and send the request with it.
        /// If no connection could be obtained for the provided host, then attempts to obtain a connection
        /// for the next host, following the query plan.
        /// </summary>
        /// <param name="validHost">First host to which the method tries to obtain a connection.</param>
        /// <returns></returns>
        private async Task SendToNextHostAsync(ValidHost validHost)
        {
            try
            {
                IConnection connection = null;
                while (connection == null)
                {
                    connection = await _parent.GetConnectionToValidHostAsync(validHost, _triedHosts).ConfigureAwait(false);
                    if (connection == null)
                    {
                        validHost = _parent.GetNextValidHost(_triedHosts);
                    }
                }

                _connection = connection;
                _host = validHost.Host;
                Send(_request, validHost.Host, HandleResponseAsync);
            }
            catch (Exception ex)
            {
                _host = validHost.Host;
                await HandleResponseAsync(RequestError.CreateClientError(ex, true), null, validHost.Host).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Useful method to retry the execution safely. While <see cref="StartAsync"/> throws exceptions,
        /// this method catches them and marks the <see cref="_parent"/> request as complete,
        /// making it suitable to be called in a fire and forget manner.
        /// </summary>
        private async Task RetryExecutionAsync(bool currentHostRetry, Host host)
        {
            try
            {
                await StartAsync(currentHostRetry).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                //There was an Exception before sending (probably no host is available).
                //This will mark the Task as faulted.
                await HandleResponseAsync(RequestError.CreateClientError(ex, true), null, host).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Sends a new request using the active connection
        /// </summary>
        private void Send(IRequest request, Host host, Func<IRequestError, Response, Host, Task> callback)
        {
            var timeoutMillis = _parent.RequestOptions.ReadTimeoutMillis;
            if (_parent.Statement != null && _parent.Statement.ReadTimeoutMillis > 0)
            {
                timeoutMillis = _parent.Statement.ReadTimeoutMillis;
            }

            _operation = _connection.Send(request, (error, response) => callback(error, response, host), timeoutMillis);
        }

        private async Task HandleResponseAsync(IRequestError error, Response response, Host host)
        {
            if (_parent.HasCompleted())
            {
                //Do nothing else, another execution finished already set the response
                return;
            }
            try
            {
                if (error?.Exception != null)
                {
                    await HandleRequestErrorAsync(error, host).ConfigureAwait(false);
                    return;
                }
                await HandleRowSetResultAsync(response).ConfigureAwait(false);
            }
            catch (Exception handlerException)
            {
                await _parent.SetCompletedAsync(handlerException).ConfigureAwait(false);
            }
        }

        private Task RetryAsync(ConsistencyLevel? consistency, bool useCurrentHost, Host host)
        {
            _retryCount++;
            if (consistency != null && _request is ICqlRequest request)
            {
                //Set the new consistency to be used for the new request
                request.Consistency = consistency.Value;
            }
            RequestExecution.Logger.Info("Retrying request: {0}", _request.GetType().Name);
            return RetryExecutionAsync(useCurrentHost, host);
        }

        private async Task HandleRowSetResultAsync(Response response)
        {
            await _requestObserver.OnNodeSuccessAsync(_host, _requestTrackingInfo).ConfigureAwait(false);
            RequestExecution.ValidateResult(response);
            var resultResponse = (ResultResponse)response;
            if (resultResponse.Output is OutputSchemaChange schemaChange)
            {
                //Schema changes need to do blocking operations
                await HandleSchemaChangeAsync(resultResponse, schemaChange).ConfigureAwait(false);
                return;
            }
            RowSet rs;
            if (resultResponse.Output is OutputRows rows)
            {
                rs = rows.RowSet;
            }
            else
            {
                if (resultResponse.Output is OutputSetKeyspace keyspace)
                {
                    _session.Keyspace = keyspace.Value;
                }
                rs = RowSet.Empty();
            }
            await _parent.SetCompletedAsync(null, FillRowSet(rs, resultResponse)).ConfigureAwait(false);
        }

        private Task HandleSchemaChangeAsync(ResultResponse response, OutputSchemaChange schemaChange)
        {
            var result = FillRowSet(new RowSet(), response);

            // This is a schema change so initialize schema in agreement as false
            result.Info.SetSchemaInAgreement(false);

            // Wait for the schema change before returning the result
            return _parent.SetCompletedAsync(
                result,
                async () =>
                {
                    var schemaAgreed = await _session.Cluster.Metadata.WaitForSchemaAgreementAsync(_connection).ConfigureAwait(false);
                    result.Info.SetSchemaInAgreement(schemaAgreed);
                    try
                    {
                        await _session.InternalCluster.GetControlConnection().HandleSchemaChangeEvent(schemaChange.SchemaChangeEventArgs, true)
                                      .WaitToCompleteAsync(_session.Cluster.Configuration.ProtocolOptions.MaxSchemaAgreementWaitSeconds * 1000).ConfigureAwait(false);
                    }
                    catch (TimeoutException)
                    {
                        RequestExecution.Logger.Warning("Schema refresh triggered by a SCHEMA_CHANGE response timed out.");
                    }
                });
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
                        if (_request is QueryRequest queryRequest)
                        {
                            query = queryRequest.Query;
                        }
                        else if (_parent.Statement is BoundStatement statement)
                        {
                            query = statement.PreparedStatement.Cql;
                        }
                        RequestExecution.Logger.Warning(
                            "Received warning ({0} of {1}): \"{2}\" for \"{3}\"", i + 1, response.Warnings.Length, response.Warnings[i], query);
                    }
                }

                if (response.NewResultMetadata != null && _parent.Statement is BoundStatement boundStatement)
                {
                    // We've sent an EXECUTE request and the server is notifying the client that the result
                    // metadata changed
                    boundStatement.PreparedStatement.UpdateResultMetadata(response.NewResultMetadata);
                }

                rs.Info.IncomingPayload = response.CustomPayload;
            }
            rs.Info.SetTriedHosts(_triedHosts.Keys.ToList());
            if (_request is ICqlRequest request)
            {
                rs.Info.SetAchievedConsistency(request.Consistency);
            }
            SetAutoPage(rs, _session);
            return rs;
        }

        private void SetAutoPage(RowSet rs, IInternalSession session)
        {
            var statement = _parent.Statement;
            rs.AutoPage = statement != null && statement.AutoPage;
            if (rs.AutoPage && rs.PagingState != null && _request is IQueryRequest)
            {
                // Automatic paging is enabled and there are following result pages
                rs.SetFetchNextPageHandler(async pagingState =>
                {
                    if (_session.IsDisposed)
                    {
                        RequestExecution.Logger.Warning("Trying to page results using a Session already disposed.");
                        return RowSet.Empty();
                    }

                    var request = (IQueryRequest)_parent.BuildRequest();
                    request.PagingState = pagingState;
                    var handler = await _session.Cluster.Configuration.RequestHandlerFactory.CreateAsync(
                        session, _parent.Serializer, request, statement, _parent.RequestOptions).ConfigureAwait(false);
                    return await handler.SendAsync().ConfigureAwait(false);
                }, _parent.RequestOptions.QueryAbortTimeout, _session.MetricsManager);
            }
        }

        /// <summary>
        /// Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
        /// </summary>
        private async Task HandleRequestErrorAsync(IRequestError error, Host host)
        {
            var ex = error.Exception;
            if (ex is PreparedQueryNotFoundException foundException &&
                (_parent.Statement is BoundStatement || _parent.Statement is BatchStatement))
            {
                RequestExecution.Logger.Info(
                    "Query {0} is not prepared on {1}, preparing before retrying the request.",
                    BitConverter.ToString(foundException.UnknownId),
                    _connection.EndPoint.EndpointFriendlyName);
                PrepareAndRetry(foundException, host);
                return;
            }

            RequestExecution.Logger.Info("RequestHandler received exception {0}", ex.ToString());

            if (ex is NoHostAvailableException exception)
            {
                //A NoHostAvailableException when trying to retrieve
                _parent.SetNoMoreHosts(exception, this);
                return;
            }

            _triedHosts[host.Address] = ex;
            if (ex is OperationTimedOutException)
            {
                RequestExecution.Logger.Warning(ex.Message);
                var connection = _connection;
                if (connection == null)
                {
                    RequestExecution.Logger.Error("Connection must not be null");
                }
                else
                {
                    // Checks how many timed out operations are in the connection
                    _session.CheckHealth(host, connection);
                }
            }

            var retryInformation = GetRetryDecisionWithReason(
                error, _parent.RetryPolicy, _parent.Statement, _session.Cluster.Configuration, _retryCount);

            switch (retryInformation.Decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Rethrow:
                    await _requestObserver.OnNodeRequestErrorAsync(host, retryInformation.Reason, RetryDecision.RetryDecisionType.Rethrow, _requestTrackingInfo, ex).ConfigureAwait(false);
                    await _parent.SetCompletedAsync(ex).ConfigureAwait(false);
                    break;
                case RetryDecision.RetryDecisionType.Ignore:
                    await _requestObserver.OnNodeRequestErrorAsync(host, retryInformation.Reason, RetryDecision.RetryDecisionType.Ignore, _requestTrackingInfo, ex).ConfigureAwait(false);
                    // The error was ignored by the RetryPolicy, return an empty rowset
                    await _parent.SetCompletedAsync(null, FillRowSet(RowSet.Empty(), null)).ConfigureAwait(false);
                    break;
                case RetryDecision.RetryDecisionType.Retry:
                    await _requestObserver.OnNodeRequestErrorAsync(host, retryInformation.Reason, RetryDecision.RetryDecisionType.Retry, _requestTrackingInfo, ex).ConfigureAwait(false);
                    //Retry the Request using the new consistency level
                    await RetryAsync(retryInformation.Decision.RetryConsistencyLevel, retryInformation.Decision.UseCurrentHost, host).ConfigureAwait(false);
                    break;
                default:
                    await _requestObserver.OnNodeRequestErrorAsync(host, retryInformation.Reason, retryInformation.Decision.DecisionType, _requestTrackingInfo, ex).ConfigureAwait(false);
                    break;
            }
        }
        
        /// <summary>
        /// Gets the retry decision based on the request error
        /// </summary>
        internal static RetryDecisionWithReason GetRetryDecisionWithReason(
            IRequestError error, IExtendedRetryPolicy policy, IStatement statement, Configuration config, int retryCount)
        {
            var ex = error.Exception;
            if (ex is SocketException || ex is OverloadedException || ex is IsBootstrappingException || ex is TruncateException ||
                ex is OperationTimedOutException)
            {
                if (ex is SocketException exception)
                {
                    RequestExecution.Logger.Verbose("Socket error " + exception.SocketErrorCode);
                }

                // For PREPARE requests, retry on next host
                var decision = statement == null && ex is OperationTimedOutException
                    ? RetryDecision.Retry(null, false)
                    : policy.OnRequestError(statement, config, ex, retryCount);
                return new RetryDecisionWithReason(decision, RequestExecution.GetErrorType(error));
            }

            if (ex is ReadTimeoutException e)
            {
                return new RetryDecisionWithReason(
                    policy.OnReadTimeout(
                        statement, 
                        e.ConsistencyLevel, 
                        e.RequiredAcknowledgements, 
                        e.ReceivedAcknowledgements, 
                        e.WasDataRetrieved,
                        retryCount),
                    RequestErrorType.ReadTimeOut
                );
            }

            if (ex is WriteTimeoutException e1)
            {
                return new RetryDecisionWithReason(
                    policy.OnWriteTimeout(
                        statement, 
                        e1.ConsistencyLevel, 
                        e1.WriteType, 
                        e1.RequiredAcknowledgements, 
                        e1.ReceivedAcknowledgements,
                        retryCount),
                    RequestErrorType.WriteTimeOut
                );
            }

            if (ex is UnavailableException e2)
            {
                return new RetryDecisionWithReason(
                    policy.OnUnavailable(statement, e2.Consistency, e2.RequiredReplicas, e2.AliveReplicas, retryCount),
                    RequestErrorType.Unavailable
                );
            }

            // Any other Exception just throw it
            return new RetryDecisionWithReason(RetryDecision.Rethrow(), RequestExecution.GetErrorType(error));
        }

        private static RequestErrorType GetErrorType(IRequestError error)
        {
            if (error.Exception is OperationTimedOutException)
            {
                return RequestErrorType.ClientTimeout;
            }

            if (error.Unsent)
            {
                return RequestErrorType.Unsent;
            }

            return error.IsServerError ? RequestErrorType.Other : RequestErrorType.Aborted;
        }

        /// <summary>
        /// Sends a prepare request before retrying the statement
        /// </summary>
        private void PrepareAndRetry(PreparedQueryNotFoundException ex, Host host)
        {
            BoundStatement boundStatement = null;
            if (_parent.Statement is BoundStatement statement1)
            {
                boundStatement = statement1;
            }
            else if (_parent.Statement is BatchStatement batch)
            {
                bool SearchBoundStatement(Statement s) =>
                    s is BoundStatement statement && statement.PreparedStatement.Id.SequenceEqual(ex.UnknownId);
                boundStatement = (BoundStatement)batch.Queries.FirstOrDefault(SearchBoundStatement);
            }
            if (boundStatement == null)
            {
                throw new DriverInternalError("Expected Bound or batch statement");
            }

            var preparedKeyspace = boundStatement.PreparedStatement.Keyspace;
            var request = new PrepareRequest(_parent.Serializer, boundStatement.PreparedStatement.Cql, preparedKeyspace, null);

            if (!_parent.Serializer.ProtocolVersion.SupportsKeyspaceInRequest() &&
                preparedKeyspace != null && _session.Keyspace != preparedKeyspace)
            {
                Logger.Warning(string.Format("The statement was prepared using another keyspace, changing the keyspace temporarily to" +
                                              " {0} and back to {1}. Use keyspace and table identifiers in your queries and avoid switching keyspaces.",
                    preparedKeyspace, _session.Keyspace));

                _connection
                    .SetKeyspace(preparedKeyspace)
                    .ContinueSync(_ =>
                    {
                        Send(request, host, NewReprepareResponseHandler(ex));
                        return true;
                    });
                return;
            }
            Send(request, host, NewReprepareResponseHandler(ex));
        }

        /// <summary>
        /// Handles the response of a (re)prepare request and retries to execute on the same connection
        /// </summary>
        private Func<IRequestError, Response, Host, Task> NewReprepareResponseHandler(
            PreparedQueryNotFoundException originalError)
        {
            async Task ResponseHandler(IRequestError error, Response response, Host host)
            {
                try
                {
                    if (error?.Exception != null)
                    {
                        await HandleRequestErrorAsync(error, host).ConfigureAwait(false);
                    }

                    RequestExecution.ValidateResult(response);
                    var output = ((ResultResponse) response).Output;
                    if (!(output is OutputPrepared outputPrepared))
                    {
                        await _parent.SetCompletedAsync(new DriverInternalError("Expected prepared response, obtained " + output.GetType().FullName));
                        return;
                    }

                    if (!outputPrepared.QueryId.SequenceEqual(originalError.UnknownId))
                    {
                        await _parent.SetCompletedAsync(new PreparedStatementIdMismatchException(
                            originalError.UnknownId, outputPrepared.QueryId)).ConfigureAwait(false);
                        return;
                    }

                    if (_parent.Statement is BoundStatement boundStatement)
                    {
                        // Use the latest result metadata id
                        boundStatement.PreparedStatement.UpdateResultMetadata(
                            new ResultMetadata(outputPrepared.ResultMetadataId, outputPrepared.ResultRowsMetadata));
                    }

                    Send(_request, host, HandleResponseAsync);
                }
                catch (Exception exception)
                {
                    //There was an issue while sending
                    await _parent.SetCompletedAsync(exception).ConfigureAwait(false);
                }
            }

            return ResponseHandler;
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
