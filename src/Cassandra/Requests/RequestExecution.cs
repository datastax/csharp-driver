//
//       Copyright DataStax, Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Cassandra.Connections;
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

        /// <summary>
        /// Host that was queried last in this execution. It can be null in case there was no attempt to send the request yet.
        /// </summary>
        private volatile Host _host;

        public RequestExecution(IRequestHandler parent, IInternalSession session, IRequest request)
        {
            _parent = parent;
            _session = session;
            _request = request;
            _host = null;
        }

        public void Cancel()
        {
            // if null then the request has not been sent yet
            _operation?.Cancel();
        }

        /// <inheritdoc />
        public Host Start(bool currentHostRetry)
        {
            if (currentHostRetry && _host != null)
            {
                SendToCurrentHostAsync().Forget();
                return _host;
            }

            // fail fast: try to choose a host before leaving this thread
            var validHost = _parent.GetNextValidHost(_triedHosts);

            SendToNextHostAsync(validHost).Forget();
            return validHost.Host;
        }

        /// <summary>
        /// Gets a new connection to the current host and send the request with it. Useful for retries on the same host.
        /// </summary>
        private async Task SendToCurrentHostAsync()
        {
            try
            {
                // host needs to be re-validated using the load balancing policy
                _connection = await _parent.ValidateHostAndGetConnectionAsync(_host, _triedHosts).ConfigureAwait(false);
                if (_connection != null)
                {
                    Send(_request, HandleResponse);
                    return;
                }

                RequestExecution.Logger.Warning("RequestHandler could not obtain a connection while attempting to retry with the current host.");
            }
            catch (Exception ex)
            {
                RequestExecution.Logger.Warning("RequestHandler received exception while attempting to retry with the current host: {0}", ex.ToString());
            }

            RetryExecution(false);
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
                Send(_request, HandleResponse);
            }
            catch (Exception ex)
            {
                _host = validHost.Host;
                HandleResponse(ex, null);
            }
        }

        /// <summary>
        /// Useful method to retry the execution safely. While <see cref="Start"/> throws exceptions,
        /// this method catches them and marks the <see cref="_parent"/> request as complete,
        /// making it suitable to be called in a fire and forget manner.
        /// </summary>
        private void RetryExecution(bool currentHostRetry)
        {
            try
            {
                Start(currentHostRetry);
            }
            catch (Exception ex)
            {
                //There was an Exception before sending (probably no host is available).
                //This will mark the Task as faulted.
                HandleResponse(ex, null);
            }
        }

        /// <summary>
        /// Sends a new request using the active connection
        /// </summary>
        private void Send(IRequest request, Action<Exception, Response> callback)
        {
            var timeoutMillis = _parent.RequestOptions.ReadTimeoutMillis;
            if (_parent.Statement != null && _parent.Statement.ReadTimeoutMillis > 0)
            {
                timeoutMillis = _parent.Statement.ReadTimeoutMillis;
            }

            _operation = _connection.Send(request, callback, timeoutMillis);
        }

        private void HandleResponse(Exception ex, Response response)
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

        private void Retry(ConsistencyLevel? consistency, bool useCurrentHost)
        {
            _retryCount++;
            if (consistency != null && _request is ICqlRequest request)
            {
                //Set the new consistency to be used for the new request
                request.Consistency = consistency.Value;
            }
            RequestExecution.Logger.Info("Retrying request: {0}", _request.GetType().Name);
            RetryExecution(useCurrentHost);
        }

        private void HandleRowSetResult(Response response)
        {
            RequestExecution.ValidateResult(response);
            var resultResponse = (ResultResponse)response;
            if (resultResponse.Output is OutputSchemaChange schemaChange)
            {
                //Schema changes need to do blocking operations
                HandleSchemaChange(resultResponse, schemaChange);
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
            _parent.SetCompleted(null, FillRowSet(rs, resultResponse));
        }

        private void HandleSchemaChange(ResultResponse response, OutputSchemaChange schemaChange)
        {
            var result = FillRowSet(new RowSet(), response);

            // This is a schema change so initialize schema in agreement as false
            result.Info.SetSchemaInAgreement(false);

            // Wait for the schema change before returning the result
            _parent.SetCompleted(
                result,
                () =>
                {
                    var schemaAgreed = _session.Cluster.Metadata.WaitForSchemaAgreement(_connection);
                    result.Info.SetSchemaInAgreement(schemaAgreed);
                    try
                    {
                        TaskHelper.WaitToComplete(
                            _session.InternalCluster.GetControlConnection().HandleSchemaChangeEvent(schemaChange.SchemaChangeEventArgs, true),
                            _session.Cluster.Configuration.ProtocolOptions.MaxSchemaAgreementWaitSeconds * 1000);
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
                rs.SetFetchNextPageHandler(pagingState =>
                {
                    if (_session.IsDisposed)
                    {
                        RequestExecution.Logger.Warning("Trying to page results using a Session already disposed.");
                        return Task.FromResult(RowSet.Empty());
                    }

                    var request = (IQueryRequest)_parent.BuildRequest();
                    request.PagingState = pagingState;
                    return _session.Cluster.Configuration.RequestHandlerFactory.Create(session, _parent.Serializer, request, statement, _parent.RequestOptions).SendAsync();
                }, _parent.RequestOptions.QueryAbortTimeout);
            }
        }

        /// <summary>
        /// Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
        /// </summary>
        private void HandleException(Exception ex)
        {
            RequestExecution.Logger.Info("RequestHandler received exception {0}", ex.ToString());
            if (ex is PreparedQueryNotFoundException foundException &&
                (_parent.Statement is BoundStatement || _parent.Statement is BatchStatement))
            {
                PrepareAndRetry(foundException.UnknownId);
                return;
            }
            if (ex is NoHostAvailableException exception)
            {
                //A NoHostAvailableException when trying to retrieve
                _parent.SetNoMoreHosts(exception, this);
                return;
            }
            var c = _connection;
            if (c != null)
            {
                _triedHosts[c.Address] = ex;
            }
            if (ex is OperationTimedOutException)
            {
                RequestExecution.Logger.Warning(ex.Message);
                var connection = _connection;
                if (connection == null)
                {
                    RequestExecution.Logger.Error("Host and Connection must not be null");
                }
                else
                {
                    // Checks how many timed out operations are in the connection
                    _session.CheckHealth(connection);
                }
            }
            var decision = RequestExecution.GetRetryDecision(
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
        internal static RetryDecision GetRetryDecision(
            Exception ex, IExtendedRetryPolicy policy, IStatement statement, Configuration config, int retryCount)
        {
            if (ex is SocketException exception)
            {
                RequestExecution.Logger.Verbose("Socket error " + exception.SocketErrorCode);
                return policy.OnRequestError(statement, config, exception, retryCount);
            }
            if (ex is OverloadedException || ex is IsBootstrappingException || ex is TruncateException)
            {
                return policy.OnRequestError(statement, config, ex, retryCount);
            }
            if (ex is ReadTimeoutException e)
            {
                return policy.OnReadTimeout(statement, e.ConsistencyLevel, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, e.WasDataRetrieved, retryCount);
            }
            if (ex is WriteTimeoutException e1)
            {
                return policy.OnWriteTimeout(statement, e1.ConsistencyLevel, e1.WriteType, e1.RequiredAcknowledgements, e1.ReceivedAcknowledgements, retryCount);
            }
            if (ex is UnavailableException e2)
            {
                return policy.OnUnavailable(statement, e2.Consistency, e2.RequiredReplicas, e2.AliveReplicas, retryCount);
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
            RequestExecution.Logger.Info(
                $"Query {BitConverter.ToString(id)} is not prepared on {_connection.Address}, preparing before retrying executing.");
            BoundStatement boundStatement = null;
            if (_parent.Statement is BoundStatement statement1)
            {
                boundStatement = statement1;
            }
            else if (_parent.Statement is BatchStatement batch)
            {
                bool SearchBoundStatement(Statement s) =>
                    s is BoundStatement statement && statement.PreparedStatement.Id.SequenceEqual(id);
                boundStatement = (BoundStatement)batch.Queries.FirstOrDefault(SearchBoundStatement);
            }
            if (boundStatement == null)
            {
                throw new DriverInternalError("Expected Bound or batch statement");
            }
            var request = new InternalPrepareRequest(boundStatement.PreparedStatement.Cql);
            if (boundStatement.PreparedStatement.Keyspace != null && _session.Keyspace != boundStatement.PreparedStatement.Keyspace)
            {
                RequestExecution.Logger.Warning("The statement was prepared using another keyspace, changing the keyspace temporarily to" +
                                                $" {boundStatement.PreparedStatement.Keyspace} and back to {_session.Keyspace}. Use keyspace and table identifiers in your queries and avoid switching keyspaces.");

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
                RequestExecution.ValidateResult(response);
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