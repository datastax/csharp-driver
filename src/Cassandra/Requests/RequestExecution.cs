using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Responses;
using Cassandra.Tasks;

namespace Cassandra.Requests
{
    internal class RequestExecution<T> where T : class
    {
        // ReSharper disable once StaticMemberInGenericType
        private readonly static Logger Logger = new Logger(typeof(Session));
        private readonly RequestHandler<T> _parent;
        private readonly ISession _session;
        private readonly IRequest _request;
        private readonly Dictionary<IPEndPoint, Exception> _triedHosts = new Dictionary<IPEndPoint, Exception>();
        private volatile Connection _connection;
        private int _retryCount;
        private volatile OperationState _operation;

        public RequestExecution(RequestHandler<T> parent, ISession session, IRequest request)
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
            _operation = _connection.Send(request, callback);
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
                if (typeof(T) == typeof(RowSet))
                {
                    HandleRowSetResult(response);
                    return;
                }
                if (typeof(T) == typeof(PreparedStatement))
                {
                    HandlePreparedResult(response);
                    return;
                }
                throw new DriverInternalError(String.Format("RequestExecution with type {0} is not supported", typeof(T).FullName));
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
                rs = new RowSet();
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
        private T FillRowSet(RowSet rs, ResultResponse response)
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
            return (T)(object)rs;
        }

        private void SetAutoPage(RowSet rs, ISession session, IStatement statement)
        {
            rs.AutoPage = statement != null && statement.AutoPage;
            if (rs.AutoPage && rs.PagingState != null && _request is IQueryRequest && typeof(T) == typeof(RowSet))
            {
                //Automatic paging is enabled and there are following result pages
                //Set the Handler for fetching the next page.
                rs.FetchNextPage = pagingState =>
                {
                    if (_session.IsDisposed)
                    {
                        Logger.Warning("Trying to page results using a Session already disposed.");
                        return new RowSet();
                    }
                    var request = (IQueryRequest)RequestHandler<RowSet>.GetRequest(statement, session.BinaryProtocolVersion, session.Cluster.Configuration);
                    request.PagingState = pagingState;
                    var task = new RequestHandler<RowSet>(session, request, statement).Send();
                    TaskHelper.WaitToComplete(task, session.Cluster.Configuration.ClientOptions.QueryAbortTimeout);
                    return (RowSet)(object)task.Result;
                };
            }
        }

        /// <summary>
        /// Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
        /// </summary>
        private void HandleException(Exception ex)
        {
            Logger.Info("RequestHandler received exception {0}", ex.ToString());
            if (ex is PreparedQueryNotFoundException && (_parent.Statement is BoundStatement || _parent.Statement is BatchStatement))
            {
                PrepareAndRetry(((PreparedQueryNotFoundException)ex).UnknownId);
                return;
            }
            if (ex is OperationTimedOutException)
            {
                OnTimeout(ex);
                return;
            }
            if (ex is NoHostAvailableException)
            {
                //A NoHostAvailableException when trying to retrieve
                _parent.SetNoMoreHosts((NoHostAvailableException)ex, this);
                return;
            }
            if (ex is SocketException)
            {
                Logger.Verbose("Socket error " + ((SocketException)ex).SocketErrorCode);
                var c = _connection;
                if (c != null)
                {
                    _parent.SetHostDown(c);
                }
                else
                {
                    //The exception happened while trying to acquire a connection
                    //nothing to do here
                }
            }
            var decision = GetRetryDecision(ex, _parent.RetryPolicy, _parent.Statement, _retryCount);
            switch (decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Rethrow:
                    _parent.SetCompleted(ex);
                    break;
                case RetryDecision.RetryDecisionType.Ignore:
                    //The error was ignored by the RetryPolicy
                    //Try to give a decent response
                    if (typeof(T).IsAssignableFrom(typeof(RowSet)))
                    {
                        var rs = new RowSet();
                        _parent.SetCompleted(null, FillRowSet(rs, null));
                    }
                    else
                    {
                        _parent.SetCompleted(null, default(T));
                    }
                    break;
                case RetryDecision.RetryDecisionType.Retry:
                    //Retry the Request using the new consistency level
                    Retry(decision.RetryConsistencyLevel, decision.UseCurrentHost);
                    break;
            }
        }

        /// <summary>
        /// It handles the steps required when there is a client-level read timeout.
        /// It is invoked by a thread from the default TaskScheduler
        /// </summary>
        private void OnTimeout(Exception ex)
        {
            Logger.Warning(ex.Message);
            if (_session == null || _connection == null)
            {
                Logger.Error("Session, Host and Connection must not be null");
                return;
            }
            var pool = ((Session)_session).GetExistingPool(_connection);
            pool.CheckHealth(_connection);
            if (_session.Cluster.Configuration.QueryOptions.RetryOnTimeout || _request is PrepareRequest)
            {
                if (_parent.HasCompleted())
                {
                    return;
                }
                TryStartNew(false);
                return;
            }
            _parent.SetCompleted(ex);
        }

        /// <summary>
        /// Gets the retry decision based on the exception from Cassandra
        /// </summary>
        public static RetryDecision GetRetryDecision(Exception ex, IRetryPolicy policy, IStatement statement, int retryCount)
        {
            var decision = RetryDecision.Rethrow();
            if (ex is SocketException)
            {
                decision = RetryDecision.Retry(null, false);
            }
            else if (ex is OverloadedException || ex is IsBootstrappingException || ex is TruncateException)
            {
                decision = RetryDecision.Retry(null, false);
            }
            else if (ex is ReadTimeoutException)
            {
                var e = (ReadTimeoutException)ex;
                decision = policy.OnReadTimeout(statement, e.ConsistencyLevel, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, e.WasDataRetrieved, retryCount);
            }
            else if (ex is WriteTimeoutException)
            {
                var e = (WriteTimeoutException)ex;
                decision = policy.OnWriteTimeout(statement, e.ConsistencyLevel, e.WriteType, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, retryCount);
            }
            else if (ex is UnavailableException)
            {
                var e = (UnavailableException)ex;
                decision = policy.OnUnavailable(statement, e.Consistency, e.RequiredReplicas, e.AliveReplicas, retryCount);
            }
            return decision;
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
        /// <summary>
        /// Creates the prepared statement and transitions the task to completed
        /// </summary>
        private void HandlePreparedResult(Response response)
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
            object statement = new PreparedStatement(prepared.Metadata, prepared.QueryId, ((PrepareRequest) _request).Query, _connection.Keyspace,
                _session.BinaryProtocolVersion)
            {
                IncomingPayload = ((ResultResponse)response).CustomPayload
            };
            _parent.SetCompleted(null, (T)statement);
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
