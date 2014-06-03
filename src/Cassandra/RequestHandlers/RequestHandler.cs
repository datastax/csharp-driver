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

namespace Cassandra.RequestHandlers
{
    /// <summary>
    /// Handles a request to cassandra, dealing with host failover and retries on error
    /// </summary>
    internal class RequestHandler<T>
    {
        private static Logger _logger = new Logger(typeof(RequestHandler<object>));
        private TaskCompletionSource<T> _tcs;
        private Session _session;
        private IRequest _request;
        private IStatement _statement;

        public RequestHandler(Session session, IRequest request, IStatement statement)
        {
            _tcs = new TaskCompletionSource<T>();
            _session = session;
            _request = request;
            _statement = statement;
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

        public void HandleResult(Exception ex, AbstractResponse response)
        {
            if (typeof(T) == typeof(RowSet))
            {
                HandleRowSetResult(ex, response);
            }
        }

        private void HandleRowSetResult(Exception exception, AbstractResponse response)
        {
            if (exception != null)
            {
                _tcs.TrySetException(exception);
                return;
            }
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

    /// <summary>
    /// Represents a handler that can get an available connection, send a request and parses the response when received.
    /// </summary>
    abstract class RequestHandler
    {
        private readonly Logger _logger = new Logger(typeof(RequestHandler));
        public CassandraConnection Connection;
        public ConsistencyLevel? Consistency = null;
        /// <summary>
        /// The statement that executed the request
        /// </summary>
        public Statement Statement { get; set; }
        private IEnumerator<Host> _hostsIter = null;
        public IAsyncResult LongActionAc;
        public readonly Dictionary<IPAddress, List<Exception>> InnerExceptions = new Dictionary<IPAddress, List<Exception>>();
        public readonly List<IPAddress> TriedHosts = new List<IPAddress>();
        public int QueryRetries = 0;

        virtual public void Connect(Session owner, bool moveNext, out int streamId)
        {
            if (_hostsIter == null)
            {
                _hostsIter = owner.Policies.LoadBalancingPolicy.NewQueryPlan(Statement).GetEnumerator();
                if (!_hostsIter.MoveNext())
                {
                    var ex = new NoHostAvailableException(new Dictionary<IPAddress, List<Exception>>());
                    _logger.Error(ex);
                    throw ex;
                }
            }
            else
            {
                if (moveNext)
                {
                    if (!_hostsIter.MoveNext())
                    {
                        var ex = new NoHostAvailableException(InnerExceptions);
                        _logger.Error(ex);
                        throw ex;
                    }
                }
            }

            Connection = owner.Connect(_hostsIter, TriedHosts, InnerExceptions, out streamId);
        }

        internal virtual RowSet ProcessResponse(IOutput outp, Session owner)
        {
            using (outp)
            {
                if (outp is OutputError)
                {
                    var ex = (outp as OutputError).CreateException();
                    _logger.Error(ex);
                    throw ex;
                }
                var rs = new RowSet();
                if (outp.TraceId != null)
                {
                    rs.Info.SetQueryTrace(new QueryTrace(outp.TraceId.Value, owner));
                }
                if (outp is OutputSetKeyspace)
                {
                    owner.SetKeyspace((outp as OutputSetKeyspace).Value);
                }
                return rs;
            }
        }

        abstract public void Begin(Session owner, int steamId);
        abstract public void Process(Session owner, IAsyncResult ar, out object value);
        abstract public void Complete(Session owner, object value, Exception exc = null);
    }
}
