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
using System.Text;

namespace Cassandra.RequestHandlers
{
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
                else if (outp is OutputRows)
                {
                    ProcessRows(rs, (OutputRows)outp, owner);
                }
                return rs;
            }
        }

        /// <summary>
        /// Process rows and sets the paging event handler
        /// </summary>
        internal virtual void ProcessRows(RowSet rs, OutputRows outputRows, Session session)
        {
            if (outputRows.Metadata != null)
            {
                rs.Columns = outputRows.Metadata.Columns;
                rs.PagingState = outputRows.Metadata.PagingState;
                if (rs.PagingState != null)
                {
                    rs.FetchNextPage = (pagingState) =>
                    {
                        if (session.IsDisposed)
                        {
                            _logger.Warning("Trying to page results using a Session already disposed.");
                            return new RowSet();
                        }
                        Statement.SetPagingState(pagingState);
                        return session.Execute(Statement);
                    };
                }
            }
            for (var i = 0; i < outputRows.Rows; i++)
            {
                rs.AddRow(ProcessRowItem(outputRows));
            }
        }

        internal virtual Row ProcessRowItem(OutputRows outputRows)
        {
            var valuesList = new List<byte[]>();
            int i = 0;
            foreach (int len in outputRows.GetRawColumnLengths())
            {
                if (len < 0)
                {
                    valuesList.Add(null);
                }
                else
                {
                    var buffer = new byte[len];
                    outputRows.ReadRawColumnValue(buffer, 0, len);
                    valuesList.Add(buffer);
                }

                i++;
                if (i >= outputRows.Metadata.Columns.Length)
                {
                    break;
                }
            }

            return new Row(valuesList.ToArray(), outputRows.Metadata.Columns, outputRows.Metadata.ColumnIndexes);
        }

        abstract public void Begin(Session owner, int steamId);
        abstract public void Process(Session owner, IAsyncResult ar, out object value);
        abstract public void Complete(Session owner, object value, Exception exc = null);
    }
}
