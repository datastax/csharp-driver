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
﻿using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    public class CqlColumn : ColumnDesc
    {
        public Type Type;
    }

		/// <summary>
		///  Basic information on the execution of a query. <p> This provides the
		///  following information on the execution of a (successful) query: <ul> <li>The
		///  list of Cassandra hosts tried in order (usually just one, unless a node has
		///  been tried but was dead/in error or a timeout provoked a retry (which depends
		///  on the RetryPolicy)).</li> <li>The consistency level achieved by the query
		///  (usually the one asked, though some specific RetryPolicy may allow this to be
		///  different).</li> <li>The query trace recorded by Cassandra if tracing had
		///  been set for the query.</li> </ul></p>
		/// </summary>
    public class ExecutionInfo
    {
        private QueryTrace _queryTrace = null;
        private List<IPAddress> _tiedHosts = null;
        private ConsistencyLevel _achievedConsistency = ConsistencyLevel.Any;

        public List<IPAddress> TriedHosts { get { return _tiedHosts; } }
        public IPAddress QueriedHost { get { return _tiedHosts.Count > 0 ? _tiedHosts[_tiedHosts.Count - 1] : null; } }
        public QueryTrace QueryTrace { get { return _queryTrace; } }
        public ConsistencyLevel AchievedConsistency { get { return _achievedConsistency; } }

        internal void SetTriedHosts(List<IPAddress> triedHosts) { _tiedHosts = triedHosts; }

        internal void SetQueryTrace(QueryTrace queryTrace) { _queryTrace = queryTrace; }
        internal void SetAchievedConsistency(ConsistencyLevel achievedConsistency) { _achievedConsistency = achievedConsistency; }
    }

    public partial class RowSet : IDisposable
    {
        readonly OutputRows _rawrows=null;
        readonly bool _ownRows;
        readonly ExecutionInfo _info = new ExecutionInfo();

        public ExecutionInfo Info { get { return _info; } }

        internal RowSet(OutputRows rawrows, Session session, bool ownRows = true)
        {
            this._rawrows = rawrows;
            this._ownRows = ownRows;
            if (rawrows != null && rawrows.TraceID != null)
                _info.SetQueryTrace(new QueryTrace(rawrows.TraceID.Value, session));
        }

        internal RowSet(OutputVoid output, Session session)
        {
            if (output.TraceID != null)
                _info.SetQueryTrace(new QueryTrace(output.TraceID.Value, session));
        }

        internal RowSet(OutputSetKeyspace output, Session session)
        {
        }

        internal RowSet(OutputSchemaChange output, Session session)
        {
            if (output.TraceID != null)
                _info.SetQueryTrace(new QueryTrace(output.TraceID.Value, session));
        }

        public CqlColumn[] Columns
        {
            get { return _rawrows == null ? new CqlColumn[] {} : _rawrows.Metadata.Columns; }
        }

        internal int RowsCount
        {
            get { return _rawrows == null ? 0 : _rawrows.Rows; }
        }
        
        BoolSwitch _alreadyIterated = new BoolSwitch();

        public IEnumerable<Row> GetRows()
        {
            if (!_alreadyIterated.TryTake())
                throw new InvalidOperationException("RowSet already iterated");
            
            if (_rawrows != null)
                for (int i = 0; i < _rawrows.Rows; i++)
                    yield return _rawrows.Metadata.GetRow(_rawrows);
        }

        BoolSwitch _alreadyDisposed = new BoolSwitch();

        public void Dispose()
        {
            if (!_alreadyDisposed.TryTake())
                return;

            if (_ownRows)
                _rawrows.Dispose();

        }

        ~RowSet()
        {
            Dispose();
        }
    }
}
