using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    public class CqlColumn : ColumnDesc
    {
        public Type Type;
    }

    public partial class CqlRowSet : IDisposable
    {
        readonly OutputRows _rawrows=null;
        readonly bool _ownRows;
        private readonly QueryTrace _queryTrace = null;
        private List<IPAddress> _tiedHosts = null;

        public QueryTrace QueryTrace { get { return _queryTrace; } }
        public List<IPAddress> TriedHosts { get { return _tiedHosts; } }
        public IPAddress QueriedHost { get { return _tiedHosts.Count > 0 ? _tiedHosts[_tiedHosts.Count - 1] : null; } }

        internal void SetTriedHosts(List<IPAddress> triedHosts) { _tiedHosts = triedHosts; }

        internal CqlRowSet(OutputRows rawrows, Session session, bool ownRows = true)
        {
            this._rawrows = rawrows;
            this._ownRows = ownRows;
            if (rawrows!=null && rawrows.TraceID != null)
                _queryTrace = new QueryTrace(rawrows.TraceID.Value, session);
        }

        internal CqlRowSet(OutputVoid output, Session session)
        {
            if (output.TraceID != null)
                _queryTrace = new QueryTrace(output.TraceID.Value, session);
        }

        internal CqlRowSet(OutputSetKeyspace output, Session session)
        {
        }

        internal CqlRowSet(OutputSchemaChange output, Session session)
        {
            if (output.TraceID != null)
                _queryTrace = new QueryTrace(output.TraceID.Value, session);
        }

        public CqlColumn[] Columns
        {
            get { return _rawrows == null ? new CqlColumn[] {} : _rawrows.Metadata.Columns; }
        }

        public int RowsCount
        {
            get { return _rawrows == null ? 0 : _rawrows.Rows; }
        }

        public IEnumerable<CqlRow> GetRows()
        {
            if (_rawrows != null)
                for (int i = 0; i < _rawrows.Rows; i++)
                    yield return _rawrows.Metadata.GetRow(_rawrows);
        }

        readonly Guarded<bool> _alreadyDisposed = new Guarded<bool>(false);

        public void Dispose()
        {
            lock (_alreadyDisposed)
            {
                if (_alreadyDisposed.Value)
                    return;

                if (_ownRows)
                    _rawrows.Dispose();
                _alreadyDisposed.Value = true;
            }
        }

        ~CqlRowSet()
        {
            Dispose();
        }
    }
}
