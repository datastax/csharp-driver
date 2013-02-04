using System;
using System.Collections.Generic;

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

        public QueryTrace QueryTrace { get { return _queryTrace; } }

        internal CqlRowSet(OutputRows rawrows, Session session, bool ownRows = true)
        {
            this._rawrows = rawrows;
            this._ownRows = ownRows;
            if (rawrows.TraceID != null)
                _queryTrace = new QueryTrace(rawrows.TraceID.Value, session);
        }

        internal CqlRowSet(OutputVoid output, Session session)
        {
            if (output.TraceID != null)
                _queryTrace = new QueryTrace(output.TraceID.Value, session);
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
