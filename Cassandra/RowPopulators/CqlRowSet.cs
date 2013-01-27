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
        readonly OutputRows _rawrows;
        readonly bool _ownRows;

        internal CqlRowSet(OutputRows rawrows, bool ownRows = true)
        {
            this._rawrows = rawrows;
            this._ownRows = ownRows;
        }

        public CqlColumn[] Columns
        {
            get
            {
                return _rawrows.Metadata.Columns;
            }
        }
        
        public int RowsCount
        {
            get { return _rawrows.Rows; }
        }

        public IEnumerable<CqlRow> GetRows()
        {
            for (int i = 0; i < _rawrows.Rows; i++)
            {
                yield return _rawrows.Metadata.GetRow(_rawrows);
            }
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
