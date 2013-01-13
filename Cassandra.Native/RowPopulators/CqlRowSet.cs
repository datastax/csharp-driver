using System;
using System.Collections.Generic;

namespace Cassandra
{
    public class CqlColumn
    {
        public string Keyspace;
        public string TableName;
        public string Name;
        public Type Type;
        public TableMetadata.ColumnTypeCode DataTypeCode;
        public TableMetadata.ColumnInfo DataTypeInfo;
    }

    public partial class CqlRowSet : IDisposable
    {
        readonly OutputRows _rawrows;
        readonly CqlColumn[] _columns;
        readonly Dictionary<string, int> _columnIdxes ;
        readonly bool _ownRows;

        internal CqlRowSet(OutputRows rawrows, bool ownRows = true)
        {
            this._rawrows = rawrows;
            this._ownRows = ownRows;
            _columns = new CqlColumn[rawrows.Metadata.Columns.Length];
            _columnIdxes = new Dictionary<string, int>();
            for (int i = 0; i < rawrows.Metadata.Columns.Length; i++)
            {
                _columns[i] = new CqlColumn()
                {
                    Name = rawrows.Metadata.Columns[i].ColumnName,
                    Keyspace = rawrows.Metadata.Columns[i].Keyspace,
                    TableName = rawrows.Metadata.Columns[i].Table,
                    Type = TypeInterpreter.GetTypeFromCqlType(
                        rawrows.Metadata.Columns[i].TypeCode,
                        rawrows.Metadata.Columns[i].TypeInfo),
                    DataTypeCode = rawrows.Metadata.Columns[i].TypeCode,
                    DataTypeInfo = rawrows.Metadata.Columns[i].TypeInfo
                };
                //TODO: what with full long column names?
                if (!_columnIdxes.ContainsKey(rawrows.Metadata.Columns[i].ColumnName))
                    _columnIdxes.Add(rawrows.Metadata.Columns[i].ColumnName, i);
            }
        }

        public CqlColumn[] Columns
        {
            get
            {
                return _columns;
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
                yield return new CqlRow(_rawrows, _columnIdxes);
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
