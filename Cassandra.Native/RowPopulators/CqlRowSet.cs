using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    public class CqlColumn
    {
        public string KsName;
        public string TableName;
        public string Name;
        public Type Type;
        public Metadata.ColumnTypeCode DataTypeCode;
        public Metadata.ColumnInfo DataTypeInfo;
    }

    public partial class CqlRowSet : IDisposable
    {
        OutputRows rawrows;
        CqlColumn[] columns;
        Dictionary<string, int> columnIdxes ;
        bool ownRows;

        internal CqlRowSet(OutputRows rawrows, bool ownRows = true)
        {
            this.rawrows = rawrows;
            this.ownRows = ownRows;
            columns = new CqlColumn[rawrows.Metadata.Columns.Length];
            columnIdxes = new Dictionary<string, int>();
            for (int i = 0; i < rawrows.Metadata.Columns.Length; i++)
            {
                columns[i] = new CqlColumn()
                {
                    Name = rawrows.Metadata.Columns[i].column_name,
                    KsName = rawrows.Metadata.Columns[i].ksname,
                    TableName = rawrows.Metadata.Columns[i].tablename,
                    Type = TypeInerpreter.GetTypeFromCqlType(
                        rawrows.Metadata.Columns[i].type_code,
                        rawrows.Metadata.Columns[i].type_info),
                    DataTypeCode = rawrows.Metadata.Columns[i].type_code,
                    DataTypeInfo = rawrows.Metadata.Columns[i].type_info
                };
                //TODO: what with full long column names?
                if (!columnIdxes.ContainsKey(rawrows.Metadata.Columns[i].column_name))
                    columnIdxes.Add(rawrows.Metadata.Columns[i].column_name, i);
            }
        }

        public CqlColumn[] Columns
        {
            get
            {
                return columns;
            }
        }

        
        public int RowsCount
        {
            get { return rawrows.Rows; }
        }

        public IEnumerable<CqlRow> GetRows()
        {
            for (int i = 0; i < rawrows.Rows; i++)
            {
                yield return new CqlRow(rawrows, columnIdxes);
            }
        }

        Guarded<bool> alreadyDisposed = new Guarded<bool>(false);

        public void Dispose()
        {
            lock (alreadyDisposed)
            {
                if (alreadyDisposed.Value)
                    return;

                if (ownRows)
                    rawrows.Dispose();
                alreadyDisposed.Value = true;
            }
        }

        ~CqlRowSet()
        {
            Dispose();
        }
    }
}
