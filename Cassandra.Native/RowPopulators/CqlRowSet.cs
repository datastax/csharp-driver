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
        bool ownRows;

        public CqlRowSet(OutputRows rawrows, bool ownRows=true)
        {
            this.rawrows = rawrows;
            this.ownRows = ownRows;
            columns = new CqlColumn[rawrows.Metadata.Columns.Length];
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
                yield return new CqlRow(rawrows);
            }
        }


        public void Dispose()
        {
            if (ownRows)
                rawrows.Dispose();
        }
    }
}
