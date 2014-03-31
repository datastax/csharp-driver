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

using System.Collections.Generic;

namespace Cassandra
{
    public class TableMetadata
    {
        public string Name { get; private set; }


        public TableColumn[] TableColumns { get; private set; }

        public TableOptions Options { get; private set; }

        internal TableMetadata(string name, TableColumn[] tableColumns, TableOptions options)
        {
            Name = name;
            TableColumns = tableColumns;
            Options = options;
        }

        internal TableMetadata(BEBinaryReader reader)
        {
            var coldat = new List<TableColumn>();
            var flags = (FlagBits) reader.ReadInt32();
            int numberOfcolumns = reader.ReadInt32();
            TableColumns = new TableColumn[numberOfcolumns];
            string gKsname = null;
            string gTablename = null;

            if ((flags & FlagBits.GlobalTablesSpec) == FlagBits.GlobalTablesSpec)
            {
                gKsname = reader.ReadString();
                gTablename = reader.ReadString();
            }
            for (int i = 0; i < numberOfcolumns; i++)
            {
                var col = new TableColumn();
                if ((flags & FlagBits.GlobalTablesSpec) != FlagBits.GlobalTablesSpec)
                {
                    col.Keyspace = reader.ReadString();
                    col.Table = reader.ReadString();
                }
                else
                {
                    col.Keyspace = gKsname;
                    col.Table = gTablename;
                }
                col.Name = reader.ReadString();
                col.TypeCode = (ColumnTypeCode) reader.ReadUInt16();
                col.TypeInfo = GetColumnInfo(reader, col.TypeCode);
                coldat.Add(col);
            }
            TableColumns = coldat.ToArray();
        }

        private IColumnInfo GetColumnInfo(BEBinaryReader reader, ColumnTypeCode code)
        {
            ColumnTypeCode innercode;
            ColumnTypeCode vinnercode;
            switch (code)
            {
                case ColumnTypeCode.Custom:
                    return new CustomColumnInfo {CustomTypeName = reader.ReadString()};
                case ColumnTypeCode.List:
                    innercode = (ColumnTypeCode) reader.ReadUInt16();
                    return new ListColumnInfo
                    {
                        ValueTypeCode = innercode,
                        ValueTypeInfo = GetColumnInfo(reader, innercode)
                    };
                case ColumnTypeCode.Map:
                    innercode = (ColumnTypeCode) reader.ReadUInt16();
                    IColumnInfo kci = GetColumnInfo(reader, innercode);
                    vinnercode = (ColumnTypeCode) reader.ReadUInt16();
                    IColumnInfo vci = GetColumnInfo(reader, vinnercode);
                    return new MapColumnInfo
                    {
                        KeyTypeCode = innercode,
                        KeyTypeInfo = kci,
                        ValueTypeCode = vinnercode,
                        ValueTypeInfo = vci
                    };
                case ColumnTypeCode.Set:
                    innercode = (ColumnTypeCode) reader.ReadUInt16();
                    return new SetColumnInfo
                    {
                        KeyTypeCode = innercode,
                        KeyTypeInfo = GetColumnInfo(reader, innercode)
                    };
                default:
                    return null;
            }
        }
    }
}