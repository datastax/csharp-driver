using System.Collections.Generic;

namespace Cassandra
{
    public enum KeyType
    {
        None = 0,
        Partition = 1,
        Clustering = 2,
        SecondaryIndex = 3
    }

    public class TableColumn : CqlColumn
    {
        public KeyType KeyType;
        public string SecondaryIndexName;
        public string SecondaryIndexType;
    }

    public class TableMetadata
    {

        public string Name { get; private set; }


        public TableColumn[] TableColumns { get; private set; }

        internal TableMetadata(string name, TableColumn[] tableColumns)
        {
            Name = name;
            TableColumns = tableColumns;
        }

        internal TableMetadata(BEBinaryReader reader)
        {
            List<TableColumn> coldat = new List<TableColumn>();
            var flags = (FlagBits)reader.ReadInt32();
            var numberOfcolumns = reader.ReadInt32();
            this.TableColumns = new TableColumn[numberOfcolumns];
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
                col.TypeCode = (ColumnTypeCode)reader.ReadUInt16();
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
                    return new CustomColumnInfo() { CustomTypeName = reader.ReadString() };
                case ColumnTypeCode.List:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    return new ListColumnInfo()
                    {
                        ValueTypeCode = innercode,
                        ValueTypeInfo = GetColumnInfo(reader, innercode)
                    };
                case ColumnTypeCode.Map:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    var kci = GetColumnInfo(reader, innercode);
                    vinnercode = (ColumnTypeCode)reader.ReadUInt16();
                    var vci = GetColumnInfo(reader, vinnercode);
                    return new MapColumnInfo()
                    {
                        KeyTypeCode = innercode,
                        KeyTypeInfo = kci,
                        ValueTypeCode = vinnercode,
                        ValueTypeInfo = vci
                    };
                case ColumnTypeCode.Set:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    return new SetColumnInfo()
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
