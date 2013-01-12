using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    public enum StrategyClass
    {
        Unknown = 0,
        SimpleStrategy = 1,
        NetworkTopologyStrategy = 2,
        OldNetworkTopologyStrategy = 3
    }

    public struct KeyspaceMetadata
    {
        public string Keyspace;
        public List<TableMetadata> Tables;
        public bool? DurableWrites;
        public StrategyClass StrategyClass;
        public SortedDictionary<string, int?> ReplicationOptions;

    }

    public class TableMetadata
    {
        [Flags]
        public enum FlagBits
        {
            GlobalTablesSpec = 0x0001
        }

        public FlagBits Flags;

        public enum ColumnTypeCode
        {
            Custom = 0x0000,
            Ascii = 0x0001,
            Bigint = 0x0002,
            Blob = 0x0003,
            Boolean = 0x0004,
            Counter = 0x0005,
#if NET_40_OR_GREATER
            Decimal = 0x0006,
#endif
            Double = 0x0007,
            Float = 0x0008,
            Int = 0x0009,
            Text = 0x000A,
            Timestamp = 0x000B,
            Uuid = 0x000C,
            Varchar = 0x000D,
#if NET_40_OR_GREATER
            Varint = 0x000E,
#endif
            Timeuuid = 0x000F,
            Inet = 0x0010,
            List = 0x0020,
            Map = 0x0021,
            Set = 0x0022
        }
        
        public enum KeyType
        {            
            Partition = 1,
            Row = 2,
            Secondary = 3,
            NotAKey = 0
        }

        public interface ColumnInfo
        {
        }

        public class CustomColumnInfo : ColumnInfo
        {            
            public string CustomTypeName;
        }
        
        public class ListColumnInfo : ColumnInfo
        {
            public ColumnTypeCode ValueTypeCode;
            public ColumnInfo ValueTypeInfo;            
        }

        public class SetColumnInfo : ColumnInfo
        {
            public ColumnTypeCode KeyTypeCode;
            public ColumnInfo KeyTypeInfo;
        }
        
        public class MapColumnInfo : ColumnInfo
        {
            public ColumnTypeCode KeyTypeCode;
            public ColumnInfo KeyTypeInfo;
            public ColumnTypeCode ValueTypeCode;
            public ColumnInfo ValueTypeInfo;            
        }

        public struct ColumnDesc
        {
            public string Keyspace;
            public string Table;
            public string ColumnName;
            public ColumnInfo TypeInfo;
            public string SecondaryIndexName;
            public string SecondaryIndexType;
            public KeyType KeyType;
            public ColumnTypeCode TypeCode;
        }        


        public ColumnDesc[] Columns;

        internal TableMetadata()
        {
        }

        internal TableMetadata(BEBinaryReader reader)
        {
            List<ColumnDesc> coldat = new List<ColumnDesc>();
            Flags = (FlagBits)reader.ReadInt32();
            var numberOfcolumns = reader.ReadInt32();
            this.Columns = new TableMetadata.ColumnDesc[numberOfcolumns];
            string g_ksname = null;
            string g_tablename = null;

            if ((Flags & FlagBits.GlobalTablesSpec) == FlagBits.GlobalTablesSpec)
            {
                g_ksname = reader.ReadString();
                g_tablename = reader.ReadString();
            }
            for (int i = 0; i < numberOfcolumns; i++)
            {
                ColumnDesc col = new ColumnDesc();
                if ((Flags & FlagBits.GlobalTablesSpec) != FlagBits.GlobalTablesSpec)
                {
                    col.Keyspace = reader.ReadString();
                    col.Table = reader.ReadString();
                }
                else
                {
                    col.Keyspace = g_ksname;
                    col.Table = g_tablename;
                }
                col.ColumnName = reader.ReadString();
                col.TypeCode = (ColumnTypeCode)reader.ReadUInt16();
                col.TypeInfo = GetColumnInfo(reader, col.TypeCode);
                coldat.Add(col);
            }
            Columns = coldat.ToArray();
        }

        private ColumnInfo GetColumnInfo(BEBinaryReader reader, ColumnTypeCode code)
        {
            ColumnTypeCode innercode;
            ColumnTypeCode vinnercode;
            switch (code)
            {
                case ColumnTypeCode.Custom:
                    return new CustomColumnInfo() { CustomTypeName = reader.ReadString() };
                case ColumnTypeCode.List:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    return new ListColumnInfo() {
                        ValueTypeCode = innercode, 
                        ValueTypeInfo = GetColumnInfo(reader, innercode) 
                    };
                case ColumnTypeCode.Map:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    var kci = GetColumnInfo(reader, innercode);
                    vinnercode = (ColumnTypeCode)reader.ReadUInt16();
                    var vci = GetColumnInfo(reader, vinnercode);
                    return new MapColumnInfo() {
                        KeyTypeCode = innercode,
                        KeyTypeInfo = kci,
                        ValueTypeCode = vinnercode, 
                        ValueTypeInfo = vci
                    };
                case ColumnTypeCode.Set:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    return new SetColumnInfo() {
                        KeyTypeCode = innercode,
                        KeyTypeInfo = GetColumnInfo(reader, innercode)
                    };
                default:
                    return null;
            }
        }
    }
}
