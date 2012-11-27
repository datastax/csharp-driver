using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public class Metadata
    {
        [Flags]
        public enum FlagBits
        {
            Global_tables_spec = 0x0001
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
            Decimal = 0x0006,
            Double = 0x0007,
            Float = 0x0008,
            Int = 0x0009,
            Text = 0x000A,
            Timestamp = 0x000B,
            Uuid = 0x000C,
            Varchar = 0x000D,
            Varint = 0x000E,
            Timeuuid = 0x000F,
            Inet = 0x0010,
            List = 0x0020,
            Map = 0x0021,
            Set = 0x0022
        }
        
        public enum KeyType
        {            
            PARTITION = 1,
            ROW = 2,
            SECONDARY = 3,
            NOT_A_KEY = 0
        }

        public enum StrategyClass
        {
            Unknown = 0,
            SimpleStrategy = 1,
            NetworkTopologyStrategy = 2,
            OldNetworkTopologyStrategy = 3
        }
        
        public interface ColumnInfo
        {
        }

        public class CustomColumnInfo : ColumnInfo
        {            
            public string custom_type_name;
        }
        
        public class ListColumnInfo : ColumnInfo
        {
            public ColumnTypeCode value_type_code;
            public ColumnInfo value_type_info;            
        }

        public class SetColumnInfo : ColumnInfo
        {
            public ColumnTypeCode key_type_code;
            public ColumnInfo key_type_info;
        }
        
        public class MapColumnInfo : ColumnInfo
        {
            public ColumnTypeCode key_type_code;
            public ColumnInfo key_type_info;
            public ColumnTypeCode value_type_code;
            public ColumnInfo value_type_info;            
        }

        public struct KeyspaceDesc
        {
            public string ksName;
            public List<Metadata> tables;
            public bool? durableWrites;
            public StrategyClass strategyClass;
            public SortedDictionary<string, int?> replicationOptions;
        }
        
        public struct ColumnDesc
        {
            public string ksname;
            public string tablename;
            public string column_name;
            public ColumnInfo type_info;
            public string secondary_index_name;
            public string secondary_index_type;
            public KeyType key_type;
            public ColumnTypeCode type_code;
            public ListColumnInfo listInfo;
            public SetColumnInfo setInfo;
            public MapColumnInfo mapInfo;
        }        


        public ColumnDesc[] Columns;


        internal Metadata()
        {
        }

        internal Metadata(BEBinaryReader reader)
        {
            List<ColumnDesc> coldat = new List<ColumnDesc>();
            Flags = (FlagBits)reader.ReadInt32();
            var numberOfcolumns = reader.ReadInt32();
            this.Columns = new Metadata.ColumnDesc[numberOfcolumns];
            string g_ksname = null;
            string g_tablename = null;

            if ((Flags & FlagBits.Global_tables_spec) == FlagBits.Global_tables_spec)
            {
                g_ksname = reader.ReadString();
                g_tablename = reader.ReadString();
            }
            for (int i = 0; i < numberOfcolumns; i++)
            {
                ColumnDesc col = new ColumnDesc();
                if ((Flags & FlagBits.Global_tables_spec) != FlagBits.Global_tables_spec)
                {
                    col.ksname = reader.ReadString();
                    col.tablename = reader.ReadString();
                }
                else
                {
                    col.ksname = g_ksname;
                    col.tablename = g_tablename;
                }
                col.column_name = reader.ReadString();
                col.type_code = (ColumnTypeCode)reader.ReadUInt16();
                col.type_info = GetColumnInfo(reader, col.type_code);
                coldat.Add(col);
            }
            Columns = coldat.ToArray();
        }

        ColumnInfo GetColumnInfo(BEBinaryReader reader, ColumnTypeCode code)
        {
            ColumnTypeCode innercode;
            ColumnTypeCode vinnercode;
            switch (code)
            {
                case ColumnTypeCode.Custom:
                    return new CustomColumnInfo() { custom_type_name = reader.ReadString() };
                case ColumnTypeCode.List:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    return new ListColumnInfo() {
                        value_type_code = innercode, 
                        value_type_info = GetColumnInfo(reader, innercode) 
                    };
                case ColumnTypeCode.Map:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    var kci = GetColumnInfo(reader, innercode);
                    vinnercode = (ColumnTypeCode)reader.ReadUInt16();
                    var vci = GetColumnInfo(reader, vinnercode);
                    return new MapColumnInfo() {
                        key_type_code = innercode,
                        key_type_info = kci,
                        value_type_code = vinnercode, 
                        value_type_info = vci
                    };
                case ColumnTypeCode.Set:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    return new SetColumnInfo() {
                        key_type_code = innercode,
                        key_type_info = GetColumnInfo(reader, innercode)
                    };
                default:
                    return null;
            }
        }
    }
}
