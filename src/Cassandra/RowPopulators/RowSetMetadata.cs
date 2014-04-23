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

using System;
using System.Collections.Generic;

namespace Cassandra
{
    [Flags]
    internal enum FlagBits
    {
        GlobalTablesSpec = 0x0001,
        HasMorePages = 0x0002,
        NoMetadata = 0x0004
    }

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

    public interface IColumnInfo
    {
    }

    public class CustomColumnInfo : IColumnInfo
    {
        public string CustomTypeName;
    }

    public class ListColumnInfo : IColumnInfo
    {
        public ColumnTypeCode ValueTypeCode;
        public IColumnInfo ValueTypeInfo;
    }

    public class SetColumnInfo : IColumnInfo
    {
        public ColumnTypeCode KeyTypeCode;
        public IColumnInfo KeyTypeInfo;
    }

    public class MapColumnInfo : IColumnInfo
    {
        public ColumnTypeCode KeyTypeCode;
        public IColumnInfo KeyTypeInfo;
        public ColumnTypeCode ValueTypeCode;
        public IColumnInfo ValueTypeInfo;
    }

    public class ColumnDesc
    {
        public string Keyspace;
        public string Name;
        public string Table;
        public ColumnTypeCode TypeCode;
        public IColumnInfo TypeInfo;
    }

    public class RowSetMetadata
    {
        /// <summary>
        /// Gets or sets the index of the columns within the row
        /// </summary>
        public virtual Dictionary<string, int> ColumnIndexes { get; protected set; }

        private readonly CqlColumn[] _columns;

        private readonly ColumnDesc[] _rawColumns;

        internal readonly byte[] PagingState = null;

        public CqlColumn[] Columns
        {
            get { return _columns; }
        }

        internal RowSetMetadata(BEBinaryReader reader)
        {
            var coldat = new List<ColumnDesc>();
            var flags = (FlagBits) reader.ReadInt32();
            int numberOfcolumns = reader.ReadInt32();

            _rawColumns = new ColumnDesc[numberOfcolumns];
            string gKsname = null;
            string gTablename = null;

            if ((flags & FlagBits.HasMorePages) == FlagBits.HasMorePages)
                PagingState = reader.ReadBytes();
            else
                PagingState = null;

            if ((flags & FlagBits.NoMetadata) != FlagBits.NoMetadata)
            {
                if ((flags & FlagBits.GlobalTablesSpec) == FlagBits.GlobalTablesSpec)
                {
                    gKsname = reader.ReadString();
                    gTablename = reader.ReadString();
                }

                for (int i = 0; i < numberOfcolumns; i++)
                {
                    var col = new ColumnDesc();
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
                _rawColumns = coldat.ToArray();

                _columns = new CqlColumn[_rawColumns.Length];
                ColumnIndexes = new Dictionary<string, int>();
                for (int i = 0; i < _rawColumns.Length; i++)
                {
                    _columns[i] = new CqlColumn
                    {
                        Name = _rawColumns[i].Name,
                        Keyspace = _rawColumns[i].Keyspace,
                        Table = _rawColumns[i].Table,
                        Type = TypeInterpreter.GetDefaultTypeFromCqlType(
                            _rawColumns[i].TypeCode,
                            _rawColumns[i].TypeInfo),
                        TypeCode = _rawColumns[i].TypeCode,
                        TypeInfo = _rawColumns[i].TypeInfo,
                        Index = i
                    };
                    //TODO: what with full long column names?
                    if (!ColumnIndexes.ContainsKey(_rawColumns[i].Name))
                        ColumnIndexes.Add(_rawColumns[i].Name, i);
                }
            }
        }

        private IColumnInfo GetColumnInfo(BEBinaryReader reader, ColumnTypeCode code)
        {
            ColumnTypeCode innercode;
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
                    var vinnercode = (ColumnTypeCode) reader.ReadUInt16();
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