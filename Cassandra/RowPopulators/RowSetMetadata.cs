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
﻿using System;
using System.Collections.Generic;

namespace Cassandra
{

    [Flags]
    internal enum FlagBits
    {
        GlobalTablesSpec = 0x0001
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
        public string Table;
        public string Name;
        public IColumnInfo TypeInfo;
        public ColumnTypeCode TypeCode;
    }

    public class RowSetMetadata
    {
        private readonly CqlColumn[] _columns;
        private readonly Dictionary<string, int> _columnIdxes;

        private readonly ColumnDesc[] _rawColumns;

        internal RowSetMetadata(BEBinaryReader reader)
        {
            var coldat = new List<ColumnDesc>();
            var flags = (FlagBits)reader.ReadInt32();
            var numberOfcolumns = reader.ReadInt32();
            this._rawColumns = new ColumnDesc[numberOfcolumns];
            string gKsname = null;
            string gTablename = null;

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
                col.TypeCode = (ColumnTypeCode)reader.ReadUInt16();
                col.TypeInfo = GetColumnInfo(reader, col.TypeCode);
                coldat.Add(col);
            }
            _rawColumns = coldat.ToArray();

            _columns = new CqlColumn[_rawColumns.Length];
            _columnIdxes = new Dictionary<string, int>();
            for (int i = 0; i < _rawColumns.Length; i++)
            {
                _columns[i] = new CqlColumn()
                    {
                        Name = _rawColumns[i].Name,
                        Keyspace = _rawColumns[i].Keyspace,
                        Table = _rawColumns[i].Table,
                        Type = TypeInterpreter.GetDefaultTypeFromCqlType(
                            _rawColumns[i].TypeCode,
                            _rawColumns[i].TypeInfo),
                        TypeCode = _rawColumns[i].TypeCode,
                        TypeInfo = _rawColumns[i].TypeInfo
                    };
                //TODO: what with full long column names?
                if (!_columnIdxes.ContainsKey(_rawColumns[i].Name))
                    _columnIdxes.Add(_rawColumns[i].Name, i);
            }
        }

        private IColumnInfo GetColumnInfo(BEBinaryReader reader, ColumnTypeCode code)
        {
            ColumnTypeCode innercode;
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
                    var vinnercode = (ColumnTypeCode)reader.ReadUInt16();
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

        public CqlColumn[] Columns
        {
            get { return _columns; }
        }

        internal object ConvertToObject(int i, byte[] buffer, Type cSharpType = null)
        {
            return TypeInterpreter.CqlConvert(buffer, _rawColumns[i].TypeCode, _rawColumns[i].TypeInfo, cSharpType);
        }

        internal byte[] ConvertFromObject(int i, object o)
        {
            return TypeInterpreter.InvCqlConvert(o, _rawColumns[i].TypeCode, _rawColumns[i].TypeInfo);
        }

        internal Row GetRow(OutputRows rawrows)
        {
            return new Row(rawrows, _columnIdxes);
        }
    }
}
