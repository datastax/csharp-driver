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
using System.Text;
using System.Data;
using System.Data.Common;
using System.Collections;
using Cassandra;

namespace Cassandra.Data
{
    public class CqlReader : DbDataReader
    {
        RowSet popul = null;
        IEnumerable<Row> enumRows = null;
        IEnumerator<Row> enumerRows = null;
        Dictionary<string, int> colidx = new Dictionary<string, int>();
        internal CqlReader(RowSet rows)
        {
            this.popul = rows;
            for (int idx = 0; idx < popul.Columns.Length; idx++)
                colidx.Add(popul.Columns[idx].Name, idx);
            enumRows = popul.GetRows();
            enumerRows = enumRows.GetEnumerator();
        }

        public override void Close()
        {
            popul.Dispose();
        }

        public override int Depth
        {
            get { return 0; }
        }

        public override int FieldCount
        {
            get { return popul.Columns.Length; }
        }

        public override bool GetBoolean(int ordinal)
        {
            return (bool)GetValue(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)GetValue(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override char GetChar(int ordinal)
        {
            return (char)GetValue(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return popul.Columns[ordinal].TypeCode.ToString();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)GetValue(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)GetValue(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return (double)GetValue(ordinal);
        }

        public override System.Collections.IEnumerator GetEnumerator()
        {
            return ((IEnumerator)new DbEnumerator(this));
        }

        public override Type GetFieldType(int ordinal)
        {
            return popul.Columns[ordinal].Type;
        }

        public override float GetFloat(int ordinal)
        {
            return (float)GetValue(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return (Guid)GetValue(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return (Int16)GetValue(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return (Int32)GetValue(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return (Int64)GetValue(ordinal);
        }

        public override string GetName(int ordinal)
        {
            return popul.Columns[ordinal].Name;
        }

        public override int GetOrdinal(string name)
        {
            return colidx[name];
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotSupportedException();
        }

        public override string GetString(int ordinal)
        {
            return (string)GetValue(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return enumerRows.Current[ordinal];
        }

        public override int GetValues(object[] values)
        {
            for (int i = 0; i < enumerRows.Current.Length; i++)
                values[i] = enumerRows.Current[i];
            return enumerRows.Current.Length;
        }

        public override bool HasRows
        {
            get { return true; }
        }

        public override bool IsClosed
        {
            get { return false; }
        }

        public override bool IsDBNull(int ordinal)
        {
            return enumerRows.Current.IsNull(ordinal);
        }

        public override bool NextResult()
        {
            return enumerRows.MoveNext();
        }

        public override bool Read()
        {
            return enumerRows.MoveNext();
        }

        public override int RecordsAffected
        {
            get { return -1; }
        }

        public override object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        public override object this[int ordinal]
        {
            get { return GetValue(ordinal); }
        }
    }
}
