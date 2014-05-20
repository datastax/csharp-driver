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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Cassandra.Data
{
    /// <summary>
    /// Reads a forward-only stream of rows from Cassandra.
    /// </summary>
    /// <inheritdoc />
    public class CqlReader : DbDataReader
    {
        private readonly Dictionary<string, int> colidx = new Dictionary<string, int>();
        private readonly IEnumerator<Row> enumerRows;
        private readonly RowSet popul;
        private IEnumerable<Row> enumRows;

        public override int Depth
        {
            get { return 0; }
        }

        /// <inheritdoc />
        public override int FieldCount
        {
            get { return popul.Columns.Length; }
        }

        public override bool HasRows
        {
            get { return true; }
        }

        public override bool IsClosed
        {
            get { return false; }
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

        internal CqlReader(RowSet rows)
        {
            popul = rows;
            for (int idx = 0; idx < popul.Columns.Length; idx++)
                colidx.Add(popul.Columns[idx].Name, idx);
            enumRows = popul.GetRows();
            enumerRows = enumRows.GetEnumerator();
        }

        public override void Close()
        {

        }

        /// <inheritdoc />
        public override bool GetBoolean(int ordinal)
        {
            return (bool) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override byte GetByte(int ordinal)
        {
            return (byte) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override char GetChar(int ordinal)
        {
            return (char) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override string GetDataTypeName(int ordinal)
        {
            return popul.Columns[ordinal].TypeCode.ToString();
        }

        /// <inheritdoc />
        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override decimal GetDecimal(int ordinal)
        {
            return (decimal) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override double GetDouble(int ordinal)
        {
            return (double) GetValue(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this);
        }

        /// <inheritdoc />
        public override Type GetFieldType(int ordinal)
        {
            return popul.Columns[ordinal].Type;
        }

        /// <inheritdoc />
        public override float GetFloat(int ordinal)
        {
            return (float) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override Guid GetGuid(int ordinal)
        {
            return (Guid) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override short GetInt16(int ordinal)
        {
            return (Int16) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override int GetInt32(int ordinal)
        {
            return (Int32) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override long GetInt64(int ordinal)
        {
            return (Int64) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override string GetName(int ordinal)
        {
            return popul.Columns[ordinal].Name;
        }

        /// <inheritdoc />
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
            return (string) GetValue(ordinal);
        }

        /// <inheritdoc />
        public override object GetValue(int ordinal)
        {
            return enumerRows.Current[ordinal];
        }

        /// <inheritdoc />
        public override int GetValues(object[] values)
        {
            for (int i = 0; i < enumerRows.Current.Length; i++)
                values[i] = enumerRows.Current[i];
            return enumerRows.Current.Length;
        }

        /// <inheritdoc />
        public override bool IsDBNull(int ordinal)
        {
            return enumerRows.Current.IsNull(ordinal);
        }

        /// <inheritdoc />
        public override bool NextResult()
        {
            return enumerRows.MoveNext();
        }

        /// <inheritdoc />
        public override bool Read()
        {
            return enumerRows.MoveNext();
        }
    }
}
