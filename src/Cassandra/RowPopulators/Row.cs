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
using System.Linq;
using System.Collections;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Represents an CQL row
    /// </summary>
    public class Row : IEnumerable<object>
    {
        /// <summary>
        /// Gets or sets the index of the columns within the row
        /// </summary>
        protected virtual Dictionary<string, int> ColumnIndexes { get; set; }

        protected virtual CqlColumn[] Columns { get; set; }

        protected virtual byte[][] Values { get; set; }

        public int Length
        {
            get { return Values.Length; }
        }

        public object this[int idx]
        {
            get { return Values[idx] == null ? null : ConvertToObject(idx, Values[idx]); }
        }

        public object this[string name]
        {
            get { return this[ColumnIndexes[name]]; }
        }

        internal Row(byte[][] values, CqlColumn[] columns, Dictionary<string, int> columnIndexes)
        {
            Values = values;
            Columns = columns;
            ColumnIndexes = columnIndexes;
        }

        public IEnumerator GetEnumerator()
        {
            return Columns.Select(c => this.GetValue(c.Type, c.Index)).GetEnumerator();
        }

        IEnumerator<object> IEnumerable<object>.GetEnumerator()
        {
            return Columns.Select(c => this.GetValue(c.Type, c.Index)).GetEnumerator();
        }

        public bool IsNull(string name)
        {
            return Values[ColumnIndexes[name]] == null;
        }

        public bool IsNull(int idx)
        {
            return Values[idx] == null;
        }

        public object GetValue(Type tpy, int idx)
        {
            return (Values[idx] == null ? null : ConvertToObject(idx, Values[idx], tpy));
        }

        public object GetValue(Type tpy, string name)
        {
            return GetValue(tpy, ColumnIndexes[name]);
        }

        public T GetValue<T>(int idx)
        {
            return (T) (Values[idx] == null ? null : ConvertToObject(idx, Values[idx], typeof (T)));
        }

        internal object ConvertToObject(int i, byte[] buffer, Type cSharpType = null)
        {
            return TypeInterpreter.CqlConvert(buffer, Columns[i].TypeCode, Columns[i].TypeInfo, cSharpType);
        }

        public T GetValue<T>(string name)
        {
            return GetValue<T>(ColumnIndexes[name]);
        }
    }
}