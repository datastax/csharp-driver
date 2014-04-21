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

namespace Cassandra
{
    public class Row : IEnumerable<object>
    {
        protected virtual Dictionary<string, int> ColumnIndexes { get; set; }
        private readonly byte[][] _columns;
        private readonly RowSetMetadata _metadata;

        public int Length
        {
            get { return _columns.Length; }
        }

        public object this[int idx]
        {
            get { return _columns[idx] == null ? null : _metadata.ConvertToObject(idx, _columns[idx]); }
        }

        public object this[string name]
        {
            get { return this[ColumnIndexes[name]]; }
        }

        internal Row(OutputRows rawrows, RowSetMetadata metadata)
        {
            var l = new List<byte[]>();
            ColumnIndexes = metadata.ColumnIndexes;
            _metadata = metadata;
            int i = 0;
            foreach (int len in rawrows.GetRawColumnLengths())
            {
                if (len < 0)
                    l.Add(null);
                else
                {
                    var buffer = new byte[len];
                    rawrows.ReadRawColumnValue(buffer, 0, len);
                    l.Add(buffer);
                }

                i++;
                if (i >= _metadata.Columns.Length)
                    break;
            }
            _columns = l.ToArray();
        }

        public IEnumerator GetEnumerator()
        {
            return new ColumnEnumerator(this);
        }

        IEnumerator<object> IEnumerable<object>.GetEnumerator()
        {
            return new ColumnEnumerator(this);
        }

        public bool IsNull(string name)
        {
            return _columns[ColumnIndexes[name]] == null;
        }

        public bool IsNull(int idx)
        {
            return _columns[idx] == null;
        }

        public object GetValue(Type tpy, int idx)
        {
            return (_columns[idx] == null ? null : _metadata.ConvertToObject(idx, _columns[idx], tpy));
        }

        public object GetValue(Type tpy, string name)
        {
            return GetValue(tpy, ColumnIndexes[name]);
        }

        public T GetValue<T>(int idx)
        {
            return (T) (_columns[idx] == null ? null : _metadata.ConvertToObject(idx, _columns[idx], typeof (T)));
        }

        public T GetValue<T>(string name)
        {
            return GetValue<T>(ColumnIndexes[name]);
        }

        public class ColumnEnumerator : IEnumerator<object>
        {
            private readonly Row _owner;
            private int _idx = -1;

            public ColumnEnumerator(Row owner)
            {
                _owner = owner;
            }

            public object Current
            {
                get
                {
                    if (_idx == -1 || _idx >= _owner._columns.Length) return null;
                    return _owner[_idx];
                }
            }

            public bool MoveNext()
            {
                _idx++;
                return _idx < _owner._columns.Length;
            }

            public void Reset()
            {
                _idx = -1;
            }

            public void Dispose()
            {
            }
        }
    }
}