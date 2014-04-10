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
using System.IO;

namespace Cassandra
{
    public class RowSet : IDisposable
    {
        public delegate string CellEncoder(object val);

        private readonly BoolSwitch _alreadyDisposed = new BoolSwitch();
        private readonly BoolSwitch _alreadyIterated = new BoolSwitch();

        private readonly ExecutionInfo _info = new ExecutionInfo();
        private readonly bool _ownRows;
        private readonly OutputRows _rawrows;

        public ExecutionInfo Info
        {
            get { return _info; }
        }

        public CqlColumn[] Columns
        {
            get { return _rawrows == null ? new CqlColumn[] {} : _rawrows.Metadata.Columns; }
        }

        public byte[] PagingState
        {
            get { return _rawrows != null ? _rawrows.Metadata.paging_state : null; }
        }

        public bool IsExhausted
        {
            get { return _rawrows != null ? _rawrows.Metadata.paging_state == null : true; }
        }


        internal RowSet(OutputRows rawrows, ISession session, bool ownRows = true, RowSetMetadata resultMetadata = null)
        {
            _rawrows = rawrows;
            _ownRows = ownRows;

            if (resultMetadata != null)
                rawrows._metadata = resultMetadata;

            if (rawrows != null && rawrows.TraceID != null)
                _info.SetQueryTrace(new QueryTrace(rawrows.TraceID.Value, session));
        }

        internal RowSet(OutputVoid output, ISession session)
        {
            if (output.TraceID != null)
                _info.SetQueryTrace(new QueryTrace(output.TraceID.Value, session));
        }

        internal RowSet(OutputSetKeyspace output, ISession session)
        {
        }

        internal RowSet(OutputSchemaChange output, ISession session)
        {
            if (output.TraceID != null)
                _info.SetQueryTrace(new QueryTrace(output.TraceID.Value, session));
        }

        public void Dispose()
        {
            if (!_alreadyDisposed.TryTake())
                return;

            if (_ownRows)
                _rawrows.Dispose();
        }

        public IEnumerable<Row> GetRows()
        {
            if (!_alreadyIterated.TryTake())
                throw new InvalidOperationException("RowSet already iterated");
            if (_rawrows != null)
            {
                for (int i = 0; i < _rawrows.Rows; i++)
                    yield return _rawrows.Metadata.GetRow(_rawrows);
            }
        }

        ~RowSet()
        {
            Dispose();
        }


        public void PrintTo(TextWriter stream,
                            string delim = "\t|",
                            string rowDelim = "\r\n",
                            bool printHeader = true,
                            bool printFooter = true,
                            string separ = "-------------------------------------------------------------------------------",
                            string lasLFrm = "Returned {0} rows.",
                            CellEncoder cellEncoder = null
            )
        {
            if (printHeader)
            {
                bool first = true;
                foreach (CqlColumn column in Columns)
                {
                    if (first) first = false;
                    else
                        stream.Write(delim);

                    stream.Write(column.Name);
                }
                stream.Write(rowDelim);
                stream.Write(separ);
                stream.Write(rowDelim);
            }
            int i = 0;
            foreach (Row row in GetRows())
            {
                bool first = true;
                for (int j = 0; j < Columns.Length; j++)
                {
                    if (first) first = false;
                    else
                        stream.Write(delim);

                    if (row[j] is Array || (row[j].GetType().IsGenericType && row[j] is IEnumerable))
                        cellEncoder = delegate(object collection)
                        {
                            string result = "<Collection>";
                            if (collection.GetType() == typeof (byte[]))
                                result += CqlQueryTools.ToHex((byte[]) collection);
                            else
                                foreach (object val in (collection as IEnumerable))
                                    result += val + ",";
                            return result.Substring(0, result.Length - 1) + "</Collection>";
                        };

                    stream.Write(cellEncoder == null ? row[j] : cellEncoder(row[j]));
                }
                stream.Write(rowDelim);
                i++;
            }
            if (printFooter)
            {
                stream.Write(separ);
                stream.Write(rowDelim);
                stream.Write(lasLFrm, i);
                stream.Write(rowDelim);
            }
        }
    }
}