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
using System.Threading;

namespace Cassandra
{
    internal class OutputRows : IOutput, IWaitableForDispose
    {
        public readonly RowSetMetadata Metadata;
        public readonly int Rows;
        internal readonly bool _buffered;
        
        private readonly BEBinaryReader _reader;
        private readonly Guid? _traceID;

        internal OutputRows(BEBinaryReader reader, bool buffered, Guid? traceID)
        {
            this._buffered = buffered;
            this._reader = reader;
            Metadata = new RowSetMetadata(reader);
            Rows = reader.ReadInt32();
            if(!buffered)
                _disposedEvent = new ManualResetEventSlim(buffered);
            _traceID = traceID;
        }

        public Guid? TraceID { get { return _traceID; } }

        public void ReadRawColumnValue(byte[] buffer, int offset, int rawLength)
        {
            _reader.Read(buffer, offset, rawLength);
        }

        int _curentIter = 0;
        readonly ManualResetEventSlim _disposedEvent = null;

        public IEnumerable<int> GetRawColumnLengths()
        {
            for (; _curentIter < Rows * Metadata.Columns.Length; )
            {
                int len = _reader.ReadInt32();
                _curentIter++;
                yield return len;
            }
        }

        public void Dispose()
        {
            if (!_buffered)
            {
                foreach (var rawLength in GetRawColumnLengths())
                    _reader.Skip(rawLength);
                _disposedEvent.Set();
            }
        }
        public void WaitForDispose()
        {
            if (!_buffered)
            {
                _disposedEvent.Wait(Timeout.Infinite);
                _disposedEvent.Dispose();
            }
        }
    }
}
