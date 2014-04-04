﻿//
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
using System.Threading;

namespace Cassandra
{
    internal class OutputRows : IOutput, IWaitableForDispose
    {
        public readonly int Rows;
        internal readonly bool _buffered;
        private readonly ManualResetEventSlim _disposedEvent;

        private readonly BEBinaryReader _reader;
        private readonly Guid? _traceID;

        private int _curentIter;
        internal RowSetMetadata _metadata;

        public RowSetMetadata Metadata
        {
            get { return _metadata; }
        }

        public Guid? TraceID
        {
            get { return _traceID; }
        }

        internal OutputRows(BEBinaryReader reader, bool buffered, Guid? traceID)
        {
            _buffered = buffered;
            _reader = reader;
            _metadata = new RowSetMetadata(reader);
            Rows = reader.ReadInt32();
            if (!buffered)
                _disposedEvent = new ManualResetEventSlim(buffered);
            _traceID = traceID;
        }

        public void Dispose()
        {
            if (!_buffered)
            {
                foreach (int rawLength in GetRawColumnLengths())
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

        public void ReadRawColumnValue(byte[] buffer, int offset, int rawLength)
        {
            _reader.Read(buffer, offset, rawLength);
        }

        public IEnumerable<int> GetRawColumnLengths()
        {
            for (; _curentIter < Rows*Metadata.Columns.Length;)
            {
                int len = _reader.ReadInt32();
                _curentIter++;
                yield return len;
            }
        }
    }
}