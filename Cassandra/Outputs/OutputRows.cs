using System;
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
            _disposedEvent = new ManualResetEvent(buffered);
            _traceID = traceID;
        }

        public Guid? TraceID { get { return _traceID; } }

        public void ReadRawColumnValue(byte[] buffer, int offset, int rawLength)
        {
            _reader.Read(buffer, offset, rawLength);
        }

        int _curentIter = 0;
        readonly ManualResetEvent _disposedEvent = null;

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
            _disposedEvent.WaitOne(Timeout.Infinite);
            _disposedEvent.Close();
        }
    }
}
