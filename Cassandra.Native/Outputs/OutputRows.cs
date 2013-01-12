using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cassandra
{
    internal class OutputRows : IOutput, IWaitableForDispose
    {
        public TableMetadata Metadata;
        public int Rows;
        internal bool buffered;
        
        private readonly BEBinaryReader _reader;

        internal OutputRows(BEBinaryReader reader,bool buffered)
        {
            this.buffered = buffered;
            this._reader = reader;
            Metadata = new TableMetadata(reader);
            Rows = reader.ReadInt32();
            _disposedEvent = new ManualResetEvent(buffered);
        }

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
            if (!buffered)
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
