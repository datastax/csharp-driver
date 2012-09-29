using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cassandra.Native
{
    public class OutputRows : IOutput, IWaitableForDispose
    {
        public Metadata Metadata;
        public int Rows;
        internal bool buffered;
        
        private BEBinaryReader reader;

        internal OutputRows(BEBinaryReader reader,bool buffered)
        {
            this.buffered = buffered;
            this.reader = reader;
            Metadata = new Metadata(reader);
            Rows = reader.ReadInt32();
            disposed = new ManualResetEvent(buffered);
        }

        public void ReadRawColumnValue(byte[] buffer, int offset, int rawLength)
        {
            reader.Read(buffer, offset, rawLength);
        }

        int curIteer = 0;
        ManualResetEvent disposed = null;

        public IEnumerable<int> GetRawColumnLengths()
        {
            for (; curIteer < Rows * Metadata.Columns.Length; )
            {
                int len = reader.ReadInt32();
                curIteer++;
                yield return len;
            }
        }

        public void Dispose()
        {
            if (!buffered)
            {
                foreach (var rawLength in GetRawColumnLengths())
                    reader.Skip(rawLength);
                disposed.Set();
            }
        }
        public void WaitForDispose()
        {
            disposed.WaitOne();
        }
    }
}
