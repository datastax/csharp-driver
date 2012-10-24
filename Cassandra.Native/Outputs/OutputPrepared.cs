using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal class OutputPrepared : IOutput, IWaitableForDispose
    {
        public byte[] QueryID;
        public Metadata Metadata;
        internal OutputPrepared(BEBinaryReader reader)
        {
            var len = reader.ReadInt16();
            QueryID = new byte[len];
            reader.Read(QueryID, 0, len);
            Metadata = new Metadata(reader);
        }

        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
