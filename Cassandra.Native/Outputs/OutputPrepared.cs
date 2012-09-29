using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public class OutputPrepared : IOutput, IWaitableForDispose
    {
        public int QueryID;
        public Metadata Metadata;
        internal OutputPrepared(BEBinaryReader reader)
        {
            QueryID = reader.ReadInt32();
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
