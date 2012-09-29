using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal interface IBuffering
    {
        int PreferedBufferSize();
        void Reset();
        IEnumerable<ResponseFrame> Process(byte[] buffer, int size, Stream stream, IProtoBufComporessor compressor);
        bool AllowSyncCompletion();
        void Close();
    }
}
