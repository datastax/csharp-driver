using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal interface IProtoBufComporessor
    {
        byte[] Decompress(byte[] buffer);
    }
}
