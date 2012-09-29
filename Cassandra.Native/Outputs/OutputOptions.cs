using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public class OutputOptions : IOutput, IWaitableForDispose
    {
        Dictionary<string, string[]> options;

        public IDictionary<string, string[]> Options { get { return options; } }

        internal OutputOptions(BEBinaryReader reader)
        {
            options = new Dictionary<string, string[]>();
            int n = reader.ReadUInt16();
            for (int i = 0; i < n; i++)
            {
                var k = reader.ReadString();
                var v = reader.ReadStringList().ToArray();
                options.Add(k, v);
            }
        }

        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
