using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal class OutputOptions : IOutput, IWaitableForDispose
    {
        readonly Dictionary<string, string[]> _options;

        public IDictionary<string, string[]> Options { get { return _options; } }

        internal OutputOptions(BEBinaryReader reader)
        {
            _options = new Dictionary<string, string[]>();
            int n = reader.ReadUInt16();
            for (int i = 0; i < n; i++)
            {
                var k = reader.ReadString();
                var v = reader.ReadStringList().ToArray();
                _options.Add(k, v);
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
