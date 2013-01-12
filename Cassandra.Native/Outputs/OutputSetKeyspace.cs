using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal class OutputSetKeyspace : IOutput, IWaitableForDispose
    {
        public string Value;
        internal OutputSetKeyspace(string val) { Value = val; }

        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
