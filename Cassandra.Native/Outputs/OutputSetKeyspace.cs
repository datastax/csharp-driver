using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public class OutputSetKeyspace : IOutput, IWaitableForDispose
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
