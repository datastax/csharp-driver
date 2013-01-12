using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal class OutputVoid : IOutput, IWaitableForDispose
    {
        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
