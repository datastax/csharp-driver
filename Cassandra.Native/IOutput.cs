using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal interface IWaitableForDispose
    {
        void WaitForDispose();
    }

    internal interface IOutput : IDisposable
    {
    }
}
