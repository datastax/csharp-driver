using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal interface IWaitableForDispose
    {
        void WaitForDispose();
    }

    public interface IOutput :IDisposable
    {
    }
}
