using System;

namespace Cassandra
{
    internal interface IWaitableForDispose
    {
        void WaitForDispose();
    }

    internal interface IOutput : IDisposable
    {
    }
}
