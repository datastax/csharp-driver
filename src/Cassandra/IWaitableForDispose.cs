namespace Cassandra
{
    internal interface IWaitableForDispose
    {
        void WaitForDispose();
    }
}