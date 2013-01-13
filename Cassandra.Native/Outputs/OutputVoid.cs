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
