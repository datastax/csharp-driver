using System;

namespace Cassandra
{
    internal class OutputVoid : IOutput, IWaitableForDispose
    {
        private Guid? _traceID;
        public Guid? TraceID { get { return _traceID; } }
        public OutputVoid(Guid? traceID)
        {
            _traceID = traceID;
        }
        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
