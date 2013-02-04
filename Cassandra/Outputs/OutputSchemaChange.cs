using System;

namespace Cassandra
{
    internal class OutputSchemaChange : IOutput, IWaitableForDispose
    {
        public string Change;
        public string Keyspace;
        public string Table;
        private Guid? _traceID;
        public Guid? TraceID { get { return _traceID; } }

        internal OutputSchemaChange(BEBinaryReader reader, Guid? traceID)
        {
            _traceID = traceID;
            this.Change = reader.ReadString();
            this.Keyspace= reader.ReadString();
            this.Table = reader.ReadString();
        }

        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
