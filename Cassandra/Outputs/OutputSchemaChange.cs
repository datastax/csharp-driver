namespace Cassandra
{
    internal class OutputSchemaChange : IOutput, IWaitableForDispose
    {
        public string Change;
        public string Keyspace;
        public string Table;

        internal OutputSchemaChange(BEBinaryReader reader)
        {
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
