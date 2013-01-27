namespace Cassandra
{
    internal class OutputPrepared : IOutput, IWaitableForDispose
    {
        public byte[] QueryID;
        public RowSetMetadata Metadata;
        internal OutputPrepared(BEBinaryReader reader)
        {
            var len = reader.ReadInt16();
            QueryID = new byte[len];
            reader.Read(QueryID, 0, len);
            Metadata = new RowSetMetadata(reader);
        }

        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
