namespace Cassandra
{
    internal class OutputWriteTimeout : OutputError
    {
        readonly WriteTimeoutInfo _info = new WriteTimeoutInfo();

        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            _info.ConsistencyLevel = (ConsistencyLevel)cb.ReadInt16();
            _info.Received = cb.ReadInt32();
            _info.BlockFor = cb.ReadInt32();
            _info.WriteType = cb.ReadString();
        }

        public override DriverException CreateException()
        {
            return new WriteTimeoutException(_info.ConsistencyLevel, _info.Received, _info.BlockFor, _info.WriteType);
        }
    }
}