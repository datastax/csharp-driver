namespace Cassandra
{
    internal class OutputReadTimeout : OutputError
    {
        private readonly ReadTimeoutInfo _info = new ReadTimeoutInfo();

        protected override void Load(BEBinaryReader cb)
        {
            _info.ConsistencyLevel = (ConsistencyLevel) cb.ReadInt16();
            _info.Received = cb.ReadInt32();
            _info.BlockFor = cb.ReadInt32();
            _info.IsDataPresent = cb.ReadByte() != 0;
        }

        public override DriverException CreateException()
        {
            return new ReadTimeoutException(_info.ConsistencyLevel, _info.Received, _info.BlockFor, _info.IsDataPresent);
        }
    }
}