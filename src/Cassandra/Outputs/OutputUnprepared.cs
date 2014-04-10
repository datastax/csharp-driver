namespace Cassandra
{
    internal class OutputUnprepared : OutputError
    {
        private readonly PreparedQueryNotFoundInfo _info = new PreparedQueryNotFoundInfo();

        protected override void Load(BEBinaryReader cb)
        {
            short len = cb.ReadInt16();
            _info.UnknownID = new byte[len];
            cb.Read(_info.UnknownID, 0, len);
        }

        public override DriverException CreateException()
        {
            return new PreparedQueryNotFoundException(Message, _info.UnknownID);
        }
    }
}