namespace Cassandra
{
    internal class OutputAlreadyExists : OutputError
    {
        readonly AlreadyExistsInfo _info = new AlreadyExistsInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            _info.Ks = cb.ReadString();
            _info.Table = cb.ReadString();
        }
        public override DriverException CreateException()
        {
            return new AlreadyExistsException(_info.Ks, _info.Table);
        }
    }
}