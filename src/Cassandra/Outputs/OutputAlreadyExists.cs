namespace Cassandra
{
    internal class OutputAlreadyExists : OutputError
    {
        private readonly AlreadyExistsInfo _info = new AlreadyExistsInfo();

        protected override void Load(BEBinaryReader cb)
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