namespace Cassandra
{
    internal class OutputUnavailableException : OutputError
    {
        private readonly UnavailableInfo _info = new UnavailableInfo();

        protected override void Load(BEBinaryReader cb)
        {
            _info.ConsistencyLevel = (ConsistencyLevel) cb.ReadInt16();
            _info.Required = cb.ReadInt32();
            _info.Alive = cb.ReadInt32();
        }

        public override DriverException CreateException()
        {
            return new UnavailableException(_info.ConsistencyLevel, _info.Required, _info.Alive);
        }
    }
}