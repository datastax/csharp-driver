namespace Cassandra
{
    internal class OutputUnauthorized : OutputError
    {
        public override DriverException CreateException()
        {
            return new UnauthorizedException(Message);
        }

        protected override void Load(BEBinaryReader reader)
        {
        }
    }
}