namespace Cassandra
{
    internal class OutputIsBootstrapping : OutputError
    {
        public override DriverException CreateException()
        {
            return new IsBootstrappingException(Message);
        }

        protected override void Load(BEBinaryReader reader)
        {
        }
    }
}