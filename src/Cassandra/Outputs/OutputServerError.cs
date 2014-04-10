namespace Cassandra
{
    internal class OutputServerError : OutputError
    {
        public override DriverException CreateException()
        {
            return new ServerErrorException(Message);
        }

        protected override void Load(BEBinaryReader reader)
        {
        }
    }
}