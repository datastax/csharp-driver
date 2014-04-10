namespace Cassandra
{
    internal class OutputOverloaded : OutputError
    {
        public override DriverException CreateException()
        {
            return new OverloadedException(Message);
        }

        protected override void Load(BEBinaryReader reader)
        {
        }
    }
}