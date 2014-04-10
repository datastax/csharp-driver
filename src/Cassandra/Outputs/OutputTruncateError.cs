namespace Cassandra
{
    internal class OutputTruncateError : OutputError
    {
        public override DriverException CreateException()
        {
            return new TruncateException(Message);
        }

        protected override void Load(BEBinaryReader reader)
        {
        }
    }
}