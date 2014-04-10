namespace Cassandra
{
    internal class OutputBadCredentials : OutputError
    {
        public override DriverException CreateException()
        {
            return new AuthenticationException(Message);
        }

        protected override void Load(BEBinaryReader reader)
        {
        }
    }
}