namespace Cassandra
{
    public class PreparedQueryNotFoundException : QueryValidationException
    {
        public byte[] UnknownID { get; private set; }
        public PreparedQueryNotFoundException(string message, byte[] unknownID) :
            base(message) { this.UnknownID = unknownID; }
    }

}
