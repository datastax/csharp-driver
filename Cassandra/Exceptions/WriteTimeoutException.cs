namespace Cassandra
{
    /// <summary>
    ///  A Cassandra timeout during a write query.
    /// </summary>
    public class WriteTimeoutException : QueryTimeoutException
    {
        public string WriteType { get; private set; }

        public WriteTimeoutException(ConsistencyLevel consistency, int received, int required,
                                     string writeType) :
            base(string.Format("Cassandra timeout during write query at consitency {0} ({1} replica acknowledged the write over {2} required)", consistency, received, required),
              consistency,
              received,
              required)
        {
            this.WriteType = writeType;
        }
    }
}