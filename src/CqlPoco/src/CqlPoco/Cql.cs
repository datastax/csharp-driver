namespace CqlPoco
{
    /// <summary>
    /// Represents a CQL statement and its arguments.
    /// </summary>
    public class Cql
    {
        /// <summary>
        /// The CQL string.
        /// </summary>
        public string Statement { get; private set; }

        /// <summary>
        /// Any bind variable values for the CQL string.
        /// </summary>
        public object[] Arguments { get; private set; }

        private Cql()
        {
        }

        public static Cql New(string cql, params object[] args)
        {
            return new Cql {Statement = cql, Arguments = args};
        }
    }
}