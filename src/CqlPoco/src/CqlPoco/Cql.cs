using System;

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

        /// <summary>
        /// Allow you to set options that are available on a per-query basis.
        /// </summary>
        public CqlQueryOptions QueryOptions { get; private set; }

        public Cql(string cql, object[] args)
        {
            Statement = cql;
            Arguments = args;
            QueryOptions = new CqlQueryOptions();
        }

        private Cql(string cql, object[] args, CqlQueryOptions queryOptions)
        {
            Statement = cql;
            Arguments = args;
            QueryOptions = queryOptions;
        }


        internal void SetStatement(string statement)
        {
            Statement = statement;
        }

        /// <summary>
        /// Creates a new CQL instance from the CQL statement and parameters specified.
        /// </summary>
        public static Cql New(string cql, params object[] args)
        {
            return new Cql(cql, args);
        }

        internal static Cql New(string cql, object[] args, CqlQueryOptions queryOptions)
        {
            return new Cql(cql, args, queryOptions);
        }
    }
}