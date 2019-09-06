using System;

namespace Cassandra.Mapping
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
        /// Execution Profile to be used when executing this CQL instance.
        /// </summary>
        public string ExecutionProfile { get; private set; }

        /// <summary>
        /// Options that are available on a per-query basis.
        /// </summary>
        internal CqlQueryOptions QueryOptions { get; private set; }

        /// <summary>
        /// Determines if automatic paging is enabled. Defaults to true.
        /// </summary>
        internal bool AutoPage { get; set; }

        /// <summary>
        /// Creates a new Cql instance using the CQL string and bind variable values specified.
        /// </summary>
        public Cql(string cql, params object[] args)
            : this(cql, args, new CqlQueryOptions())
        {
        }

        private Cql(string cql, object[] args, CqlQueryOptions queryOptions)
        {
            AutoPage = true;
            Statement = cql;
            Arguments = args;
            QueryOptions = queryOptions;
        }

        /// <summary>
        /// Configures any options for execution of this Cql instance.
        /// </summary>
        public Cql WithOptions(Action<CqlQueryOptions> options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            options(QueryOptions);
            return this;
        }

        /// <summary>
        /// Configures the execution profile for execution of this Cql instance.
        /// </summary>
        public Cql WithExecutionProfile(string executionProfile)
        {
            ExecutionProfile = executionProfile ?? throw new ArgumentNullException(nameof(executionProfile));
            return this;
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
        
        /// <summary>
        /// Creates an empty CQL instance for cases where a cql string is not needed like fetch queries.
        /// </summary>
        public static Cql New()
        {
            return new Cql(string.Empty);
        }

        internal static Cql New(string cql, object[] args, CqlQueryOptions queryOptions)
        {
            return new Cql(cql, args, queryOptions);
        }
    }
}