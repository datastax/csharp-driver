using Cassandra;

namespace CqlPoco
{
    /// <summary>
    /// Represents options available on a per-query basis.
    /// </summary>
    public class CqlQueryOptions
    {
        /// <summary>
        /// An empty instance of CqlQueryOptions (i.e. no options are set).
        /// </summary>
        internal static readonly CqlQueryOptions None = new EmptyQueryOptions();

        private ConsistencyLevel? _consistencyLevel;
        private bool? _tracingEnabled;
        private int? _pageSize;
        private IRetryPolicy _retryPolicy;
        private ConsistencyLevel? _serialConsistencyLevel;

        private bool _noPrepare;

        /// <summary>
        /// Whether or not to use a PreparedStatement when executing the query.
        /// </summary>
        internal bool NoPrepare
        {
            get { return _noPrepare; }
        }

        /// <summary>
        /// Sets the consistency level to be used when executing the query.
        /// </summary>
        public CqlQueryOptions SetConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }

        /// <summary>
        /// Enables or disables tracing for the query. 
        /// </summary>
        public CqlQueryOptions SetTracingEnabled(bool tracingEnabled)
        {
            _tracingEnabled = tracingEnabled;
            return this;
        }

        /// <summary>
        /// Sets the page size for automatic paging for the query.
        /// </summary>
        public CqlQueryOptions SetPageSize(int pageSize)
        {
            _pageSize = pageSize;
            return this;
        }

        /// <summary>
        /// Sets the retry policy for the query.
        /// </summary>
        public CqlQueryOptions SetRetryPolicy(IRetryPolicy retryPolicy)
        {
            _retryPolicy = retryPolicy;
            return this;
        }

        /// <summary>
        /// Sets the serial consistency level for execution of the query.  (NOTE: This only applies to queries using lightweight
        /// transactions -- LWT).
        /// </summary>
        public CqlQueryOptions SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            _serialConsistencyLevel = consistencyLevel;
            return this;
        }

        /// <summary>
        /// Specifies that a PreparedStatement should not be used for executing the query.
        /// </summary>
        public CqlQueryOptions DoNotPrepare()
        {
            _noPrepare = true;
            return this;
        }

        /// <summary>
        /// Copies any options set on this Cql instance to the statement provided.
        /// </summary>
        internal virtual void CopyOptionsToStatement(IStatement statement)
        {
            if (_consistencyLevel.HasValue)
                statement.SetConsistencyLevel(_consistencyLevel.Value);

            if (_tracingEnabled.HasValue)
                statement.EnableTracing(_tracingEnabled.Value);

            if (_pageSize.HasValue)
                statement.SetPageSize(_pageSize.Value);

            if (_retryPolicy != null)
                statement.SetRetryPolicy(_retryPolicy);

            if (_serialConsistencyLevel.HasValue)
                statement.SetSerialConsistencyLevel(_serialConsistencyLevel.Value);
        }

        /// <summary>
        /// Represents no query options.  Copying options to a statement is a no-op.
        /// </summary>
        private class EmptyQueryOptions : CqlQueryOptions
        {
            internal override void CopyOptionsToStatement(IStatement statement)
            {
                // No op
            }
        }
    }
}