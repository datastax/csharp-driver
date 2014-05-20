using System;
namespace Cassandra
{
    /// <summary>
    ///  An executable query.
    ///  This represents either a <see cref="SimpleStatement"/>, a <see cref="BoundStatement"/> or a
    ///  <see cref="BoundStatement"/> along with the query options (consistency level,
    ///  whether to trace the query, ...).
    /// </summary>
    public interface IStatement
    {
        /// <summary>
        /// Gets the consistency level for this query.
        /// </summary>
        ConsistencyLevel? ConsistencyLevel { get; }
        /// <summary>
        ///  Disable tracing for the statement.
        /// </summary>
        IStatement DisableTracing();
        /// <summary>
        ///  Enables tracing for the statement
        /// </summary>
        IStatement EnableTracing(bool enable = true);
        /// <summary>
        ///  Gets whether tracing is enabled for this query or not.
        /// </summary>
        bool IsTracing { get; }
        /// <summary>
        /// Gets query's page size.
        /// </summary>
        int PageSize { get; }
        byte[] PagingState { get; }
        object[] QueryValues { get; }
        /// <summary>
        ///  Gets the retry policy sets for this query, if any.
        /// </summary>
        IRetryPolicy RetryPolicy { get; }
        /// <summary>
        ///  The routing key (in binary raw form) to use for token aware routing of this
        ///  query. <p> The routing key is optional in the sense that implementers are
        ///  free to return <c>null</c>. The routing key is an hint used for token
        ///  aware routing (see
        ///  <link>TokenAwarePolicy</link>), and if
        ///  provided should correspond to the binary value for the query partition key.
        ///  However, not providing a routing key never causes a query to fail and if the
        ///  load balancing policy used is not token aware, then the routing key can be
        ///  safely ignored.</p>
        /// </summary>
        RoutingKey RoutingKey { get; }
        ConsistencyLevel SerialConsistencyLevel { get; }
        /// <summary>
        ///  Sets the consistency level for the query. <p> The default consistency level,
        ///  if this method is not called, is ConsistencyLevel.ONE.</p>
        /// </summary>
        IStatement SetConsistencyLevel(ConsistencyLevel? consistency);
        IStatement SetPageSize(int pageSize);
        IStatement SetPagingState(byte[] pagingState);
        IStatement SetRetryPolicy(IRetryPolicy policy);
        IStatement SetSerialConsistencyLevel(ConsistencyLevel serialConsistency);
        bool SkipMetadata { get; }
    }
}
