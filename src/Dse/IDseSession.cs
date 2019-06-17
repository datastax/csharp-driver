//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System.Threading.Tasks;

using Dse.Graph;

namespace Dse
{
    /// <summary>
    /// Represents an <see cref="ISession"/> suitable for querying a DataStax Enterprise (DSE) Cluster.
    /// <para>
    /// Session instances are designed to be long-lived, thread-safe and usually a single instance is enough per
    /// application.
    /// </para>
    /// </summary>
    public interface IDseSession : ISession
    {
        /// <summary>
        /// Executes a graph statement.
        /// </summary>
        /// <param name="statement">The graph statement containing the query</param>
        /// <example>
        /// <code>
        /// GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
        /// </code>
        /// </example>
        GraphResultSet ExecuteGraph(IGraphStatement statement);

        /// <summary>
        /// Executes a graph statement.
        /// </summary>
        /// <param name="statement">The graph statement containing the query</param>
        /// <example>
        /// <code>
        /// Task&lt;GraphResultSet$gt; task = session.ExecuteGraphAsync(new SimpleGraphStatement("g.V()"));
        /// </code>
        /// </example>
        Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement statement);

        /// <summary>
        /// Executes a graph statement with the provided execution profile.
        /// The execution profile must have been added previously to the DseCluster
        /// using <see cref="DseClusterBuilder.WithExecutionProfiles"/>.
        /// </summary>
        /// <param name="statement">The graph statement containing the query</param>
        /// <param name="executionProfileName">The graph execution profile name to use while executing this statement.</param>
        /// <example>
        /// <code>
        /// GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"), "graphProfile");
        /// </code>
        /// </example>
        GraphResultSet ExecuteGraph(IGraphStatement statement, string executionProfileName);
        
        /// <summary>
        /// Executes a graph statement asynchronously with the provided graph execution profile.
        /// The graph execution profile must have been added previously to the DseCluster
        /// using <see cref="DseClusterBuilder.WithExecutionProfiles"/>.
        /// </summary>
        /// <param name="statement">The graph statement containing the query</param>
        /// <param name="executionProfileName">The graph execution profile name to use while executing this statement.</param>
        /// <example>
        /// <code>
        /// Task&lt;GraphResultSet$gt; task = session.ExecuteGraphAsync(new SimpleGraphStatement("g.V()"), "graphProfile");
        /// </code>
        /// </example>
        Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement statement, string executionProfileName);
    }
}