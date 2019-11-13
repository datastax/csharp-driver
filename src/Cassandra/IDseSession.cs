//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
using System.Threading.Tasks;

using Cassandra.Graph;

namespace Cassandra
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