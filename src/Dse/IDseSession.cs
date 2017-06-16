//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dse;
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
    }
}
