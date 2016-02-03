using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using Dse.Graph;

namespace Dse
{
    /// <summary>
    /// Represents an <see cref="ISession"/> suitable for querying a DataStax Enterprise (DSE) Cluster.
    /// </summary>
    public interface IDseSession : ISession
    {
        /// <summary>
        /// Executes a graph statement.
        /// </summary>
        /// <param name="statement">The graph statement containing the query</param>
        GraphResultSet ExecuteGraph(IGraphStatement statement);

        /// <summary>
        /// Executes a graph statement.
        /// </summary>
        /// <param name="statement">The graph statement containing the query</param>
        Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement statement);
    }
}
