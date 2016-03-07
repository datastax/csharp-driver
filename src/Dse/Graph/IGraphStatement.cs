using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;

namespace Dse.Graph
{
    /// <summary>
    /// Represents a graph statement.
    /// </summary>
    public interface IGraphStatement
    {
        /// <summary>
        /// Gets the graph alias to use with this statement.
        /// </summary>
        string GraphAlias { get; }

        /// <summary>
        /// Gets the graph language to use with this statement.
        /// </summary>
        string GraphLanguage { get; }

        /// <summary>
        /// Gets the graph name to use with this statement.
        /// </summary>
        string GraphName { get; }

        /// <summary>
        /// Sets the graph traversal source name to use with this statement.
        /// </summary>
        string GraphSource { get; }

        /// <summary>
        /// Determines whether this statement is marked as a system query.
        /// </summary>
        bool IsSystemQuery { get; }

        /// <summary>
        /// Returns the <see cref="IStatement"/> representation of the Graph statement.
        /// </summary>
        /// <remarks>Used by the DSE driver to translate between this statement and Core driver statement instances</remarks>
        IStatement ToIStatement();
    }
}
