﻿//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Threading;

namespace Dse.Graph
{
    /// <summary>
    /// Represents a graph statement.
    /// </summary>
    public interface IGraphStatement
    {
        /// <summary>
        /// Returns the consistency level to use for this statement.
        /// </summary>
        ConsistencyLevel? ConsistencyLevel { get; }

        /// <summary>
        /// Gets the graph language to use with this statement.
        /// </summary>
        string GraphLanguage { get; }

        /// <summary>
        /// Gets the graph name to use with this statement.
        /// </summary>
        string GraphName { get; }

        /// <summary>
        /// Gets the consistency level used for read graph queries.
        /// </summary>
        ConsistencyLevel? GraphReadConsistencyLevel { get; }

        /// <summary>
        /// Gets the ReadTimeout for the statement that, when is different than 0, overrides
        /// <see cref="GraphOptions.ReadTimeoutMillis"/>.
        /// <para>Use <see cref="Timeout.Infinite"/> to disable timeouts for this Statement.</para>
        /// </summary>
        int ReadTimeoutMillis { get; }

        /// <summary>
        /// Gets the graph traversal source name to use with this statement.
        /// </summary>
        string GraphSource { get; }

        /// <summary>
        /// Gets the consistency level used for write graph queries.
        /// </summary>
        ConsistencyLevel? GraphWriteConsistencyLevel { get; }

        /// <summary>
        /// Determines whether this statement is marked as a system query.
        /// </summary>
        bool IsSystemQuery { get; }

        /// <summary>
        /// Gets the default timestamp for this query.
        /// </summary>
        DateTimeOffset? Timestamp { get; }

        /// <summary>
        /// Returns the <see cref="IStatement"/> representation of the Graph statement.
        /// </summary>
        /// <remarks>Used by the DSE driver to translate between this statement and Core driver statement instances</remarks>
        IStatement ToIStatement(GraphOptions options);

        /// <summary>
        /// Sets the consistency level to use for this statement.
        /// <para>
        /// This setting will affect the general consistency when executing the gremlin query. However
        /// executing a gremlin query on the server side is going to involve the execution of CQL queries to the 
        /// persistence engine that is Cassandra. Those queries can be both reads and writes and both will have a
        /// settable consistency level. Setting only this property will indicate to the server to use this consistency
        /// level for both reads and writes in Cassandra. Read or write consistency level can be set separately with
        /// respectively
        /// <see cref="SetGraphReadConsistencyLevel(ConsistencyLevel)"/> and
        /// <see cref="SetGraphWriteConsistencyLevel(ConsistencyLevel)"/> will override the consistency set
        /// here.
        /// </para>
        /// </summary>
        IGraphStatement SetConsistencyLevel(ConsistencyLevel consistency);

        /// <summary>
        /// Sets the graph language to use with this statement.
        /// <para>
        /// This property is not required; if it is not set, the default <see cref="GraphOptions.Language"/> will be
        /// used when executing the statement.
        /// </para>
        /// </summary>
        IGraphStatement SetGraphLanguage(string language);

        /// <summary>
        /// Sets the graph name to use in graph queries.
        /// If you don't call this method, it is left unset.
        /// </summary>
        IGraphStatement SetGraphName(string name);

        /// <summary>
        /// Sets the consistency level used for the graph read query.
        /// <para>
        /// This setting will override the consistency level set with 
        /// <see cref="SetConsistencyLevel(ConsistencyLevel)"/> only for the READ part of the graph query.
        /// </para>
        /// </summary>
        IGraphStatement SetGraphReadConsistencyLevel(ConsistencyLevel consistency);

        /// <summary>
        /// Sets the graph traversal source name to use in graph queries.
        /// If you don't call this method, it defaults to <see cref="GraphOptions.Source"/>.
        /// </summary>
        IGraphStatement SetGraphSource(string source);

        /// <summary>
        /// Sets the graph source to the server-defined analytic traversal source ("a") for this statement.
        /// </summary>
        IGraphStatement SetGraphSourceAnalytics();

        /// <summary>
        /// Sets the consistency level used for the graph write query.
        /// <para>
        /// This setting will override the consistency level set with 
        /// <see cref="SetConsistencyLevel(ConsistencyLevel)"/> only for the WRITE part of the graph query.
        /// </para>
        /// </summary>
        IGraphStatement SetGraphWriteConsistencyLevel(ConsistencyLevel consistency);

        /// <summary>
        /// Sets the per-host read timeout in milliseconds for this statement.
        /// <para>Use <see cref="Timeout.Infinite"/> to disable timeouts for this Statement.</para>
        /// <para>Use zero to use the default value specified in the <see cref="GraphOptions.ReadTimeoutMillis"/>.</para>
        /// </summary>
        /// <param name="timeout">Timeout in milliseconds.</param>
        /// <returns>This instance</returns>
        IGraphStatement SetReadTimeoutMillis(int timeout);

        /// <summary>
        /// Forces this statement to use no graph name, even if a default graph name was defined 
        /// with <see cref="GraphOptions.SetName(string)"/>.
        /// <para>
        /// If a graph name was previously defined on this statement, it will be reset.
        /// </para>
        /// </summary>
        IGraphStatement SetSystemQuery();

        /// <summary>
        /// Sets the timestamp associated with this query.
        /// </summary>
        IGraphStatement SetTimestamp(DateTimeOffset timestamp);
    }
}
