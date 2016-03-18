using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Cassandra;

namespace Dse.Graph
{
    /// <summary>
    /// Base class for graph statements.
    /// </summary>
    public abstract class GraphStatement : IGraphStatement
    {
        /// <inheritdoc/>
        public ConsistencyLevel? ConsistencyLevel { get; protected set; }

        /// <inheritdoc/>
        public string GraphAlias { get; protected set; }

        /// <inheritdoc/>
        public string GraphLanguage { get; protected set; }

        /// <inheritdoc/>
        public string GraphName { get; protected set; }

        /// <inheritdoc/>
        public ConsistencyLevel? GraphReadConsistencyLevel { get; protected set; }

        /// <inheritdoc/>
        public string GraphSource { get; protected set; }

        /// <inheritdoc/>
        public ConsistencyLevel? GraphWriteConsistencyLevel { get; protected set; }

        /// <inheritdoc/>
        public bool IsSystemQuery { get; protected set; }

        /// <summary>
        /// Gets the default timestamp associated with this query.
        /// </summary>
        public DateTimeOffset? Timestamp { get; protected set; }

        /// <summary>
        /// Gets the IStatement for this GraphStatement instance.
        /// </summary>
        internal abstract IStatement GetIStatement();

        /// <summary>
        /// Determines whether the object is anonymous.
        /// </summary>
        /// <exception cref="ArgumentNullException" />
        protected bool IsAnonymous(object obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj");
            }
            var type = obj.GetType();
            return type.IsGenericType
                   && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic
                   && (type.Name.StartsWith("<>", StringComparison.OrdinalIgnoreCase) || type.Name.StartsWith("VB$", StringComparison.OrdinalIgnoreCase))
                   && (type.Name.Contains("AnonymousType") || type.Name.Contains("AnonType"))
                   && Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false);
        }

        /// <summary>
        /// Sets the consistency level to use for this statement.
        /// <para>
        /// This setting will affect the general consistency when executing the gremlin query. However
        /// executing a gremlin query on the server side is going to involve the execution of CQL queries to the 
        /// persistence engine that is Cassandra. Those queries can be both reads and writes and both will have a
        /// settable consistency level. Setting only this property will indicate to the server to use this consistency
        /// level for both reads and writes in Cassandra. Read or write consistency level can be set separately with
        /// respectively
        /// <see cref="SetGraphReadConsistencyLevel(Cassandra.ConsistencyLevel)"/> and
        /// <see cref="SetGraphWriteConsistencyLevel(Cassandra.ConsistencyLevel)"/> will override the consistency set
        /// here.
        /// </para>
        /// </summary>
        public GraphStatement SetConsistencyLevel(ConsistencyLevel consistency)
        {
            ConsistencyLevel = consistency;
            return this;
        }

        /// <summary>
        /// Sets the graph alias to use in graph queries.
        /// If you don't call this method, it is left unset.
        /// </summary>
        public GraphStatement SetGraphAlias(string alias)
        {
            GraphAlias = alias;
            return this;
        }

        /// <summary>
        /// Sets the graph language to use with this statement.
        /// <para>
        /// This property is not required; if it is not set, the default <see cref="GraphOptions.Language"/> will be
        /// used when executing the statement.
        /// </para>
        /// </summary>
        public GraphStatement SetGraphLanguage(string language)
        {
            GraphLanguage = language;
            return this;
        }

        /// <summary>
        /// Sets the graph name to use in graph queries.
        /// If you don't call this method, it is left unset.
        /// </summary>
        public GraphStatement SetGraphName(string name)
        {
            GraphName = name;
            return this;
        }

        /// <summary>
        /// Sets the consistency level used for the graph read query.
        /// <para>
        /// This setting will override the consistency level set with 
        /// <see cref="SetConsistencyLevel(Cassandra.ConsistencyLevel)"/> only for the READ part of the graph query.
        /// </para>
        /// </summary>
        public GraphStatement SetGraphReadConsistencyLevel(ConsistencyLevel consistency)
        {
            GraphReadConsistencyLevel = consistency;
            return this;
        }

        /// <summary>
        /// Sets the graph traversal source name to use in graph queries.
        /// If you don't call this method, it defaults to <see cref="GraphOptions.Source"/>.
        /// </summary>
        public GraphStatement SetGraphSource(string source)
        {
            GraphSource = source;
            return this;
        }

        /// <summary>
        /// Sets the consistency level used for the graph write query.
        /// <para>
        /// This setting will override the consistency level set with 
        /// <see cref="SetConsistencyLevel(Cassandra.ConsistencyLevel)"/> only for the WRITE part of the graph query.
        /// </para>
        /// </summary>
        public GraphStatement SetGraphWriteConsistencyLevel(ConsistencyLevel consistency)
        {
            GraphReadConsistencyLevel = consistency;
            return this;
        }

        /// <summary>
        /// Forces this statement to use no graph name, even if a default graph name was defined 
        /// with <see cref="GraphOptions.SetName(string)"/>.
        /// <para>
        /// If a graph name was previously defined on this statement, it will be reset.
        /// </para>
        /// </summary>
        public GraphStatement SetSystemQuery()
        {
            IsSystemQuery = true;
            GraphName = null;
            return this;
        }

        /// <summary>
        /// Sets the timestamp associated with this query.
        /// </summary>
        public GraphStatement SetTimestamp(DateTimeOffset timestamp)
        {
            Timestamp = timestamp;
            return this;
        }

        IStatement IGraphStatement.ToIStatement()
        {
            return GetIStatement();
        }
    }
}
