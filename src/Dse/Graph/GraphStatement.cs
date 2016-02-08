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
        public string GraphAlias { get; protected set; }

        /// <inheritdoc/>
        public string GraphLanguage { get; protected set; }

        /// <inheritdoc/>
        public string GraphName { get; protected set; }

        /// <inheritdoc/>
        public string GraphSource { get; protected set; }

        /// <inheritdoc/>
        public bool IsSystemQuery { get; protected set; }

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
        /// Sets the graph traversal source name to use in graph queries.
        /// If you don't call this method, it defaults to <see cref="GraphOptions.Source"/>.
        /// </summary>
        public GraphStatement SetGraphSource(string source)
        {
            GraphSource = source;
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

        IStatement IGraphStatement.ToIStatement()
        {
            return GetIStatement();
        }
    }
}
