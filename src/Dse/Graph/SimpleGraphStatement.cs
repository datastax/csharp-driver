using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;

namespace Dse.Graph
{
    /// <summary>
    /// Represents a graph query.
    /// </summary>
    public class SimpleGraphStatement : GraphStatement
    {
        private readonly string _query;
        private readonly object _values;

        /// <summary>
        /// Creates a new instance of <see cref="SimpleGraphStatement"/> using a query with no parameters.
        /// </summary>
        /// <param name="query">The graph query string.</param>
        public SimpleGraphStatement(string query) : this(query, null)
        {
            
        }

        /// <summary>
        /// Creates a new instance of <see cref="SimpleGraphStatement"/> using a query with named parameters.
        /// </summary>
        /// <param name="query">The graph query string.</param>
        /// <param name="values">An anonymous object containing the parameters as properties.</param>
        /// <example>new SimpleGraphStatement(&quot;g.V().has('name', myName)&quot;, new { myName = &quot;mark&quot;})</example>
        public SimpleGraphStatement(string query, object values)
        {
            _query = query;
            if (values != null && !IsAnonymous(values))
            {
                throw new ArgumentException("Expected anonymous object containing the parameters as properties", "values");
            }
            _values = values;
        }

        internal override IStatement GetIStatement()
        {
            IStatement stmt = null;
            if (_values != null)
            {
                stmt = new SimpleStatement(_query, _values);
            }
            else
            {
                stmt = new SimpleStatement(_query);
            }
            return stmt;
        }
    }
}