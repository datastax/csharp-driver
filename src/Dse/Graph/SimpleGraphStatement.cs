//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using Dse.Serialization.Graph.GraphSON1;
using Newtonsoft.Json;

namespace Dse.Graph
{
    /// <summary>
    /// Represents a graph query.
    /// </summary>
    public class SimpleGraphStatement : GraphStatement
    {
        /// <summary>
        /// The underlying query string
        /// </summary>
        public string Query { get; private set; }

        /// <summary>
        /// Values object used for parameter substitution in the query string
        /// </summary>
        public object Values { get; private set; }

        /// <summary>
        /// Values dictionary used for parameter substitution in the query string
        /// </summary>
        public IDictionary<string, object> ValuesDictionary { get; private set; }

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
        /// <example>
        /// <code>new SimpleGraphStatement(&quot;g.V().has('name', myName)&quot;, new { myName = &quot;mark&quot;})</code>
        /// </example>
        public SimpleGraphStatement(string query, object values)
        {
            Query = query ?? throw new ArgumentNullException("query");
            if (values != null && !IsAnonymous(values))
            {
                throw new ArgumentException("Expected anonymous object containing the parameters as properties", "values");
            }
            Values = values;
        }

        /// <summary>
        /// Creates a new instance of <see cref="SimpleGraphStatement"/> using a query with named parameters.
        /// </summary>
        /// <param name="values">An Dictionary object containing the parameters name and values as key and values.</param>
        /// <param name="query">The graph query string.</param>
        /// <example>
        /// <code>
        /// new SimpleGraphStatement(
        ///     new Dictionary&lt;string, object&gt;{ { &quot;myName&quot;, &quot;mark&quot; } }, 
        ///     &quot;g.V().has('name', myName)&quot;)
        /// </code>
        /// </example>
        public SimpleGraphStatement(IDictionary<string, object> values, string query)
        {
            Query = query ?? throw new ArgumentNullException("query");
            ValuesDictionary = values ?? throw new ArgumentNullException("values");
        }

        internal override IStatement GetIStatement(GraphOptions options)
        {
            var parameters = ValuesDictionary ?? Values;
            IStatement stmt;
            if (parameters != null)
            {
                var jsonParams = JsonConvert.SerializeObject(parameters, GraphSON1ContractResolver.Settings);
                stmt = new TargettedSimpleStatement(Query, jsonParams);
            }
            else
            {
                stmt = new TargettedSimpleStatement(Query);
            }
            //Set Cassandra.Statement properties
            if (Timestamp != null)
            {
                stmt.SetTimestamp(Timestamp.Value);
            }
            var readTimeout = ReadTimeoutMillis != 0 ? ReadTimeoutMillis : options.ReadTimeoutMillis;
            if (readTimeout <= 0)
            {
                // Infinite (-1) is not supported in the core driver, set an arbitrarily large int
                readTimeout = int.MaxValue;
            }
            return stmt
                .SetIdempotence(false)
                .SetConsistencyLevel(ConsistencyLevel)
                .SetReadTimeoutMillis(readTimeout)
                .SetOutgoingPayload(options.BuildPayload(this));
        }
    }
}