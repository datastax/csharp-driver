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

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Serialization.Graph;
using Cassandra.Serialization.Graph.GraphSON1;
using Cassandra.Serialization.Graph.GraphSON2;
using Newtonsoft.Json;

namespace Cassandra.DataStax.Graph
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
            IDictionary<string, object> parameters = null;
            if (ValuesDictionary != null)
            {
                parameters = ValuesDictionary;
            }
            else if (Values != null)
            {
                parameters = Utils.GetValues(Values);
            }

            var graphProtocol = GraphProtocolVersion ?? options.GraphProtocolVersion;

            IStatement stmt;
            if (parameters != null)
            {
                string jsonParams;
                switch (graphProtocol)
                {
                    case GraphProtocol.GraphSON1:
                        jsonParams = JsonConvert.SerializeObject(parameters, GraphSON1ContractResolver.Settings);
                        break;
                    case GraphProtocol.GraphSON2:
                    case GraphProtocol.GraphSON3:
                        jsonParams = JsonConvert.SerializeObject(
                            parameters.ToDictionary(
                                kvp => kvp.Key,
                                kvp => GraphSONTypeConverter.DefaultInstance.ToDb(kvp.Value)),
                            GraphSONNode.GraphSONSerializerSettings);
                        break;
                    default:
                        throw new DriverInternalError(
                            "Could not resolve graph protocol version. This is a bug, please report.");
                }
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