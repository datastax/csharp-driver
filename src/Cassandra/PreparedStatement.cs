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

using System.Collections.Generic;
using System.Linq;
using Cassandra.Requests;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    ///  Represents a prepared statement, a query with bound variables that has been
    ///  prepared (pre-parsed) by the database. <p> A prepared statement can be
    ///  executed once concrete values has been provided for the bound variables. The
    ///  pair of a prepared statement and values for its bound variables is a
    ///  BoundStatement and can be executed (by <link>Session#Execute</link>).</p>
    /// </summary>
    public class PreparedStatement
    {
        private readonly RowSetMetadata _variablesRowsMetadata;
        private readonly ISerializerManager _serializerManager = SerializerManager.Default;
        private volatile RoutingKey _routingKey;
        private string[] _routingNames;
        private volatile int[] _routingIndexes;
        private volatile ResultMetadata _resultMetadata;

        /// <summary>
        /// The cql query
        /// </summary>
        internal string Cql { get; private set; }

        /// <summary>
        /// The prepared statement identifier
        /// </summary>
        internal byte[] Id { get; private set; }

        /// <summary>
        /// The keyspace were the prepared statement was first executed
        /// </summary>
        internal string Keyspace { get; private set; }

        /// <summary>
        /// Gets the the incoming payload, that is, the payload that the server
        /// sent back with its prepared response, or null if the server did not include any custom payload.
        /// </summary>
        public IDictionary<string, byte[]> IncomingPayload { get; internal set; }

        /// <summary>
        /// Gets custom payload for that will be included when executing an Statement.
        /// </summary>
        public IDictionary<string, byte[]> OutgoingPayload { get; private set; }

        /// <summary>
        ///  Gets metadata on the bounded variables of this prepared statement.
        /// </summary>
        public RowSetMetadata Variables
        {
            get { return _variablesRowsMetadata; }
        }
        
        /// <summary>
        ///  Gets metadata on the columns that will be returned for this prepared statement.
        /// </summary>
        internal ResultMetadata ResultMetadata
        {
            get { return _resultMetadata; }
        }

        /// <summary>
        /// Gets the routing key for the prepared statement.
        /// </summary>
        public RoutingKey RoutingKey
        {
            get { return _routingKey; }
        }

        /// <summary>
        /// Gets or sets the parameter indexes that are part of the partition key
        /// </summary>
        public int[] RoutingIndexes
        {
            get { return _routingIndexes; }
            internal set { _routingIndexes = value; }
        }

        /// <summary>
        /// Gets the default consistency level for all executions using this instance
        /// </summary>
        public ConsistencyLevel? ConsistencyLevel { get; private set; }

        /// <summary>
        /// Determines if the query is idempotent, i.e. whether it can be applied multiple times without 
        /// changing the result beyond the initial application.
        /// <para>
        /// Idempotence of the prepared statement plays a role in <see cref="ISpeculativeExecutionPolicy"/>.
        /// If a query is <em>not idempotent</em>, the driver will not schedule speculative executions for it.
        /// </para>
        /// When the property is null, the driver will use the default value from the <see cref="QueryOptions.GetDefaultIdempotence()"/>.
        /// </summary>
        public bool? IsIdempotent { get; private set; }

        /// <summary>
        /// Initializes a new instance of the Cassandra.PreparedStatement class
        /// </summary>
        public PreparedStatement()
        {
            //Default constructor for client test and mocking frameworks
        }

        internal PreparedStatement(RowSetMetadata variablesRowsMetadata, byte[] id, ResultMetadata resultMetadata, string cql,
                                   string keyspace, ISerializerManager serializer)
        {
            _variablesRowsMetadata = variablesRowsMetadata;
            _resultMetadata = resultMetadata;
            Id = id;
            Cql = cql;
            Keyspace = keyspace;
            _serializerManager = serializer;
        }

        internal void UpdateResultMetadata(ResultMetadata resultMetadata)
        {
            _resultMetadata = resultMetadata;
        }

        /// <summary>
        /// <para>
        /// Creates a new <see cref="BoundStatement"/> instance with the provided parameter values.
        /// </para>
        /// <para>
        /// You can specify the parameter values by the position of the markers in the query, or by name 
        /// using a single instance of an anonymous type, with property names as parameter names.
        /// </para>
        /// <para>
        /// Note that while no more <c>values</c> than bound variables can be provided, it is allowed to
        /// provide less <c>values</c> that there is variables.
        /// </para>
        /// <para>
        /// You can provide a comma-separated variable number of arguments to the <c>Bind()</c> method. When providing
        /// an array, the reference might be used by the driver making it not safe to modify its content.
        /// </para>
        /// </summary>
        /// <param name="values">The values to bind to the variables of the newly created BoundStatement.</param>
        /// <returns>The newly created <see cref="BoundStatement"/> with the query parameters set.</returns>
        /// <example>
        /// Binding different parameters:
        /// <code>
        /// PreparedStatement ps = session.Prepare("INSERT INTO table (id, name) VALUES (?, ?)");
        /// BoundStatement statement = ps.Bind(Guid.NewGuid(), "Franz Ferdinand");
        /// session.Execute(statement);
        /// </code>
        /// </example>
        public virtual BoundStatement Bind(params object[] values)
        {
            var bs = new BoundStatement(this);
            bs.SetRoutingKey(_routingKey);
            if (values == null)
            {
                return bs;
            }
            var valuesByPosition = values;
            var useNamedParameters = values.Length == 1 && Utils.IsAnonymousType(values[0]);
            if (useNamedParameters)
            {
                //Using named parameters
                //Reorder the params according the position in the query
                valuesByPosition = Utils.GetValues(_variablesRowsMetadata.Columns.Select(c => c.Name), values[0]).ToArray();
            }

            var serializer = _serializerManager.GetCurrentSerializer();
            bs.SetValues(valuesByPosition, serializer);
            bs.CalculateRoutingKey(serializer, useNamedParameters, RoutingIndexes, _routingNames, valuesByPosition, values);
            return bs;
        }

        /// <summary>
        ///  Sets a default consistency level for all <c>BoundStatement</c> created
        ///  from this object. <p> If no consistency level is set through this method, the
        ///  BoundStatement created from this object will use the default consistency
        ///  level (One). </p><p> Changing the default consistency level is not retroactive,
        ///  it only applies to BoundStatement created after the change.</p>
        /// </summary>
        /// <param name="consistency"> the default consistency level to set. </param>
        /// <returns>this <c>PreparedStatement</c> object.</returns>
        public PreparedStatement SetConsistencyLevel(ConsistencyLevel consistency)
        {
            ConsistencyLevel = consistency;
            return this;
        }

        /// <summary>
        /// Sets the partition keys of the query
        /// </summary>
        /// <returns>True if it was possible to set the routing indexes for this query</returns>
        internal bool SetPartitionKeys(TableColumn[] keys)
        {
            var queryParameters = _variablesRowsMetadata.Columns;
            var routingIndexes = new List<int>();
            foreach (var key in keys)
            {
                //find the position of the key in the parameters
                for (var i = 0; i < queryParameters.Length; i++)
                {
                    if (queryParameters[i].Name != key.Name)
                    {
                        continue;
                    }
                    routingIndexes.Add(i);
                    break;
                }
            }
            if (routingIndexes.Count != keys.Length)
            {
                //The parameter names don't match the partition keys
                return false;
            }
            _routingIndexes = routingIndexes.ToArray();
            return true;
        }

        /// <summary>
        /// Set the routing key for this query.
        /// <para>
        /// The routing key is a hint for token aware load balancing policies but is never mandatory.
        /// This method allows you to manually provide a routing key for this query.
        /// </para>
        /// <para>
        /// Use this method ONLY if the partition keys are the same for all query executions (hard-coded parameters).
        /// </para>
        /// <para>
        /// If the partition key is composite, you should provide multiple routing key components.
        /// </para>
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose to
        ///  obtain the routing key. </param>
        /// <returns>this <c>PreparedStatement</c> object.</returns>
        public PreparedStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            _routingKey = RoutingKey.Compose(routingKeyComponents);
            return this;
        }

        /// <summary>
        /// For named query markers, it sets the parameter names that are part of the routing key.
        /// <para>
        /// Use this method ONLY if the parameter names are different from the partition key names.
        /// </para>
        /// </summary>
        /// <returns>this <c>PreparedStatement</c> object.</returns>
        public PreparedStatement SetRoutingNames(params string[] names)
        {
            if (names == null)
            {
                return this;
            }
            _routingNames = names;
            return this;
        }

        /// <summary>
        /// Sets whether the prepared statement is idempotent.
        /// <para>
        /// Idempotence of the query plays a role in <see cref="ISpeculativeExecutionPolicy"/>.
        /// If a query is <em>not idempotent</em>, the driver will not schedule speculative executions for it.
        /// </para>
        /// </summary>
        public PreparedStatement SetIdempotence(bool value)
        {
            IsIdempotent = value;
            return this;
        }

        /// <summary>
        /// Sets a custom outgoing payload for this statement.
        /// Each time an statement generated using this prepared statement is executed, this payload will be included in the request.
        /// Once it is set using this method, the payload should not be modified.
        /// </summary>
        public PreparedStatement SetOutgoingPayload(IDictionary<string, byte[]> payload)
        {
            OutgoingPayload = payload;
            return this;
        }    }
}
