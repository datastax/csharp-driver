//
//      Copyright (C) 2012-2014 DataStax Inc.
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
        internal readonly RowSetMetadata Metadata;
        internal readonly RowSetMetadata ResultMetadata;
        private readonly int _protocolVersion;
        private volatile RoutingKey _routingKey;
        private string[] _routingNames;

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
        ///  Gets metadata on the bounded variables of this prepared statement.
        /// </summary>
        public RowSetMetadata Variables
        {
            get { return Metadata; }
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
        public int[] RoutingIndexes { get; internal set; }

        public ConsistencyLevel? ConsistencyLevel
        {
            get;
            private set;
        }

        internal PreparedStatement(RowSetMetadata metadata, byte[] id, string cql, string keyspace, RowSetMetadata resultMetadata, int protocolVersion)
        {
            Metadata = metadata;
            Id = id;
            Cql = cql;
            ResultMetadata = resultMetadata;
            Keyspace = keyspace;
            _protocolVersion = protocolVersion;
        }

        /// <summary>
        /// Creates a new BoundStatement object and bind its variables to the provided
        /// values.
        /// <para>
        /// Specify the parameter values by the position of the markers in the query or by name, 
        /// using a single instance of an anonymous type, with property names as parameter names.
        /// </para>
        /// <para>
        /// Note that while no more <c>values</c> than bound variables can be provided, it is allowed to
        /// provide less <c>values</c> that there is variables.
        /// </para>
        /// </summary>
        /// <param name="values"> the values to bind to the variables of the newly
        ///  created BoundStatement. </param>
        /// <returns>the newly created <c>BoundStatement</c> with its variables
        ///  bound to <c>values</c>. </returns>
        public BoundStatement Bind(params object[] values)
        {
            var bs = new BoundStatement(this);
            if (values == null)
            {
                return bs;
            }
            var valuesByPosition = values;
            var useNamedParameters = values.Length == 1 && Utils.IsAnonymousType(values[0]);
            if (useNamedParameters)
            {
                //Using named params
                //Reorder the params according the position in the query
                valuesByPosition = Utils.GetValues(Metadata.Columns.Select(c => c.Name), values[0]).ToArray();
            }
            bs.SetValues(valuesByPosition);
            if (_routingKey != null)
            {
                //The routing key was specified by the user
                return bs;
            }
            if (RoutingIndexes != null)
            {
                var keys = new RoutingKey[RoutingIndexes.Length];
                for (var i = 0; i < RoutingIndexes.Length; i++)
                {
                    var index = RoutingIndexes[i];
                    keys[i] = new RoutingKey(TypeCodec.Encode(_protocolVersion, valuesByPosition[index]));
                }
                bs.SetRoutingKey(keys);
                return bs;
            }
            if (_routingNames != null && useNamedParameters)
            {
                var keys = new RoutingKey[_routingNames.Length];
                var routingValues = Utils.GetValues(_routingNames, values[0]).ToArray();
                if (routingValues.Length != keys.Length)
                {
                    //The routing names are not valid
                    return bs;
                }
                for (var i = 0; i < routingValues.Length; i++)
                {
                    keys[i] = new RoutingKey(TypeCodec.Encode(_protocolVersion, routingValues[i]));
                }
                bs.SetRoutingKey(keys);
                return bs;
            }
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
            var queryParameters = Metadata.Columns;
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
            RoutingIndexes = routingIndexes.ToArray();
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
    }
}
