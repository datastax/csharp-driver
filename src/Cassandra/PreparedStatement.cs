//
//      Copyright (C) 2012 DataStax Inc.
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
        internal readonly string Cql;
        internal readonly byte[] Id;
        internal readonly RowSetMetadata Metadata;
        internal readonly RowSetMetadata ResultMetadata;
        private volatile ConsistencyLevel _consistency;
        private volatile RoutingKey _routingKey;

        /// <summary>
        ///  Gets metadata on the bounded variables of this prepared statement.
        /// </summary>
        public RowSetMetadata Variables
        {
            get { return Metadata; }
        }

        public RoutingKey RoutingKey
        {
            get { return _routingKey; }
        }

        internal PreparedStatement(RowSetMetadata metadata, byte[] id, string cql, RowSetMetadata resultMetadata)
        {
            Metadata = metadata;
            Id = id;
            Cql = cql;
            ResultMetadata = resultMetadata;
        }

        /// <summary>
        ///  Sets a default consistency level for all <c>BoundStatement</c> created
        ///  from this object. <p> If no consistency level is set through this method, the
        ///  BoundStatement created from this object will use the default consistency
        ///  level (One). </p><p> Changing the default consistency level is not retroactive,
        ///  it only applies to BoundStatement created after the change.</p>
        /// </summary>
        /// <param name="consistency"> the default consistency level to set. </param>
        /// 
        /// <returns>this <c>PreparedStatement</c> object.</returns>
        public PreparedStatement SetConsistencyLevel(ConsistencyLevel consistency)
        {
            _consistency = consistency;
            return this;
        }

        /// <summary>
        ///  Set the routing key for this query. <p> See
        ///  <link>#setRoutingKey(ByteBuffer)</link> for more information. This method is
        ///  a variant for when the query partition key is composite and thus the routing
        ///  key must be built from multiple values.</p>
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose to
        ///  obtain the routing key. </param>
        /// <returns>this <c>PreparedStatement</c> object.  <see>Query#GetRoutingKey</see></returns>
        public PreparedStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            _routingKey = RoutingKey.Compose(routingKeyComponents);
            return this;
        }

        /// <summary>
        ///  Creates a new BoundStatement object and bind its variables to the provided
        ///  values. This method is a shortcut for <c>new
        ///  BoundStatement(this).Bind(...)</c>. <p> Note that while no more
        ///  <c>values</c> than bound variables can be provided, it is allowed to
        ///  provide less <c>values</c> that there is variables. In that case, the
        ///  remaining variables will have to be bound to values by another mean because
        ///  the resulting <c>BoundStatement</c> being executable.</p>
        /// </summary>
        /// <param name="values"> the values to bind to the variables of the newly
        ///  created BoundStatement. </param>
        /// 
        /// <returns>the newly created <c>BoundStatement</c> with its variables
        ///  bound to <c>values</c>. </returns>
        public BoundStatement Bind(params object[] values)
        {
            var bs = new BoundStatement(this);
            bs.SetValues(values);

            return bs;
        }
    }
}