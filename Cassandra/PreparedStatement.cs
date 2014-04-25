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
        private volatile ConsistencyLevel _consistency;
        private volatile RoutingKey _routingKey;

        internal readonly RowSetMetadata Metadata;
        internal readonly byte[] Id;

        internal PreparedStatement(RowSetMetadata metadata, byte[] id)
        {
            this.Metadata = metadata;
            this.Id = id;
        }

        /// <summary>
        ///  Gets metadata on the bounded variables of this prepared statement.
        /// </summary>
        public RowSetMetadata Variables { get { return Metadata; } }

        /// <summary>
        ///  Sets a default consistency level for all <code>BoundStatement</code> created
        ///  from this object. <p> If no consistency level is set through this method, the
        ///  BoundStatement created from this object will use the default consistency
        ///  level (One). </p><p> Changing the default consistency level is not retroactive,
        ///  it only applies to BoundStatement created after the change.</p>
        /// </summary>
        /// <param name="consistency"> the default consistency level to set. </param>
        /// 
        /// <returns>this <code>PreparedStatement</code> object.</returns>
        public PreparedStatement SetConsistencyLevel(ConsistencyLevel consistency)
        {
            this._consistency = consistency;
            return this;
        }

        public RoutingKey RoutingKey { get { return _routingKey; } }

        /// <summary>
        ///  Set the routing key for this query. <p> See
        ///  <link>#setRoutingKey(ByteBuffer)</link> for more information. This method is
        ///  a variant for when the query partition key is composite and thus the routing
        ///  key must be built from multiple values.</p>
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose to
        ///  obtain the routing key. </param>
        /// 
        /// <returns>this <code>PreparedStatement</code> object. <see>Query#GetRoutingKey</see></returns>
        public PreparedStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            this._routingKey = RoutingKey.Compose(routingKeyComponents); return this;
        }

        /// <summary>
        ///  Creates a new BoundStatement object and bind its variables to the provided
        ///  values. This method is a shortcut for <code>new
        ///  BoundStatement(this).Bind(...)</code>. <p> Note that while no more
        ///  <code>values</code> than bound variables can be provided, it is allowed to
        ///  provide less <code>values</code> that there is variables. In that case, the
        ///  remaining variables will have to be bound to values by another mean because
        ///  the resulting <code>BoundStatement</code> being executable.</p>
        /// </summary>
        /// <param name="values"> the values to bind to the variables of the newly
        ///  created BoundStatement. </param>
        /// 
        /// <returns>the newly created <code>BoundStatement</code> with its variables
        ///  bound to <code>values</code>. </returns>
        public BoundStatement Bind(params object[] values)
        {
            var bs = new BoundStatement(this);
            return bs.Bind(values);
        }
    }
}