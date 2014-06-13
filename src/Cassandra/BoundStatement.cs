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

using System;

namespace Cassandra
{
    /// <summary>
    ///  A prepared statement with values bound to the bind variables. <p> Once a
    ///  BoundStatement has values for all the variables of the
    ///  <see cref="PreparedStatement"/> it has been created from, it can executed
    ///  (through <see cref="ISession.Execute(IStatement)"/>). </p><p> The values of a BoundStatement
    ///  can be set by either index or name. When setting them by name, names follow
    ///  the case insensitivity rules explained in <link>ColumnDefinitions</link>.
    ///  Note-worthily, if multiple bind variables correspond to the same column (as
    ///  would be the case if you prepare <c>SELECT * FROM t WHERE x &gt; ? AND x &lt; ?</c>), 
    ///  you will have to set values by indexes (or the <c>PreparedStatement.Bind(object[])</c>
    ///  method) as the methods to set by name only allows to set the first prepared
    ///  occurrence of the column.</p>
    /// <seealso cref="Cassandra.PreparedStatement"/>
    /// </summary>
    public class BoundStatement : Statement
    {
        private readonly PreparedStatement _statement;
        private RoutingKey _routingKey;

        /// <summary>
        ///  Gets the prepared statement on which this BoundStatement is based.
        /// </summary>
        public PreparedStatement PreparedStatement
        {
            get { return _statement; }
        }


        /// <summary>
        ///  Gets the routing key for this bound query. <p> This method will return a
        ///  non-<c>null</c> value if: <ul> <li>either all the TableColumns composing the
        ///  partition key are bound variables of this <c>BoundStatement</c>. The
        ///  routing key will then be built using the values provided for these partition
        ///  key TableColumns.</li> <li>or the routing key has been set through
        ///  <c>PreparedStatement.SetRoutingKey</c> for the
        ///  <see cref="PreparedStatement"/> this statement has been built from.</li> </ul>
        ///  Otherwise, <c>null</c> is returned.</p> <p> Note that if the routing key
        ///  has been set through <link>PreparedStatement.SetRoutingKey</link>, that value
        ///  takes precedence even if the partition key is part of the bound variables.</p>
        /// </summary>
        public override RoutingKey RoutingKey
        {
            get { return _routingKey; }
        }

        /// <summary>
        ///  Creates a new <c>BoundStatement</c> from the provided prepared
        ///  statement.
        /// </summary>
        /// <param name="statement"> the prepared statement from which to create a <c>BoundStatement</c>.</param>
        public BoundStatement(PreparedStatement statement)
        {
            _statement = statement;
            _routingKey = statement.RoutingKey;
        }
        
        /// <summary>
        ///  Set the routing key for this query. This method allows to manually
        ///  provide a routing key for this BoundStatement. It is thus optional since the routing
        ///  key is only an hint for token aware load balancing policy but is never
        ///  mandatory.
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose the routing key.</param>
        public BoundStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            this._routingKey = RoutingKey.Compose(routingKeyComponents);
            return this;
        }

        internal override IQueryRequest CreateBatchRequest()
        {
            return new ExecuteRequest(-1, PreparedStatement.Id, PreparedStatement.Metadata, IsTracing,
                                      QueryProtocolOptions.CreateFromQuery(this, Cassandra.ConsistencyLevel.Any));
                // this Cassandra.ConsistencyLevel.Any is not used due fact that BATCH got own CL 
        }
    }
}