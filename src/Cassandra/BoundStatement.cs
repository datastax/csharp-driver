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
    ///  <link>PreparedStatement</link> it has been created from, it can executed
    ///  (through <link>Session#execute</link>). </p><p> The values of a BoundStatement
    ///  can be set by either index or name. When setting them by name, names follow
    ///  the case insensitivity rules explained in <link>ColumnDefinitions</link>.
    ///  Noteworthily, if multiple bind variables correspond to the same column (as
    ///  would be the case if you prepare <code>SELECT * FROM t WHERE x &gt; ? AND x &lt; ?</code>), 
    ///  you will have to set values by indexes (or the <link>#bind</link>
    ///  method) as the methods to set by name only allows to set the first prepared
    ///  occurrence of the column.</p>
    /// </summary>
    public class BoundStatement : Query
    {
        private readonly PreparedStatement _statement;

        /// <summary>
        ///  Gets the prepared statement on which this BoundStatement is based.
        /// </summary>
        public PreparedStatement PreparedStatement
        {
            get { return _statement; }
        }


        /// <summary>
        ///  Gets the routing key for this bound query. <p> This method will return a
        ///  non-<code>null</code> value if: <ul> <li>either all the TableColumns composing the
        ///  partition key are bound variables of this <code>BoundStatement</code>. The
        ///  routing key will then be built using the values provided for these partition
        ///  key TableColumns.</li> <li>or the routing key has been set through
        ///  <link>PreparedStatement#setRoutingKey</link> for the
        ///  <code>PreparedStatement</code> this statement has been built from.</li> </ul>
        ///  Otherwise, <code>null</code> is returned.</p> <p> Note that if the routing key
        ///  has been set through <link>PreparedStatement#setRoutingKey</link>, that value
        ///  takes precedence even if the partition key is part of the bound variables.</p>
        /// </summary>
        public override RoutingKey RoutingKey
        {
            get
            {
                if (_statement.RoutingKey != null)
                    return _statement.RoutingKey;

                return null;
            }
        }

        /// <summary>
        ///  Creates a new <code>BoundStatement</code> from the provided prepared
        ///  statement.
        /// </summary>
        /// <param name="statement"> the prepared statement from which to create a <code>BoundStatement</code>.</param>
        public BoundStatement(PreparedStatement statement)
        {
            _statement = statement;
        }

        protected internal override IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state)
        {
            return session.BeginExecuteQuery(PreparedStatement.Id, PreparedStatement.Metadata,
                                             QueryProtocolOptions.CreateFromQuery(this,
                                                                                  session.Cluster.Configuration.QueryOptions.GetConsistencyLevel()),
                                             callback, state, ConsistencyLevel, this, this, tag, IsTracing);
        }

        protected internal override RowSet EndSessionExecute(Session session, IAsyncResult ar)
        {
            return session.EndExecuteQuery(ar);
        }

        internal override IQueryRequest CreateBatchRequest()
        {
            return new ExecuteRequest(-1, PreparedStatement.Id, PreparedStatement.Metadata, IsTracing,
                                      QueryProtocolOptions.CreateFromQuery(this, Cassandra.ConsistencyLevel.Any));
                // this Cassandra.ConsistencyLevel.Any is not used due fact that BATCH got own CL 
        }
    }
}