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
    ///  A simple <code>Statement</code> implementation built directly from a query
    ///  string.
    /// </summary>
    public class SimpleStatement : Statement
    {
        private string _query;
        private volatile RoutingKey _routingKey;

        /// <summary>
        ///  Gets the query string.
        /// </summary>
        public override string QueryString
        {
            get { return _query; }
        }

        /// <summary>
        ///  Gets the routing key for the query. <p> Note that unless the routing key has been
        ///  explicitly set through <link>#setRoutingKey</link>, this will method will
        ///  return <code>null</code> (to avoid having to parse the query string to
        ///  retrieve the partition key).</p>
        /// </summary>
        public override RoutingKey RoutingKey
        {
            get { return _routingKey; }
        }

        public SimpleStatement() : base(QueryProtocolOptions.DEFAULT)
        {
        }

        /// <summary>
        ///  Creates a new <code>SimpleStatement</code> with the provided query string.
        /// </summary>
        /// <param name="query"> the query string.</param>
        public SimpleStatement(string query)
            : base(QueryProtocolOptions.DEFAULT)
        {
            _query = query;
        }

        internal SimpleStatement(string query, QueryProtocolOptions queryProtocolOptions)
            : base(queryProtocolOptions)
        {
            _query = query;
            SetConsistencyLevel(queryProtocolOptions.Consistency);
            SetSerialConsistencyLevel(queryProtocolOptions.SerialConsistency);
            SetPageSize(queryProtocolOptions.PageSize);
        }

        /// <summary>
        ///  Set the routing key for this query. <p> This method allows to manually
        ///  provide a routing key for this query. It is thus optional since the routing
        ///  key is only an hint for token aware load balancing policy but is never
        ///  mandatory. </p><p> If the partition key for the query is composite, use the
        ///  <link>#setRoutingKey(ByteBuffer...)</link> method instead to build the
        ///  routing key.</p>
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose to
        ///  obtain the routing key.
        ///  </param>
        /// 
        /// <returns>this <code>SimpleStatement</code> object.
        ///  <see>Query#getRoutingKey</returns>
        public SimpleStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            _routingKey = RoutingKey.Compose(routingKeyComponents);
            return this;
        }


        protected internal override IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state)
        {
            return session.BeginQuery(QueryString, callback, state,
                                      QueryProtocolOptions.CreateFromQuery(this, session.Cluster.Configuration.QueryOptions.GetConsistencyLevel()),
                                      ConsistencyLevel, IsTracing, this, this, tag);
        }

        protected internal override RowSet EndSessionExecute(Session session, IAsyncResult ar)
        {
            return session.EndQuery(ar);
        }

        public SimpleStatement SetQueryString(string queryString)
        {
            _query = queryString;
            return this;
        }

        public SimpleStatement Bind(params object[] values)
        {
            SetValues(values);
            return this;
        }

        public SimpleStatement BindObjects(object[] values)
        {
            SetValues(values);
            return this;
        }

        internal override IQueryRequest CreateBatchRequest()
        {
            return new QueryRequest(-1, QueryString, IsTracing, QueryProtocolOptions.CreateFromQuery(this, Cassandra.ConsistencyLevel.Any));
                // this Cassandra.ConsistencyLevel.Any is not used due fact that BATCH got own CL 
        }
    }
}