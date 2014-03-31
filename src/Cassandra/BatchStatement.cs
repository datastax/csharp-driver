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
using System.Collections.Generic;

namespace Cassandra
{

    /// <summary>
    ///  A simple <code>Statement</code> implementation built directly from a query
    ///  string.
    /// </summary>
    public class BatchStatement : Query
    {

        private List<Query> _queries = new List<Query>();
        private BatchType _batchType = BatchType.Logged;
        private volatile RoutingKey _routingKey;

        /// <summary>
        ///  Creates a new <code>BatchStatement</code> with the provided query string.
        /// </summary>
        /// <param name="query"> the query string.</param>
        public BatchStatement()
        {
        }

        public bool IsEmpty { get { return _queries.Count == 0; } }

        public IEnumerable<Query> Queries { get { return _queries; } }

        /// <summary>
        ///  Gets the routing key for the query. <p> Note that unless the routing key has been
        ///  explicitly set through <link>#setRoutingKey</link>, this will method will
        ///  return <code>null</code> (to avoid having to parse the query string to
        ///  retrieve the partition key).</p>
        /// </summary>
        public override RoutingKey RoutingKey { get { return _routingKey; } }

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
        /// <returns>this <code>BatchStatement</code> object.
        ///  <see>Query#getRoutingKey</returns>
        public BatchStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            this._routingKey = RoutingKey.Compose(routingKeyComponents); return this;
        }

        public BatchStatement AddQuery(Query query)
        {
            this._queries.Add(query); return this;
        }

        public BatchStatement SetBatchType(BatchType batchType)
        {
            this._batchType = batchType; return this;
        }

        protected internal override IAsyncResult BeginSessionExecute(Session session, object tag, AsyncCallback callback, object state)
        {
            return session.BeginBatch(_batchType, _queries, callback, state, ConsistencyLevel, IsTracing, this, this, tag);
        }

        protected internal override RowSet EndSessionExecute(Session session, IAsyncResult ar)
        {
            return session.EndBatch(ar);
        }

        internal override IQueryRequest CreateBatchRequest()
        {
            throw new InvalidOperationException("Batches cannot be included recursivelly");
        }
    }
}