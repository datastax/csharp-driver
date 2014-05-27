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
    /// A statement that groups a number of <see cref="BoundStatement" /> and / or <see cref="SimpleStatement" /> so they get executed as a batch.
    /// </summary>
    public class BatchStatement : Statement
    {
        private readonly List<Statement> _queries = new List<Statement>();
        private BatchType _batchType = BatchType.Logged;
        private volatile RoutingKey _routingKey;

        /// <summary>
        /// Gets the batch type
        /// </summary>
        public BatchType BatchType
        {
            get { return _batchType; }
        }

        /// <summary>
        /// Determines if the batch does not contain any query
        /// </summary>
        public bool IsEmpty
        {
            get { return _queries.Count == 0; }
        }

        internal List<Statement> Queries
        {
            get { return _queries; }
        }

        /// <summary>
        ///  Gets the routing key for the query. <p> Note that unless the routing key has been
        ///  explicitly set through <link>#setRoutingKey</link>, this will method will
        ///  return <c>null</c> (to avoid having to parse the query string to
        ///  retrieve the partition key).</p>
        /// </summary>
        public override RoutingKey RoutingKey
        {
            get { return _routingKey; }
        }

        /// <summary>
        ///  Set the routing key for this query. <p> This method allows to manually
        ///  provide a routing key for this query. It is thus optional since the routing
        ///  key is only an hint for token aware load balancing policy but is never
        ///  mandatory. </p><p> If the partition key for the query is composite, use the
        ///  <link>#setRoutingKey(ByteBuffer...)</link> method instead to build the
        ///  routing key.</p>
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose to obtain the routing key.</param>
        /// <returns>this <c>BatchStatement</c> object.</returns>
        public BatchStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            _routingKey = RoutingKey.Compose(routingKeyComponents);
            return this;
        }

        /// <summary>
        /// Adds a new statement to this batch.
        /// Note that statement can be any <c>Statement</c>. It is allowed to mix <see cref="SimpleStatement"/> and <see cref="BoundStatement"/> in the same <c>BatchStatement</c> in particular.
        /// Please note that the options of the added <c>Statement</c> (all those defined directly by the Statement class: consistency level, fetch size, tracing, ...) will be ignored for the purpose of the execution of the Batch. Instead, the options used are the one of this <c>BatchStatement</c> object.
        /// </summary>
        /// <param name="statement">Statement to add to the batch</param>
        /// <returns>The Batch statement</returns>
        public BatchStatement Add(Statement statement)
        {
            _queries.Add(statement);
            return this;
        }

        /// <summary>
        /// Sets the <see cref="BatchType"/>
        /// </summary>
        /// <returns></returns>
        public BatchStatement SetBatchType(BatchType batchType)
        {
            _batchType = batchType;
            return this;
        }

        internal override IQueryRequest CreateBatchRequest()
        {
            throw new InvalidOperationException("Batches cannot be included recursively");
        }
    }
}