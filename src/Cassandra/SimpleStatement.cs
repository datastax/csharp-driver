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
using Cassandra.Requests;

namespace Cassandra
{
    /// <summary>
    ///  A simple <c>Statement</c> implementation built directly from a query
    ///  string.
    /// </summary>
    public class SimpleStatement : RegularStatement
    {
        private string _query;
        private volatile RoutingKey _routingKey;
        private object[] _routingValues;

        /// <summary>
        ///  Gets the query string.
        /// </summary>
        public override string QueryString
        {
            get { return _query; }
        }

        /// <summary>
        /// Gets the routing key for the query.
        /// <para>
        /// Routing key can be provided using the <see cref="SetRoutingValues"/> method.
        /// </para>
        /// </summary>
        public override RoutingKey RoutingKey
        {
            get
            {
                if (_routingKey != null)
                {
                    return _routingKey;   
                }
                if (_routingValues == null)
                {
                    return null;
                }
                //Calculate the routing key
                return RoutingKey.Compose(
                    _routingValues
                    .Select(key => new RoutingKey(TypeCodec.Encode(ProtocolVersion, key)))
                    .ToArray());
            }
        }

        public SimpleStatement()
        {
        }

        /// <summary>
        ///  Creates a new instance of <c>SimpleStatement</c> with the provided CQL query.
        /// </summary>
        /// <param name="query">The cql query string.</param>
        public SimpleStatement(string query)
        {
            _query = query;
        }

        /// <summary>
        ///  Creates a new instance of <c>SimpleStatement</c> with the provided CQL query and values provided.
        /// </summary>
        /// <param name="query">The cql query string</param>
        /// <param name="values">Parameter values required for the execution of <c>query</c></param>
        public SimpleStatement(string query, params object[] values) : this(query)
        {
            // ReSharper disable once DoNotCallOverridableMethodsInConstructor
            SetValues(values);
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
        /// <returns>this <c>SimpleStatement</c> object.  <see>Query#getRoutingKey</see></returns>
        public SimpleStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            _routingKey = RoutingKey.Compose(routingKeyComponents);
            return this;
        }

        /// <summary>
        /// Sets the partition key values in order to route the query to the correct replicas.
        /// <para>For simple partition keys, set the partition key value.</para>
        /// <para>For composite partition keys, set the multiple the partition key values in correct order.</para>
        /// </summary>
        public SimpleStatement SetRoutingValues(params object[] keys)
        {
            _routingValues = keys;
            return this;
        }

        public SimpleStatement SetQueryString(string queryString)
        {
            _query = queryString;
            return this;
        }

        /// <summary>
        /// Sets the parameter values for the query.
        /// <para>
        /// The same amount of values must be provided as parameter markers in the query.
        /// </para>
        /// <para>
        /// Specify the parameter values by the position of the markers in the query or by name, 
        /// using a single instance of an anonymous type, with property names as parameter names.
        /// </para>
        /// </summary>
        [Obsolete("The method Bind() is deprecated, use SimpleStatement constructor parameters to provide query values")]
        public SimpleStatement Bind(params object[] values)
        {
            SetValues(values);
            return this;
        }

        [Obsolete("The method BindObject() is deprecated, use SimpleStatement constructor parameters to provide query values")]
        public SimpleStatement BindObjects(object[] values)
        {
            return Bind(values);
        }

        internal override IQueryRequest CreateBatchRequest(int protocolVersion)
        {
            //Uses the default query options as the individual options of the query will be ignored
            var options = QueryProtocolOptions.CreateFromQuery(this, new QueryOptions());
            return new QueryRequest(protocolVersion, QueryString, IsTracing, options);
        }

        internal override void SetValues(object[] values)
        {
            if (values != null && values.Length == 1 && Utils.IsAnonymousType(values[0]))
            {
                var keyValues = Utils.GetValues(values[0]);
                //Force named values to lowercase as identifiers are lowercased in Cassandra
                QueryValueNames = keyValues.Keys.Select(k => k.ToLowerInvariant()).ToList();
                values = keyValues.Values.ToArray();
            }
            base.SetValues(values);
        }
    }
}
