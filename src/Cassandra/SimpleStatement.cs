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

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Requests;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    ///  A simple <see cref="IStatement"/> implementation built directly from a query string.
    /// </summary>
    public class SimpleStatement : RegularStatement
    {
        private static readonly Logger Logger = new Logger(typeof(SimpleStatement));
        private string _query;
        private volatile RoutingKey _routingKey;
        private object[] _routingValues;
        private string _keyspace;

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

                var serializer = Serializer;
                if (serializer == null)
                {
                    serializer = Serialization.SerializerManager.Default.GetCurrentSerializer();
                    Logger.Warning("Calculating routing key before executing is not supported for SimpleStatement " +
                                   "instances, using default serializer.");
                }

                // Calculate the routing key
                return RoutingKey.Compose(
                    _routingValues
                    .Select(value => new RoutingKey(serializer.Serialize(value)))
                    .ToArray());
            }
        }

        /// <summary>
        /// Returns the keyspace this query operates on, as set using <see cref="SetKeyspace(string)"/>
        /// <para>
        /// The keyspace returned is used as a hint for token-aware routing.
        /// </para>
        /// </summary>
        /// <remarks>
        /// Consider using a <see cref="ISession"/> connected to single keyspace using 
        /// <see cref="ICluster.Connect(string)"/>.
        /// </remarks>
        public override string Keyspace
        {
            get { return _keyspace; }
        }

        /// <summary>
        /// Creates a new instance of <see cref="SimpleStatement"/> without any query string or parameters.
        /// </summary>
        public SimpleStatement()
        {
        }

        /// <summary>
        ///  Creates a new instance of <see cref="SimpleStatement"/> with the provided CQL query.
        /// </summary>
        /// <param name="query">The cql query string.</param>
        public SimpleStatement(string query)
        {
            _query = query;
        }

        /// <summary>
        ///  Creates a new instance of <see cref="SimpleStatement"/> with the provided CQL query and values provided.
        /// </summary>
        /// <param name="query">The cql query string.</param>
        /// <param name="values">Parameter values required for the execution of <c>query</c>.</param>
        /// <example>
        /// Using positional parameters:
        /// <code>
        /// const string query = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";
        /// var statement = new SimpleStatement(query, id, name, email);
        /// </code>
        /// Using named parameters (using anonymous objects):
        /// <code>
        /// const string query = "INSERT INTO users (id, name, email) VALUES (:id, :name, :email)";
        /// var statement = new SimpleStatement(query, new { id, name, email } );
        /// </code>
        /// </example>
        public SimpleStatement(string query, params object[] values) : this(query)
        {
            // ReSharper disable once DoNotCallOverridableMethodsInConstructor
            SetValues(values, Serializer);
        }

        /// <summary>
        /// Creates a new instance of <see cref="SimpleStatement"/> using a dictionary of parameters and a query with
        /// named parameters.
        /// </summary>
        /// <param name="valuesDictionary">
        /// A dictionary containing the query parameters values using the parameter name as keys.
        /// </param>
        /// <param name="query">The cql query string.</param>
        /// <remarks>
        /// This constructor is valid for dynamically-sized named parameters, consider using anonymous types for
        /// fixed-size named parameters.
        /// </remarks>
        /// <example>
        /// <code>
        /// const string query = "INSERT INTO users (id, name, email) VALUES (:id, :name, :email)";
        /// var parameters = new Dictionary&lt;string, object&gt; 
        /// {
        ///   { "id", id },
        ///   { "name", name },
        ///   { "email", email },
        /// };
        /// var statement = new SimpleStatement(parameters, query);
        /// </code>
        /// </example>
        /// <seealso cref="SimpleStatement(string, object[])"/>
        public SimpleStatement(IDictionary<string, object> valuesDictionary, string query)
        {
            if (valuesDictionary == null)
            {
                throw new ArgumentNullException("valuesDictionary");
            }

            _query = query ?? throw new ArgumentNullException("query");

            //The order of the keys and values is unspecified, but is guaranteed to be both in the same order.
            SetParameterNames(valuesDictionary.Keys);
            base.SetValues(valuesDictionary.Values.ToArray(), Serializer);
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
            SetValues(values, Serializer);
            return this;
        }

        [Obsolete("The method BindObject() is deprecated, use SimpleStatement constructor parameters to provide query values")]
        public SimpleStatement BindObjects(object[] values)
        {
            return Bind(values);
        }

        /// <summary>
        /// Sets the keyspace this Statement operates on. The keyspace should only be set when the
        /// <see cref="IStatement"/> applies to a different keyspace to the logged keyspace of the
        /// <see cref="ISession"/>.
        /// </summary>
        /// <param name="name">The keyspace name.</param>
        public SimpleStatement SetKeyspace(string name)
        {
            _keyspace = name;
            return this;
        }

        internal override IQueryRequest CreateBatchRequest(ISerializer serializer)
        {
            // Use the default query options as the individual options of the query will be ignored
            var options = QueryProtocolOptions.CreateForBatchItem(this);
            return new QueryRequest(serializer, QueryString, options, IsTracing, null);
        }

        internal override void SetValues(object[] values, ISerializer serializer)
        {
            if (values != null && values.Length == 1 && Utils.IsAnonymousType(values[0]))
            {
                var keyValues = Utils.GetValues(values[0]);
                SetParameterNames(keyValues.Keys);
                values = keyValues.Values.ToArray();
            }
            base.SetValues(values, serializer);
        }

        private void SetParameterNames(IEnumerable<string> names)
        {
            //Force named values to lowercase as identifiers are lowercased in Cassandra
            QueryValueNames = names.Select(k => k.ToLowerInvariant()).ToList();
        }
    }
}
