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
using System.Linq;
using Cassandra.Requests;

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
        private readonly PreparedStatement _preparedStatement;
        private RoutingKey _routingKey;

        /// <summary>
        ///  Gets the prepared statement on which this BoundStatement is based.
        /// </summary>
        public PreparedStatement PreparedStatement
        {
            get { return _preparedStatement; }
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
        /// Initializes a new instance of the Cassandra.BoundStatement class
        /// </summary>
        public BoundStatement()
        {
            //Default constructor for client test and mocking frameworks
        }

        /// <summary>
        ///  Creates a new <c>BoundStatement</c> from the provided prepared
        ///  statement.
        /// </summary>
        /// <param name="statement"> the prepared statement from which to create a <c>BoundStatement</c>.</param>
        public BoundStatement(PreparedStatement statement)
        {
            _preparedStatement = statement;
            _routingKey = statement.RoutingKey;
            SetConsistencyLevel(statement.ConsistencyLevel);
            if (statement.IsIdempotent != null)
            {
                SetIdempotence(statement.IsIdempotent.Value);
            }
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
            _routingKey = RoutingKey.Compose(routingKeyComponents);
            return this;
        }

        internal override void SetValues(object[] values)
        {
            ValidateValues(values);
            base.SetValues(values);
        }

        /// <summary>
        /// Validate values using prepared statement metadata
        /// </summary>
        private void ValidateValues(object[] values)
        {
            if (values == null)
            {
                return;
            }
            if (PreparedStatement.Metadata == null || PreparedStatement.Metadata.Columns == null || PreparedStatement.Metadata.Columns.Length == 0)
            {
                return;
            }
            var paramsMetadata = PreparedStatement.Metadata.Columns;
            if (values.Length > paramsMetadata.Length)
            {
                throw new ArgumentException(
                    String.Format("Provided {0} parameters to bind, expected {1}", values.Length, paramsMetadata.Length));
            }
            for (var i = 0; i < values.Length; i++)
            {
                var p = paramsMetadata[i];
                var value = values[i];
                if (!TypeCodec.IsAssignableFrom(p, value))
                {
                    throw new InvalidTypeException(
                        String.Format("It is not possible to encode a value of type {0} to a CQL type {1}", value.GetType(), p.TypeCode));
                }
            }
        }

        internal override IQueryRequest CreateBatchRequest(int protocolVersion)
        {
            //Uses the default query options as the individual options of the query will be ignored
            var options = QueryProtocolOptions.CreateFromQuery(this, new QueryOptions());
            return new ExecuteRequest(protocolVersion, PreparedStatement.Id, PreparedStatement.Metadata, IsTracing, options);
        }

        internal void CalculateRoutingKey(bool useNamedParameters, int[] routingIndexes, string[] routingNames, object[] valuesByPosition, object[] rawValues)
        {
            if (_routingKey != null)
            {
                //The routing key was specified by the user
                return;
            }
            if (routingIndexes != null)
            {
                var keys = new RoutingKey[routingIndexes.Length];
                for (var i = 0; i < routingIndexes.Length; i++)
                {
                    var index = routingIndexes[i];
                    var key = TypeCodec.Encode(ProtocolVersion, valuesByPosition[index]);
                    if (key == null)
                    {
                        //The partition key can not be null
                        //Get out and let any node reply a Response Error
                        return;
                    }
                    keys[i] = new RoutingKey(key);
                }
                SetRoutingKey(keys);
                return;
            }
            if (routingNames != null && useNamedParameters)
            {
                var keys = new RoutingKey[routingNames.Length];
                var routingValues = Utils.GetValues(routingNames, rawValues[0]).ToArray();
                if (routingValues.Length != keys.Length)
                {
                    //The routing names are not valid
                    return;
                }
                for (var i = 0; i < routingValues.Length; i++)
                {
                    var key = TypeCodec.Encode(ProtocolVersion, routingValues[i]);
                    if (key == null)
                    {
                        //The partition key can not be null
                        return;
                    }
                    keys[i] = new RoutingKey(key);
                }
                SetRoutingKey(keys);
            }
        }
    }
}
