//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Requests;
using Dse.Serialization;

namespace Dse
{
    /// <summary>
    /// A statement that groups a number of <see cref="BoundStatement" /> and / or <see cref="SimpleStatement" /> so they get executed as a batch.
    /// </summary>
    public class BatchStatement : Statement
    {
        private static readonly Logger Logger = new Logger(typeof(BatchStatement));
        private readonly List<Statement> _queries = new List<Statement>();
        private BatchType _batchType = BatchType.Logged;
        private RoutingKey _routingKey;
        private object[] _routingValues;

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
                //Calculate the routing key based on Routing values
                var serializer = Serializer;
                if (serializer == null)
                {
                    serializer = Serializer.Default;
                    Logger.Warning("Calculating routing key before executing is not supporting for BatchStatements, " +
                                   "using default serializer.");
                }
                return RoutingKey.Compose(
                    _routingValues
                    .Select(value => new RoutingKey(serializer.Serialize(value)))
                    .ToArray());
            }
        }

        internal Serializer Serializer { get; set; }

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
        /// Sets the partition key values in order to route the query to the correct replicas.
        /// <para>For simple partition keys, set the partition key value.</para>
        /// <para>For composite partition keys, set the multiple the partition key values in correct order.</para>
        /// </summary>
        public BatchStatement SetRoutingValues(params object[] keys)
        {
            _routingValues = keys;
            return this;
        }

        /// <summary>
        /// Adds a new statement to this batch.
        /// Note that statement can be any <c>Statement</c>. It is allowed to mix <see cref="SimpleStatement"/> and <see cref="BoundStatement"/> in the same <c>BatchStatement</c> in particular.
        /// Please note that the options of the added <c>Statement</c> (all those defined directly by the Statement class: consistency level, fetch size, tracing, ...) will be ignored for the purpose of the execution of the Batch. Instead, the options used are the one of this <c>BatchStatement</c> object.
        /// </summary>
        /// <param name="statement">Statement to add to the batch</param>
        /// <returns>The Batch statement</returns>
        /// <exception cref="System.ArgumentOutOfRangeException">Thrown when trying to add more than <c>short.MaxValue</c> Statements</exception>
        public BatchStatement Add(Statement statement)
        {
            if (_queries.Count > short.MaxValue)
            {
                throw new ArgumentOutOfRangeException(
                    string.Format("Batch statement cannot contain more than {0} statements", short.MaxValue));
            }
            if (statement.OutgoingPayload != null && statement.OutgoingPayload.ContainsKey(ProxyExecuteKey))
            {
                throw new ArgumentException("Batch statement cannot contain statements with proxy execution." +
                                            "Use ExecuteAs(...) on the batch statement instead");
            }
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

        internal override IQueryRequest CreateBatchRequest(ProtocolVersion protocolVersion)
        {
            throw new InvalidOperationException("Batches cannot be included recursively");
        }
    }
}
