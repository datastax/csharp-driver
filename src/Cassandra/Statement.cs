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
using System.Text;
using Cassandra.Requests;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// Base class for statements that contains the options.
    /// </summary>
    public abstract class Statement : IStatement
    {
        protected const string ProxyExecuteKey = "ProxyExecute";
        private ConsistencyLevel _serialConsistency = QueryProtocolOptions.Default.SerialConsistency;
        private object[] _values;
        private bool _autoPage = true;
        private volatile int _isIdempotent = int.MinValue;
        private volatile Host _host;
        private string _authorizationId;
        private IDictionary<string, byte[]> _outgoingPayload;

        public virtual object[] QueryValues
        {
            get { return _values; }
        }
        /// <inheritdoc />
        public bool SkipMetadata { get; private set; }

        /// <inheritdoc />
        public ConsistencyLevel? ConsistencyLevel { get; private set; }

        /// <summary>
        /// Gets the serial consistency level for this query.
        /// </summary>        
        public ConsistencyLevel SerialConsistencyLevel
        {
            get { return _serialConsistency; }
        }

        /// <inheritdoc />
        public int PageSize { get; private set; }

        /// <inheritdoc />
        public bool IsTracing { get; private set; }

        /// <inheritdoc />
        public int ReadTimeoutMillis { get; private set; }

        /// <inheritdoc />
        public IRetryPolicy RetryPolicy { get; private set; }

        /// <inheritdoc />
        public byte[] PagingState { get; private set; }

        /// <inheritdoc />
        public DateTimeOffset? Timestamp { get; private set; }

        /// <inheritdoc />
        public bool AutoPage
        {
            get { return _autoPage; }
        }

        /// <inheritdoc />
        public IDictionary<string, byte[]> OutgoingPayload
        {
            get { return _outgoingPayload; }
            private set { RebuildOutgoingPayload(value); }
        }

        /// <inheritdoc />
        public abstract RoutingKey RoutingKey { get; }

        /// <inheritdoc />
        public bool? IsIdempotent
        {
            get
            {
                var idempotence = _isIdempotent;
                if (idempotence == int.MinValue)
                {
                    return null;
                }
                return idempotence == 1;
            }
        }

        /// <inheritdoc />
        public virtual string Keyspace
        {
            get { return null; }
        }

        /// <summary>
        /// Gets the host configured on this <see cref="Statement"/>, or <c>null</c> if none is configured.
        /// <para>
        /// In the general case, the host used to execute this <see cref="Statement"/> will depend on the configured
        /// <see cref="ILoadBalancingPolicy"/> and this property will return <c>null</c>.
        /// </para>
        /// <seealso cref="SetHost"/>
        /// </summary>
        public Host Host => _host;

        protected Statement()
        {

        }

        // ReSharper disable once UnusedParameter.Local
        protected Statement(QueryProtocolOptions queryProtocolOptions)
        {
            //the unused parameter is maintained for backward compatibility
        }

        /// <inheritdoc />
        public IStatement ExecutingAs(string userOrRole)
        {
            _authorizationId = userOrRole;
            RebuildOutgoingPayload(_outgoingPayload);
            return this;
        }

        private void RebuildOutgoingPayload(IDictionary<string, byte[]> payload)
        {
            if (_authorizationId == null)
            {
                _outgoingPayload = payload;
                return;
            }
            IDictionary<string, byte[]> builder;
            if (payload != null)
            {
                builder = new Dictionary<string, byte[]>(payload);
            }
            else
            {
                builder = new Dictionary<string, byte[]>(1);
            }
            builder[ProxyExecuteKey] = Encoding.UTF8.GetBytes(_authorizationId);
            _outgoingPayload = builder;
        }

        /// <inheritdoc />
        internal Statement SetSkipMetadata(bool val)
        {
            SkipMetadata = val;
            return this;
        }

        /// <summary>
        ///  Bound values to the variables of this statement. This method provides a
        ///  convenience to bound all the variables of the <c>BoundStatement</c> in
        ///  one call.
        /// </summary>
        /// <param name="values"> the values to bind to the variables of the newly
        ///  created BoundStatement. The first element of <c>values</c> will 
        ///  be bound to the first bind variable,
        ///  etc.. It is legal to provide less values than the statement has bound
        ///  variables. In that case, the remaining variable need to be bound before
        ///  execution. If more values than variables are provided however, an
        ///  IllegalArgumentException will be raised. </param>
        /// <param name="serializer">Current serializer.</param>
        /// <returns>this bound statement. </returns>
        internal virtual void SetValues(object[] values, ISerializer serializer)
        {
            _values = values;
        }

        /// <inheritdoc />
        public IStatement SetAutoPage(bool autoPage)
        {
            _autoPage = autoPage;
            return this;
        }

        /// <inheritdoc />
        public IStatement SetPagingState(byte[] pagingState)
        {
            PagingState = pagingState;
            //Disable automatic paging only if paging state is set to something other then null
            if (pagingState != null && pagingState.Length > 0)
            {
                return SetAutoPage(false);
            }
            return this;
        }

        /// <inheritdoc />
        public IStatement SetReadTimeoutMillis(int timeout)
        {
            ReadTimeoutMillis = timeout;
            return this;
        }

        /// <inheritdoc />
        public IStatement SetConsistencyLevel(ConsistencyLevel? consistency)
        {
            ConsistencyLevel = consistency;
            return this;
        }

        /// <inheritdoc />
        public IStatement SetSerialConsistencyLevel(ConsistencyLevel serialConsistency)
        {
            if (serialConsistency.IsSerialConsistencyLevel() == false)
            {
                throw new ArgumentException("The serial consistency can only be set to ConsistencyLevel.LocalSerial or ConsistencyLevel.Serial.");
            }
            _serialConsistency = serialConsistency;
            return this;
        }

        /// <inheritdoc />
        public IStatement SetTimestamp(DateTimeOffset value)
        {
            Timestamp = value;
            return this;
        }

        /// <inheritdoc />
        public IStatement EnableTracing(bool enable = true)
        {
            IsTracing = enable;
            return this;
        }

        /// <inheritdoc />
        public IStatement DisableTracing()
        {
            IsTracing = false;
            return this;
        }

        /// <inheritdoc />
        public IStatement SetRetryPolicy(IRetryPolicy policy)
        {
            RetryPolicy = policy;
            return this;
        }

        internal virtual IQueryRequest CreateBatchRequest(ISerializer serializer)
        {
            throw new InvalidOperationException("Cannot insert this query into the batch");
        }
        
        /// <inheritdoc />
        public IStatement SetIdempotence(bool value)
        {
            _isIdempotent = value ? 1 : 0;
            return this;
        }

        /// <inheritdoc />
        public IStatement SetPageSize(int pageSize)
        {
            PageSize = pageSize;
            return this;
        }

        /// <inheritdoc />
        public IStatement SetOutgoingPayload(IDictionary<string, byte[]> payload)
        {
            OutgoingPayload = payload;
            return this;
        }

        /// <summary>
        /// Sets the <see cref="Host"/> that should handle this query.
        /// <para>
        /// In the general case, use of this method is <em>heavily discouraged</em> and should only be
        /// used in the following cases:
        /// </para>
        /// <list type="bullet">
        /// <item><description>Querying node-local tables, such as tables in the <c>system</c> and <c>system_views</c>
        /// keyspaces.</description></item>
        /// <item><description>Applying a series of schema changes, where it may be advantageous to execute schema
        /// changes in sequence on the same node.</description></item>
        /// </list>
        /// <para>Configuring a specific host causes the configured <see cref="ILoadBalancingPolicy"/> to be
        /// completely bypassed. However, if the load balancing policy dictates that the host is at
        /// distance <see cref="HostDistance.Ignored"/> or there is no active connectivity to the host, the
        /// request will fail with a <see cref="NoHostAvailableException"/>.</para>
        /// </summary>
        /// <param name="host">The host that should be used to handle executions of this statement or null to
        /// delegate to the configured load balancing policy.</param>
        /// <returns>this instance</returns>
        public IStatement SetHost(Host host)
        {
            _host = host;
            return this;
        }
    }
}
