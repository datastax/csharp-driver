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

namespace Cassandra
{
    /// <summary>
    /// Base class for statements that contains the options.
    /// </summary>
    public abstract class Statement : IStatement
    {
        private ConsistencyLevel? _consistency;
        private int _pageSize;
        private byte[] _pagingState;
        private IRetryPolicy _retryPolicy;
        private ConsistencyLevel _serialConsistency = QueryProtocolOptions.Default.SerialConsistency;
        private bool _skipMetadata;
        private bool _traceQuery;
        private object[] _values;
        private DateTimeOffset? _timestamp;
        private bool _autoPage = true;

        public virtual object[] QueryValues
        {
            get { return _values; }
        }
        /// <inheritdoc />
        public bool SkipMetadata
        {
            get { return _skipMetadata; }
        }
        /// <inheritdoc />
        public ConsistencyLevel? ConsistencyLevel
        {
            get { return _consistency; }
        }

        /// <summary>
        /// Gets the serial consistency level for this query.
        /// </summary>        
        public ConsistencyLevel SerialConsistencyLevel
        {
            get { return _serialConsistency; }
        }

        /// <inheritdoc />
        public int PageSize
        {
            get { return _pageSize; }
        }

        /// <inheritdoc />
        public bool IsTracing
        {
            get { return _traceQuery; }
        }

        /// <inheritdoc />
        public IRetryPolicy RetryPolicy
        {
            get { return _retryPolicy; }
        }
        /// <inheritdoc />
        public byte[] PagingState
        {
            get { return _pagingState; }
        }

        /// <inheritdoc />
        public DateTimeOffset? Timestamp
        {
            get { return _timestamp; }
        }

        /// <inheritdoc />
        public bool AutoPage
        {
            get { return _autoPage; }
        }

        /// <inheritdoc />
        public abstract RoutingKey RoutingKey { get; }

        /// <summary>
        /// Gets or sets the protocol version used for Routing Key parts encoding
        /// </summary>
        internal int ProtocolVersion { get; set; }

        protected Statement()
        {
            ProtocolVersion = 1;
        }

        /// <inheritdoc />
        protected Statement(QueryProtocolOptions queryProtocolOptions)
        {
        }

        /// <inheritdoc />
        internal Statement SetSkipMetadata(bool val)
        {
            _skipMetadata = val;
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
        /// <returns>this bound statement. </returns>
        internal virtual void SetValues(object[] values)
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
            _pagingState = pagingState;
            //Disable automatic paging only if paging state is set to something other then null
            if (pagingState != null && pagingState.Length > 0)
            {
                return SetAutoPage(false);
            }
            return this;
        }

        /// <inheritdoc />
        public IStatement SetConsistencyLevel(ConsistencyLevel? consistency)
        {
            _consistency = consistency;
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
            _timestamp = value;
            return this;
        }

        /// <inheritdoc />
        public IStatement EnableTracing(bool enable = true)
        {
            _traceQuery = enable;
            return this;
        }

        /// <inheritdoc />
        public IStatement DisableTracing()
        {
            _traceQuery = false;
            return this;
        }

        /// <inheritdoc />
        public IStatement SetRetryPolicy(IRetryPolicy policy)
        {
            _retryPolicy = policy;
            return this;
        }

        internal virtual IQueryRequest CreateBatchRequest(int protocolVersion)
        {
            throw new InvalidOperationException("Cannot insert this query into the batch");
        }

        /// <inheritdoc />
        public IStatement SetPageSize(int pageSize)
        {
            _pageSize = pageSize;
            return this;
        }
    }
}
