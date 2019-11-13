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

namespace Cassandra.Mapping
{
    /// <summary>
    /// Represents options available on a per-query basis.
    /// </summary>
    public class CqlQueryOptions
    {
        /// <summary>
        /// An empty instance of CqlQueryOptions (i.e. no options are set).
        /// </summary>
        internal static readonly CqlQueryOptions None = new EmptyQueryOptions();

        private ConsistencyLevel? _consistencyLevel;
        private bool? _tracingEnabled;
        private int? _pageSize;
        private IRetryPolicy _retryPolicy;
        private ConsistencyLevel? _serialConsistencyLevel;
        private byte[] _pagingState;
        private DateTimeOffset? _timestamp;

        private bool _noPrepare;

        /// <summary>
        /// Whether or not to use a PreparedStatement when executing the query.
        /// </summary>
        internal bool NoPrepare
        {
            get { return _noPrepare; }
        }

        /// <summary>
        /// Sets the consistency level to be used when executing the query.
        /// </summary>
        public CqlQueryOptions SetConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }

        /// <summary>
        /// Enables tracing for the query.
        /// </summary>
        public CqlQueryOptions EnableTracing()
        {
            _tracingEnabled = true;
            return this;
        }

        /// <summary>
        /// Disables tracing for the query.
        /// </summary>
        public CqlQueryOptions DisableTracing()
        {
            _tracingEnabled = false;
            return this;
        }

        /// <summary>
        /// Sets the page size for automatic paging for the query.
        /// </summary>
        public CqlQueryOptions SetPageSize(int pageSize)
        {
            _pageSize = pageSize;
            return this;
        }

        /// <summary>
        /// Sets the token representing the page state for the query.
        /// Use <c>null</c> to get the first page of results.
        /// </summary>
        public CqlQueryOptions SetPagingState(byte[] pagingState)
        {
            _pagingState = pagingState;
            return this;
        }

        /// <summary>
        /// Sets the retry policy for the query.
        /// </summary>
        public CqlQueryOptions SetRetryPolicy(IRetryPolicy retryPolicy)
        {
            _retryPolicy = retryPolicy;
            return this;
        }

        /// <summary>
        /// Sets the serial consistency level for execution of the query.  (NOTE: This only applies to queries using lightweight
        /// transactions -- LWT).
        /// </summary>
        public CqlQueryOptions SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            _serialConsistencyLevel = consistencyLevel;
            return this;
        }

        /// <summary>
        /// Specifies that a PreparedStatement should not be used for executing the query.
        /// </summary>
        public CqlQueryOptions DoNotPrepare()
        {
            _noPrepare = true;
            return this;
        }

        /// <summary>
        /// Sets the timestamp for the query.
        /// </summary>
        public CqlQueryOptions SetTimestamp(DateTimeOffset? timestamp)
        {
            _timestamp = timestamp;
            return this;
        }

        /// <summary>
        /// Copies any options set on this Cql instance to the statement provided.
        /// </summary>
        internal virtual void CopyOptionsToStatement(IStatement statement)
        {
            if (_consistencyLevel.HasValue)
            {
                statement.SetConsistencyLevel(_consistencyLevel.Value);
            }
            if (_tracingEnabled.HasValue)
            {
                statement.EnableTracing(_tracingEnabled.Value);
            }
            if (_pageSize.HasValue)
            {
                statement.SetPageSize(_pageSize.Value);
            }
            statement.SetPagingState(_pagingState);
            if (_retryPolicy != null)
            {
                statement.SetRetryPolicy(_retryPolicy);
            }
            if (_serialConsistencyLevel.HasValue)
            {
                statement.SetSerialConsistencyLevel(_serialConsistencyLevel.Value);
            }
            if (_timestamp.HasValue)
            {
                statement.SetTimestamp(_timestamp.Value);
            }
        }

        /// <summary>
        /// Creates a new instance of CqlQueryOptions.
        /// </summary>
        public static CqlQueryOptions New()
        {
            return new CqlQueryOptions();
        }

        /// <summary>
        /// Represents no query options.  Copying options to a statement is a no-op.
        /// </summary>
        private class EmptyQueryOptions : CqlQueryOptions
        {
            internal override void CopyOptionsToStatement(IStatement statement)
            {
                // No op
            }
        }
    }
}