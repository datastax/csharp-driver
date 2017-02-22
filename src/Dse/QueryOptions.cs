//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra
{
    /// <summary>
    /// Options related to defaults for individual queries.
    /// </summary>
    public class QueryOptions
    {
        /// <summary>
        /// The default consistency level for queries: <c>ConsistencyLevel.LocalOne</c>.
        /// </summary>    
        public const ConsistencyLevel DefaultConsistencyLevel = ConsistencyLevel.LocalOne;

        /// <summary>
        /// The default serial consistency level for conditional updates: <c>ConsistencyLevel.Serial</c>.
        /// </summary>
        public const ConsistencyLevel DefaultSerialConsistencyLevel = ConsistencyLevel.Serial;

        /// <summary>
        /// The default page size for SELECT queries: 5000.
        /// </summary>
        public const int DefaultPageSize = 5000;

        /// <summary>
        /// Default value for <see cref="RetryOnTimeout"/>
        /// </summary>
        public const bool DefaultRetryOnTimeout = true;

        private ConsistencyLevel _consistency = DefaultConsistencyLevel;
        private int _pageSize = DefaultPageSize;
        private ConsistencyLevel _serialConsistency = DefaultSerialConsistencyLevel;
        private bool _retryOnTimeout = DefaultRetryOnTimeout;
        private bool _defaultIdempotence = false;

        /// <summary>
        /// Gets a value that determines if the client should retry when it didn't hear back from a host within <see cref="SocketOptions.ReadTimeoutMillis"/>.
        /// </summary>
        public bool RetryOnTimeout { get { return _retryOnTimeout; }}

        /// <summary>
        /// Sets the default consistency level to use for queries.
        /// 
        /// The consistency level set through this method will be use for queries
        /// that don't explicitely have a consistency level.
        /// </summary>
        /// <param name="consistencyLevel">the new consistency level to set as default.</param>
        /// <returns>this QueryOptions instance</returns>
        public QueryOptions SetConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            _consistency = consistencyLevel;
            return this;
        }


        /// <summary>
        ///  The default consistency level used by queries.
        /// </summary>
        /// <returns>the default consistency level used by queries.</returns>
        public ConsistencyLevel GetConsistencyLevel()
        {
            return _consistency;
        }


        /// <summary>
        /// Sets the default serial consistency level to use for queries.
        /// The serial consistency level set through this method will be use for queries
        /// that don't explicitely have a serial consistency level.
        /// </summary>
        /// <param name="serialConsistencyLevel">the new serial consistency level to set as default.</param>
        /// <returns>this QueryOptions instance.</returns>
        public QueryOptions SetSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel)
        {
            _serialConsistency = serialConsistencyLevel;
            return this;
        }


        /// <summary>
        /// The default serial consistency level used by queries.
        /// </summary>
        /// <returns>the default serial consistency level used by queries.</returns>
        public ConsistencyLevel GetSerialConsistencyLevel()
        {
            return _serialConsistency;
        }


        /// <summary>
        /// Sets the default page size to use for SELECT queries.
        /// The page size set through this method will be use for queries
        /// that don't explicitely have a page size.
        /// </summary>
        /// <param name="pageSize">the new page size to set as default. It must be
        /// strictly positive but you can use int.MaxValue to disable paging.</param>
        /// <returns>this QueryOptions instance</returns>
        public QueryOptions SetPageSize(int pageSize)
        {
            if (pageSize <= 0)
            {
                throw new ArgumentException("Invalid pageSize, should be > 0, got " + pageSize);
            }
            _pageSize = pageSize;
            return this;
        }

        /// <summary>
        /// Determines if the client should retry when it didn't hear back from a host within <see cref="SocketOptions.ReadTimeoutMillis"/>.
        /// </summary>
        public QueryOptions SetRetryOnTimeout(bool retry)
        {
            _retryOnTimeout = retry;
            return this;
        }

        /// <summary>
        /// The default page size used by queries.
        /// </summary>
        /// <returns>the default page size used by queries.</returns> 
        public int GetPageSize()
        {
            return _pageSize;
        }

        /// <summary>
        /// Sets the default idempotence for all queries.
        /// </summary>
        public QueryOptions SetDefaultIdempotence(bool idempotence)
        {
            _defaultIdempotence = idempotence;
            return this;
        }

        public bool GetDefaultIdempotence()
        {
            //A get method is not very C#-like, a get property should be more appropriate
            //But it's to be consistent with the rest of the class
            return _defaultIdempotence;
        }
    }
}
