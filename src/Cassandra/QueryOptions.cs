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

namespace Cassandra
{
    /// <summary>
    /// Options related to defaults for individual queries.
    /// </summary>
    public class QueryOptions
    {
        /// <summary>
        /// Represents the 
        /// </summary>
        internal static readonly QueryOptions Empty = new QueryOptions();

        /// <summary>
        /// The default consistency level for queries: <c>ConsistencyLevel.LocalOne</c>.
        /// For DataStax Astra, this constant should be ignored as the default is LocalQuorum.
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

        private ConsistencyLevel? _consistency;
        private ConsistencyLevel _defaultConsistencyLevel = QueryOptions.DefaultConsistencyLevel;
        private int _pageSize = DefaultPageSize;
        private ConsistencyLevel _serialConsistency = QueryOptions.DefaultSerialConsistencyLevel;
        private bool _retryOnTimeout = DefaultRetryOnTimeout;
        private bool _defaultIdempotence = false;
        private bool _prepareOnAllHosts = true;
        private bool _reprepareOnUp = true;

        /// <summary>
        /// Gets a value that determines if the client should retry when it didn't hear back from a host within <see cref="SocketOptions.ReadTimeoutMillis"/>.
        /// <para>
        /// DEPRECATED: Instead, use <see cref="IExtendedRetryPolicy.OnRequestError"/> to control the behavior when 
        /// <see cref="OperationTimedOutException"/> is obtained.
        /// </para>
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

        internal void SetDefaultConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            _defaultConsistencyLevel = consistencyLevel;
        }

        /// <summary>
        ///  The default consistency level used by queries.
        /// </summary>
        /// <returns>the default consistency level used by queries.</returns>
        public ConsistencyLevel GetConsistencyLevel()
        {
            return _consistency ?? _defaultConsistencyLevel;
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
            if (!serialConsistencyLevel.IsSerialConsistencyLevel())
            {
                throw new ArgumentException("Serial consistency level can only be set to LocalSerial or Serial");
            }
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
        /// <para>
        /// DEPRECATED: Instead, use <see cref="IExtendedRetryPolicy.OnRequestError"/> to control the behavior when 
        /// <see cref="OperationTimedOutException"/> is obtained.
        /// </para>
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

        /// <summary>
        /// Gets the default idempotence for all queries.
        /// </summary>
        public bool GetDefaultIdempotence()
        {
            //A get method is not very C#-like, a get property should be more appropriate
            //But it's to be consistent with the rest of the class
            return _defaultIdempotence;
        }

        /// <summary>
        /// Determines whether the driver should prepare statements on all hosts in the cluster.
        /// </summary>
        public bool IsPrepareOnAllHosts()
        {
            return _prepareOnAllHosts;
        }

        /// <summary>
        /// Sets whether the driver should prepare statements on all hosts in the cluster.
        /// <para>
        /// A statement is normally prepared in two steps: prepare the query on a single host in the cluster; 
        /// if that succeeds, prepare on all other hosts.
        /// </para>
        /// <para>
        /// This option controls whether step 2 is executed. It is enabled by default.
        /// </para>
        /// </summary>
        /// <remarks>
        /// <para>
        /// The reason why you might want to disable it is to optimize network usage if you have a large 
        /// number of clients preparing the same set of statements at startup. If your load balancing policy
        /// distributes queries randomly, each client will pick a different host to prepare its statements, 
        /// and on the whole each host has a good chance of having been hit by at least one client for each statement.
        /// </para>
        /// <para>
        /// On the other hand, if that assumption turns out to be wrong and one host hasn't prepared a given
        /// statement, it needs to be re-prepared on the fly the first time it gets executed; this causes a 
        /// performance penalty (one extra roundtrip to resend the query to prepare, and another to retry
        /// the execution).
        /// </para>
        /// </remarks>
        public QueryOptions SetPrepareOnAllHosts(bool prepareOnAllHosts)
        {
            _prepareOnAllHosts = prepareOnAllHosts;
            return this;
        }

        /// <summary>
        /// Determines whether the driver should re-prepare all cached prepared statements on a host when its marks
        /// that host back up.
        /// </summary>
        public bool IsReprepareOnUp()
        {
            return _reprepareOnUp;
        }

        /// <summary>
        /// Set whether the driver should re-prepare all cached prepared statements on a host when it marks it back up.
        /// <para>This option is enabled by default.</para>
        /// </summary>
        /// <remarks>
        /// <para>
        /// The reason why you might want to disable it is to optimize reconnection time when you believe hosts 
        /// often get marked down because of temporary network issues, rather than the host really crashing. 
        /// In that case, the host still has prepared statements in its cache when the driver reconnects, 
        /// so re-preparing is redundant.
        /// </para>
        /// <para>
        /// On the other hand, if that assumption turns out to be wrong and the host had really restarted, 
        /// its prepared statement cache is empty, and statements need to be re-prepared on the fly the 
        /// first time they get executed; this causes a performance penalty (one extra roundtrip to resend 
        /// the query to prepare, and another to retry the execution).
        /// </para>
        /// </remarks>
        public QueryOptions SetReprepareOnUp(bool reprepareOnUp)
        {
            _reprepareOnUp = reprepareOnUp;
            return this;
        }
    }
}
