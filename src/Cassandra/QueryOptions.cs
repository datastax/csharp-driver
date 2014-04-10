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

using System;

namespace Cassandra
{
    /// <summary>
    /// Options related to defaults for individual queries.
    /// </summary>
    public class QueryOptions
    {
        /// <summary>
        /// The default consistency level for queries: ConsistencyLevel.One.
        /// </summary>    
        public const ConsistencyLevel DefaultConsistencyLevel = ConsistencyLevel.One;

        /// <summary>
        /// The default serial consistency level for conditional updates: {@code ConsistencyLevel.Serial}.
        /// </summary>
        public const ConsistencyLevel DefaultSerialConsistencyLevel = ConsistencyLevel.Serial;

        /// <summary>
        /// The default page size for SELECT queries: 5000.
        /// </summary>
        public const int DefaultPageSize = 5000;

        private volatile ConsistencyLevel _consistency = DefaultConsistencyLevel;
        private volatile int _pageSize = DefaultPageSize;
        private volatile ConsistencyLevel _serialConsistency = DefaultSerialConsistencyLevel;


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
                throw new ArgumentException("Invalid pageSize, should be > 0, got " + pageSize);
            this._pageSize = pageSize;
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
    }
}