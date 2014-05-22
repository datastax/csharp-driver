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
//

using System;
using System.Linq;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents a result of a query returned by Cassandra.
    /// </summary>
    public class RowSet : IEnumerable<Row>, IDisposable
    {
        private object _pageLock = new object();
        /// <summary>
        /// Contains the PagingState keys of the pages already retrieved.
        /// </summary>
        protected ConcurrentDictionary<byte[], bool> _pagers = new ConcurrentDictionary<byte[], bool>();

        /// <summary>
        /// Delegate that is called to get the next page.
        /// </summary>
        protected internal Func<byte[], RowSet> FetchNextPage { get; set; }

        /// <summary>
        /// Gets or set the internal row list. It contains the rows of the latest query page.
        /// </summary>
        protected virtual ConcurrentQueue<Row> RowQueue { get; set; }

        /// <summary>
        /// Gets the amount of items in the internal queue. For testing purposes.
        /// </summary>
        internal int InnerQueueCount { get { return RowQueue.Count; } }

        /// <summary>
        /// Gets the execution info of the query
        /// </summary>
        public virtual ExecutionInfo Info { get; set; }

        /// <summary>
        /// Gets or sets the columns in the rowset
        /// </summary>
        public virtual CqlColumn[] Columns { get; set; }

        /// <summary>
        /// Gets or sets the paging state of the query for the rowset.
        /// When set it states that there are more pages.
        /// </summary>
        public virtual byte[] PagingState { get; set; }

        /// <summary>
        /// Returns whether this ResultSet has more results.
        /// It has side-effects, if the internal queue has been consumed it will page for more results.
        /// </summary>
        public virtual bool IsExhausted()
        {
            if (RowQueue.Count > 0)
            {
                return false;
            }
            PageNext();

            return RowQueue.Count == 0;
        }

        /// <summary>
        /// Whether all results from this result set has been fetched from the database.
        /// </summary>
        public virtual bool IsFullyFetched 
        { 
            get
            {
                return this.PagingState == null;
            } 
        }

        public RowSet()
        {
            RowQueue = new ConcurrentQueue<Row>();
            Info = new ExecutionInfo();
            Columns = new CqlColumn[] { };
        }

        /// <summary>
        /// Adds a row to the inner row list
        /// </summary>
        internal virtual void AddRow(Row row)
        {
            RowQueue.Enqueue(row);
        }

        /// <summary>
        /// Force the fetching the next page of results for this result set, if any.
        /// </summary>
        public void FetchMoreResults()
        {
            PageNext();
        }

        /// <summary>
        /// Force the fetching the next page of results without blocking for this result set, if any.
        /// </summary>
        public Task FetchMoreResultsAsync()
        {
            return Task.Factory.StartNew(() => FetchMoreResults());
        }

        /// <summary>
        /// The number of rows available in this row set that can be retrieved without blocking to fetch.
        /// </summary>
        public int GetAvailableWithoutFetching()
        {
            return RowQueue.Count;
        }

        /// <summary>
        /// For backward compatibility: It is possible to iterate using the RowSet as it is enumerable.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Row> GetRows()
        {
            //legacy: Keep the GetRows method for Compatibity.
            return this;
        }

        public IEnumerator<Row> GetEnumerator()
        {
            while (!IsExhausted())
            {
                Row row = null;
                while (RowQueue.TryDequeue(out row))
                {
                    yield return row;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        /// Gets the next results and add the rows to the current rowset queue
        /// </summary>
        protected virtual void PageNext()
        {
            if (IsFullyFetched)
            {
                return;
            }
            if (FetchNextPage == null)
            {
                //There is no handler, clear the paging state
                this.PagingState = null;
                return;
            }
            lock (_pageLock)
            {
                var pageState = this.PagingState;
                if (pageState == null)
                {
                    return;
                }
                bool value;
                bool alreadyPresent = _pagers.TryGetValue(pageState, out value);
                if (!alreadyPresent)
                {
                    var rs = FetchNextPage(pageState);
                    foreach (var newRow in rs.RowQueue)
                    {
                        this.RowQueue.Enqueue(newRow);
                    }
                    this.PagingState = rs.PagingState;
                    _pagers.AddOrUpdate(pageState, true, (k, v) => v);
                }
            }
        }

        /// <summary>
        /// For backward compatibity only
        /// </summary>
        [Obsolete("Explicitly releasing the RowSet resources is not required. It will be removed in future versions.", false)]
        public void Dispose()
        {

        }
    }
}