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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Metrics.Internal;
using Cassandra.Tasks;

// ReSharper disable DoNotCallOverridableMethodsInConstructor
// ReSharper disable CheckNamespace

namespace Cassandra
{
    /// <summary>
    /// Represents the result of a query returned by the server.
    /// <para>
    /// The retrieval of the rows of a <see cref="RowSet"/> is generally paged (a first page
    /// of result is fetched and the next one is only fetched once all the results
    /// of the first page have been consumed). The size of the pages can be configured
    /// either globally through <see cref="QueryOptions.SetPageSize(int)"/> or per-statement
    /// with <see cref="IStatement.SetPageSize(int)"/>. Though new pages are automatically
    /// and transparently fetched when needed, it is possible to force the retrieval
    /// of the next page early through <see cref="FetchMoreResults"/> and  <see cref="FetchMoreResultsAsync"/>.
    /// </para>
    /// <para>
    /// The RowSet dequeues <see cref="Row"/> items while iterated. After a full enumeration of this instance, following
    /// enumerations will be empty, as all rows have been dequeued.
    /// </para>
    /// </summary>
    /// <remarks>
    /// RowSet paging is not available with the version 1 of the native protocol.
    /// If the protocol version 1 is in use, a RowSet is always fetched in it's entirely and
    /// it's up to the client to make sure that no query can yield ResultSet that won't hold
    /// in memory.
    /// </remarks>
    /// <remarks>Parallel enumerations are supported and thread-safe.</remarks>
    public class RowSet : IEnumerable<Row>, IDisposable
    {
        private static readonly CqlColumn[] EmptyColumns = new CqlColumn[0];
        private volatile Func<byte[], Task<RowSet>> _fetchNextPage;
        private volatile byte[] _pagingState;
        private int _isPaging;
        private volatile Task _currentFetchNextPageTask;
        private volatile int _pageSyncAbortTimeout = Timeout.Infinite;
        private volatile bool _autoPage;
        private volatile IMetricsManager _metricsManager;

        /// <summary>
        /// Determines if when dequeuing, it will automatically fetch the following result pages.
        /// </summary>
        protected internal bool AutoPage
        {
            get => _autoPage;
            set => _autoPage = value;
        }

        /// <summary>
        /// Sets the method that is called to get the next page.
        /// </summary>
        internal void SetFetchNextPageHandler(Func<byte[], Task<RowSet>> handler, int pageSyncAbortTimeout, IMetricsManager metricsManager)
        {
            if (_fetchNextPage != null)
            {
                throw new InvalidOperationException("Multiple sets to FetchNextPage handler not supported");
            }
            _fetchNextPage = handler;
            _pageSyncAbortTimeout = pageSyncAbortTimeout;
            _metricsManager = metricsManager;
        }

        /// <summary>
        /// Gets or set the internal row list. It contains the rows of the latest query page.
        /// </summary>
        protected virtual ConcurrentQueue<Row> RowQueue { get; set; }

        /// <summary>
        /// Gets the amount of items in the internal queue. For testing purposes.
        /// </summary>
        internal int InnerQueueCount => RowQueue.Count;

        /// <summary>
        /// Gets the execution info of the query
        /// </summary>
        public virtual ExecutionInfo Info { get; set; }

        /// <summary>
        /// Gets or sets the columns in the RowSet
        /// </summary>
        public virtual CqlColumn[] Columns { get; set; }

        /// <summary>
        /// Gets or sets the paging state of the query for the RowSet.
        /// When set it states that there are more pages.
        /// </summary>
        public virtual byte[] PagingState
        {
            get => _pagingState;
            protected internal set => _pagingState = value;
        }

        /// <summary>
        /// Returns whether this ResultSet has more results.
        /// It has side-effects, if the internal queue has been consumed it will page for more results.
        /// </summary>
        /// <seealso cref="IsFullyFetched"/>
        public virtual bool IsExhausted()
        {
            if (RowQueue == null)
            {
                return true;
            }
            if (!RowQueue.IsEmpty)
            {
                return false;
            }
            PageNext();
            return RowQueue.IsEmpty;
        }

        /// <summary>
        /// Whether all results from this result set has been fetched from the database.
        /// </summary>
        public virtual bool IsFullyFetched => PagingState == null || !AutoPage;

        /// <summary>
        /// Creates a new instance of RowSet.
        /// </summary>
        public RowSet() : this(false)
        {

        }

        /// <summary>
        /// Creates a new instance of RowSet.
        /// </summary>
        /// <param name="isVoid">Determines if the RowSet instance is created for a VOID result</param>
        private RowSet(bool isVoid)
        {
            if (!isVoid)
            {
                RowQueue = new ConcurrentQueue<Row>();
            }
            Info = new ExecutionInfo();
            Columns = EmptyColumns;
            AutoPage = true;
        }

        /// <summary>
        /// Returns a new RowSet instance without any columns or rows, designed for VOID results.
        /// </summary>
        internal static RowSet Empty()
        {
            return new RowSet(true);
        }

        /// <summary>
        /// Adds a row to the inner row list
        /// </summary>
        internal virtual void AddRow(Row row)
        {
            if (RowQueue == null)
            {
                throw new InvalidOperationException("Can not append a Row to a RowSet instance created for VOID results");
            }
            RowQueue.Enqueue(row);
        }

        /// <summary>
        /// Forces the fetching the next page of results for this <see cref="RowSet"/>.
        /// </summary>
        public void FetchMoreResults()
        {
            PageNext();
        }

        /// <summary>
        /// Asynchronously retrieves the next page of results for this <see cref="RowSet"/>.
        /// <para>
        /// The Task will be completed once the internal queue is filled with the new <see cref="Row"/>
        /// instances.
        /// </para>
        /// </summary>
        public async Task FetchMoreResultsAsync()
        {
            var pagingState = _pagingState;
            if (pagingState == null || !AutoPage)
            {
                return;
            }

            // Only one concurrent call to page
            if (Interlocked.CompareExchange(ref _isPaging, 1, 0) != 0)
            {
                // Once isPaging flag is set, task will be set shortly
                Task task;
                var spin = new SpinWait();
                while ((task = _currentFetchNextPageTask) == null)
                {
                    // Use busy spin as the task should be set immediately after
                    // There is no risk on task being null after that
                    spin.SpinOnce();
                }

                // In a race, the task might be old and completed but that's OK as GetEnumerator()
                // checks the pagingState in a loop.
                await task.ConfigureAwait(false);
                return;
            }

            pagingState = _pagingState;
            if (pagingState == null)
            {
                // It finished paging
                Interlocked.Exchange(ref _isPaging, 0);
                return;
            }

            var tcs = new TaskCompletionSource<bool>();
            // Set the task field as soon as possible
            _currentFetchNextPageTask = tcs.Task;

            var fetchMethod = _fetchNextPage ??
                              throw new DriverInternalError("Paging state set but delegate to retrieve is not");

            try
            {
                var rs = await fetchMethod(pagingState).ConfigureAwait(false);
                foreach (var newRow in rs.RowQueue)
                {
                    RowQueue.Enqueue(newRow);
                }

                // PagingState must be set AFTER all rows have been enqueued
                PagingState = rs.PagingState;
            }
            catch (Exception ex)
            {
                tcs.SetException(ex);
                throw;
            }
            finally
            {
                // Set task BEFORE allowing other threads to page.
                tcs.TrySetResult(true);
                Interlocked.Exchange(ref _isPaging, 0);
            }
        }

        /// <summary>
        /// The number of rows available in this row set that can be retrieved without blocking to fetch.
        /// </summary>
        public int GetAvailableWithoutFetching()
        {
            return RowQueue?.Count ?? 0;
        }

        /// <summary>
        /// For backward compatibility: It is possible to iterate using the RowSet as it is enumerable.
        /// <para>Obsolete: Note that it will be removed in future versions</para>
        /// </summary>
        public IEnumerable<Row> GetRows()
        {
            //legacy: Keep the GetRows method for Compatibility.
            return this;
        }

        /// <inheritdoc />
        public virtual IEnumerator<Row> GetEnumerator()
        {
            if (RowQueue == null)
            {
                yield break;
            }

            var hasMoreData = true;
            while (hasMoreData)
            {
                while (RowQueue.TryDequeue(out var row))
                {
                    yield return row;
                }
                hasMoreData = AutoPage && _pagingState != null;
                PageNext();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Gets the next results and add the rows to the current <see cref="RowSet"/> queue.
        /// </summary>
        protected virtual void PageNext()
        {
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, FetchMoreResultsAsync(), _pageSyncAbortTimeout);
        }

        /// <summary>
        /// For backward compatibility only
        /// </summary>
        [Obsolete("Explicitly releasing the RowSet resources is not required. It will be removed in future versions.", false)]
        public void Dispose()
        {

        }
    }
}
