//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Linq;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
// ReSharper disable DoNotCallOverridableMethodsInConstructor
// ReSharper disable CheckNamespace

namespace Cassandra
{
    /// <summary>
    /// Represents a result of a query returned by Cassandra.
    /// <para>
    /// The retrieval of the rows of a RowSet is generally paged (a first page
    /// of result is fetched and the next one is only fetched once all the results
    /// of the first one has been consumed). The size of the pages can be configured
    /// either globally through <see cref="QueryOptions.SetPageSize(int)"/> or per-statement
    /// with <see cref="IStatement.SetPageSize(int)"/>. Though new pages are automatically
    /// (and transparently) fetched when needed, it is possible to force the retrieval
    /// of the next page early through <see cref="FetchMoreResults()"/>.
    /// </para>
    /// <para>
    /// The RowSet dequeues <see cref="Row"/> items while iterated. Parallel enumerations 
    /// is supported and thread-safe. After a full enumeration of this instance, following
    /// enumerations will be empty, as all rows have been dequeued.
    /// </para>
    /// </summary>
    /// <remarks>
    /// RowSet paging is not available with the version 1 of the native protocol. 
    /// If the protocol version 1 is in use, a RowSet is always fetched in it's entirely and
    /// it's up to the client to make sure that no query can yield ResultSet that won't hold
    /// in memory.
    /// </remarks>
    public class RowSet : IEnumerable<Row>, IDisposable
    {
        private static readonly CqlColumn[] EmptyColumns = new CqlColumn[0];
        private readonly object _pageLock = new object();
        private Func<byte[], RowSet> _fetchNextPage;
        // ReSharper disable once InconsistentNaming
        /// <summary>
        /// Contains the PagingState keys of the pages already retrieved.
        /// </summary>
        protected ConcurrentDictionary<byte[], bool> _pagers;

        /// <summary>
        /// Determines if when dequeuing, it will automatically fetch the following result pages.
        /// </summary>
        protected internal bool AutoPage { get; set; }

        /// <summary>
        /// Delegate that is called to get the next page.
        /// </summary>
        protected internal Func<byte[], RowSet> FetchNextPage
        {
            get { return _fetchNextPage; }
            set
            {
                if (_fetchNextPage != null)
                {
                    throw new InvalidOperationException("Multiple sets to FetchNextPage not supported");
                }
                if (value != null)
                {
                    // ReSharper disable once InconsistentlySynchronizedField
                    // The getter is called outside synchronization
                    _pagers = new ConcurrentDictionary<byte[], bool>(1, 1);
                }
                _fetchNextPage = value;
            }
        }

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
        /// Gets or sets the columns in the RowSet
        /// </summary>
        public virtual CqlColumn[] Columns { get; set; }

        /// <summary>
        /// Gets or sets the paging state of the query for the RowSet.
        /// When set it states that there are more pages.
        /// </summary>
        public virtual byte[] PagingState { get; set; }

        /// <summary>
        /// Returns whether this ResultSet has more results.
        /// It has side-effects, if the internal queue has been consumed it will page for more results.
        /// </summary>
        public virtual bool IsExhausted()
        {
            if (RowQueue == null)
            {
                return true;
            }
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
                return PagingState == null || !AutoPage;
            } 
        }

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
            return Task.Factory.StartNew(FetchMoreResults);
        }

        /// <summary>
        /// The number of rows available in this row set that can be retrieved without blocking to fetch.
        /// </summary>
        public int GetAvailableWithoutFetching()
        {
            return RowQueue == null ? 0 : RowQueue.Count;
        }

        /// <summary>
        /// For backward compatibility: It is possible to iterate using the RowSet as it is enumerable.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Row> GetRows()
        {
            //legacy: Keep the GetRows method for Compatibility.
            return this;
        }

        public virtual IEnumerator<Row> GetEnumerator()
        {
            if (RowQueue == null)
            {
                yield break;
            }
            while (!IsExhausted())
            {
                Row row;
                while (RowQueue.TryDequeue(out row))
                {
                    yield return row;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Gets the next results and add the rows to the current RowSet queue
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
                if (alreadyPresent)
                {
                    return;
                }
                var rs = FetchNextPage(pageState);
                foreach (var newRow in rs.RowQueue)
                {
                    RowQueue.Enqueue(newRow);
                }
                PagingState = rs.PagingState;
                _pagers.AddOrUpdate(pageState, true, (k, v) => v);
            }
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
