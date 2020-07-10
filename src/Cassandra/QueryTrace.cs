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
using System.Net;
using System.Threading.Tasks;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// The Cassandra trace for a query. 
    /// <para>
    /// The trace is generated by Cassandra when query tracing is enabled for the query. The trace itself is stored in
    /// Cassandra in the <c>sessions</c> and <c>events</c> table in the <c>system_traces</c> keyspace and can be 
    /// retrieve manually using the trace identifier (the one returned by <see cref="TraceId"/>).
    /// </para>
    /// </summary>
    public class QueryTrace
    {
        private readonly object _fetchLock = new object();
        private readonly Guid _traceId;
        private readonly IInternalSession _session;
        private IPAddress _coordinator;
        private int _duration = int.MinValue;
        private List<Event> _events;
        private IDictionary<string, string> _parameters;
        private string _requestType;
        private long _startedAt;
        private volatile bool _isDisconnected;
        private IPAddress _clientAddress;
        private readonly int _metadataFetchSyncTimeout;

        /// <summary>
        /// The identifier of this trace.
        /// </summary>
        /// <returns>the identifier of this trace.</returns>
        public Guid TraceId
        {
            get { return _traceId; }
        }

        /// <summary>
        /// The type of request.
        /// </summary>
        /// <returns>the type of request. This method returns <c>null</c> if the
        ///  request type is not yet available.</returns>
        public string RequestType
        {
            get
            {
                MaybeFetchTrace();
                return _requestType;
            }
            internal set { _requestType = value; }
        }

        /// <summary>
        /// The (server side) duration of the query in microseconds.
        /// </summary>
        /// <returns>
        /// The (server side) duration of the query in microseconds. This method will return <c>Int32.MinValue</c> if
        /// the duration is not yet available.
        /// </returns>
        public int DurationMicros
        {
            get
            {
                MaybeFetchTrace();
                return _duration;
            }
            internal set { _duration = value; }
        }

        /// <summary>
        /// The coordinator host of the query.
        /// </summary>
        /// <returns>
        /// The coordinator host of the query. This method returns <c>null</c> if the coordinator is not yet
        /// available.
        /// </returns>
        public IPAddress Coordinator
        {
            get
            {
                MaybeFetchTrace();
                return _coordinator;
            }
            internal set { _coordinator = value; }
        }

        /// <summary>
        /// The parameters attached to this trace.
        /// </summary>
        /// <returns>
        /// The parameters attached to this trace. This method returns <c>null</c> if the coordinator is not yet
        /// available.
        /// </returns>
        public IDictionary<string, string> Parameters
        {
            get
            {
                MaybeFetchTrace();
                return _parameters;
            }
            internal set { _parameters = value; }
        }

        /// <summary>
        /// The server side timestamp of the start of this query.
        /// </summary>
        /// <returns>
        /// The server side timestamp of the start of this query.
        /// This method returns 0 if the start timestamp is not available.
        /// </returns>
        public long StartedAt
        {
            get
            {
                MaybeFetchTrace();
                return _startedAt;
            }
            internal set { _startedAt = value; }
        }

        /// <summary>
        /// The events contained in this trace.
        /// </summary>
        /// <returns>The events contained in this trace.</returns>
        public List<Event> Events
        {
            get
            {
                MaybeFetchTrace();
                return _events;
            }
            internal set { _events = value; }
        }

        /// <summary>
        /// Source address of the query.
        /// </summary>
        public IPAddress ClientAddress
        {
            get
            {
                MaybeFetchTrace();
                return _clientAddress;
            }
            internal set { _clientAddress = value; }
        }

        internal QueryTrace(Guid traceId, IInternalSession session)
        {
            if (session == null)
            {
                throw new ArgumentNullException("session");
            }
            if (session.Cluster == null)
            {
                throw new NullReferenceException("session.Cluster is null");
            }
            //The instance is created before fetching the actual trace metadata
            //The properties will be populated later.
            _traceId = traceId;
            _session = session;
            _metadataFetchSyncTimeout = session.Cluster.Configuration.DefaultRequestOptions.QueryAbortTimeout;
        }

        public override string ToString()
        {
            MaybeFetchTrace();
            return $"{_requestType} [{_traceId}] - {_duration}µs";
        }

        private void MaybeFetchTrace()
        {
            if (_isDisconnected)
            {
                //Explicitly avoid metadata fetches
                return;
            }
            if (_duration != int.MinValue)
            {
                return;
            }

            lock (_fetchLock)
            {
                // If by the time we grab the lock we've fetch the events, it's
                // fine, move on. Otherwise, fetch them.
                if (_duration != int.MinValue)
                {
                    return;
                }
                DoFetchTrace();
            }
        }

        private void DoFetchTrace()
        {
            try
            {
                Load();
            }
            catch (Exception ex)
            {
                throw new TraceRetrievalException("Unexpected exception while fetching query trace", ex);
            }
            finally
            {
                _isDisconnected = false;   
            }
        }
        internal QueryTrace Load()
        {
            // mark as disconnected, guaranteeing that it wont make metadata fetches triggered by a property get
            // ReSharper disable once InconsistentlySynchronizedField : Can be both async and sync, don't mind
            _isDisconnected = false;
            var metadata = _session.TryInitAndGetMetadata();
            return TaskHelper.WaitToComplete(metadata.GetQueryTraceAsync(this), _metadataFetchSyncTimeout);
        }

        internal async Task<QueryTrace> LoadAsync()
        {
            // mark as disconnected, guaranteeing that it wont make metadata fetches triggered by a property get
            // ReSharper disable once InconsistentlySynchronizedField : Can be both async and sync, don't mind
            _isDisconnected = false;
            var metadata = await _session.TryInitAndGetMetadataAsync().ConfigureAwait(false);
            return await metadata.GetQueryTraceAsync(this).ConfigureAwait(false);
        }

        /// <summary>
        /// A trace event.
        /// <para>
        /// A query trace is composed of a list of trace events.
        /// </para>
        /// </summary>
        public class Event
        {
            private readonly string _name;
            private readonly IPAddress _source;
            private readonly int _sourceElapsed;
            private readonly string _threadName;
            private readonly DateTimeOffset _timestamp;

            /// <summary>
            /// The event description, i.e. which activity this event correspond to.
            /// </summary>
            /// <returns>The event description.</returns>
            public string Description
            {
                get { return _name; }
            }

            /// <summary>
            /// The server side timestamp of the event.
            /// </summary>
            /// <returns>The server side timestamp of the event.</returns>
            public DateTimeOffset Timestamp
            {
                get { return _timestamp; }
            }

            /// <summary>
            /// The address of the host having generated this event.
            /// </summary>
            /// <returns>The address of the host having generated this event.</returns>
            public IPAddress Source
            {
                get { return _source; }
            }

            /// <summary>
            /// The number of microseconds elapsed on the source when this event occurred
            /// since when the source started handling the query.
            /// </summary>
            /// <returns>the elapsed time on the source host when that event happened in
            ///  microseconds.</returns>
            public int SourceElapsedMicros
            {
                get { return _sourceElapsed; }
            }

            /// <summary>
            /// The name of the thread on which this event occurred.
            /// </summary>
            /// <returns>the name of the thread on which this event occurred.</returns>
            public string ThreadName
            {
                get { return _threadName; }
            }

            internal Event(string name, DateTimeOffset timestamp, IPAddress source, int sourceElapsed, string threadName)
            {
                _name = name;
                _timestamp = timestamp;
                _source = source;
                _sourceElapsed = sourceElapsed;
                _threadName = threadName;
            }

            public override string ToString()
            {
                return string.Format("{0} on {1}[{2}] at {3}", _name, _source, _threadName, _timestamp);
            }
        }
    }
}