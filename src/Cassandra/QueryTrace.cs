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
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  The Cassandra trace for a query. <p> Such trace is generated by Cassandra
    ///  when query tracing is enabled for the query. The trace itself is stored in
    ///  Cassandra in the <c>sessions</c> and <c>events</c> table in the
    ///  <c>system_traces</c> keyspace and can be retrieve manually using the
    ///  trace identifier (the one returned by <link>#getTraceId</link>). </p><p> This
    ///  class provides facilities to fetch the traces from Cassandra. Please note
    ///  that the writting of the trace is done asynchronously in Cassandra. So
    ///  accessing the trace too soon after the query may result in the trace being
    ///  incomplete.</p>
    /// </summary>
    public class QueryTrace
    {
        private const string SelectSessionsFormat = "SELECT * FROM system_traces.sessions WHERE session_id = {0}";

        private const string SelectEventsFormat = "SELECT * FROM system_traces.events WHERE session_id = {0}";
        private readonly object _fetchLock = new object();
        private readonly Logger _logger = new Logger(typeof (QueryTrace));
        private readonly ISession _session;

        private readonly Guid _traceId;

        private IPAddress _coordinator;
        private int _duration = int.MinValue;
        private List<Event> _events;
        private IDictionary<string, string> _parameters;
        private string _requestType;
        private long _startedAt;

        /// <summary>
        ///  The identifier of this trace.
        /// </summary>
        /// 
        /// <returns>the identifier of this trace.</returns>
        public Guid TraceId
        {
            get { return _traceId; }
        }

        /// <summary>
        ///  The type of request.
        /// </summary>
        /// 
        /// <returns>the type of request. This method returns <c>null</c> if the
        ///  request type is not yet available.</returns>
        public string RequestType
        {
            get
            {
                MaybeFetchTrace();
                return _requestType;
            }
        }

        /// <summary>
        ///  The (server side) duration of the query in microseconds.
        /// </summary>
        /// 
        /// <returns>the (server side) duration of the query in microseconds. This method
        ///  will return <c>Integer.MIN_VALUE</c> if the duration is not yet
        ///  available.</returns>
        public int DurationMicros
        {
            get
            {
                MaybeFetchTrace();
                return _duration;
            }
        }

        /// <summary>
        ///  The coordinator host of the query.
        /// </summary>
        /// 
        /// <returns>the coordinator host of the query. This method returns
        ///  <c>null</c> if the coordinator is not yet available.</returns>
        public IPAddress Coordinator
        {
            get
            {
                MaybeFetchTrace();
                return _coordinator;
            }
        }

        /// <summary>
        ///  The parameters attached to this trace.
        /// </summary>
        /// 
        /// <returns>the parameters attached to this trace. This method returns
        ///  <c>null</c> if the coordinator is not yet available.</returns>
        public IDictionary<string, string> Parameters
        {
            get
            {
                MaybeFetchTrace();
                return _parameters;
            }
        }

        /// <summary>
        ///  The server side timestamp of the start of this query.
        /// </summary>
        /// 
        /// <returns>the server side timestamp of the start of this query. This method
        ///  returns 0 if the start timestamp is not available.</returns>
        public long StartedAt
        {
            get
            {
                MaybeFetchTrace();
                return _startedAt;
            }
        }

        /// <summary>
        ///  The events contained in this trace.
        /// </summary>
        /// 
        /// <returns>the events contained in this trace.</returns>
        public List<Event> Events
        {
            get
            {
                MaybeFetchTrace();
                return _events;
            }
        }

        /// <summary>
        /// Source address of the query
        /// </summary>
        public IPAddress ClientAddress { get; private set; }

        public QueryTrace(Guid traceId, ISession session)
        {
            _traceId = traceId;
            _session = session;
        }

        public override string ToString()
        {
            MaybeFetchTrace();
            return string.Format("{0} [{1}] - {2}µs", _requestType, _traceId, _duration);
        }

        private void MaybeFetchTrace()
        {
            if (_duration != int.MinValue)
                return;

            lock (_fetchLock)
            {
                // If by the time we grab the lock we've fetch the events, it's
                // fine, move on. Otherwise, fetch them.
                if (_duration == int.MinValue)
                {
                    DoFetchTrace();
                }
            }
        }

        private void DoFetchTrace()
        {
            try
            {
                var sessionRow = _session.Execute(string.Format(SelectSessionsFormat, _traceId)).First();
                _requestType = sessionRow.GetValue<string>("request");
                if (!sessionRow.IsNull("duration"))
                {
                    _duration = sessionRow.GetValue<int>("duration");
                }
                _coordinator = sessionRow.GetValue<IPAddress>("coordinator");
                if (!sessionRow.IsNull("parameters"))
                {
                    _parameters = sessionRow.GetValue<IDictionary<string, string>>("parameters");
                }
                _startedAt = sessionRow.GetValue<DateTimeOffset>("started_at").ToFileTime();
                if (sessionRow.GetColumn("client") != null)
                {
                    ClientAddress = sessionRow.GetValue<IPAddress>("client");
                }
                _events = new List<Event>();

                var eventRows = _session.Execute(string.Format(SelectEventsFormat, _traceId));
                foreach (var row in eventRows)
                {
                    _events.Add(new Event(row.GetValue<string>("activity"),
                                            new DateTimeOffset(
                                                Utils.GetTimestampFromGuid(row.GetValue<Guid>("event_id")) +
                                                (new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero)).Ticks, TimeSpan.Zero),
                                            row.GetValue<IPAddress>("source"),
                                            row.IsNull("source_elapsed") ? 0 : row.GetValue<int>("source_elapsed"),
                                            row.GetValue<string>("thread")));
                }
            }
            catch (Exception ex)
            {
                _logger.Error("Unexpected exception while fetching query trace", ex);
                throw new TraceRetrievalException("Unexpected exception while fetching query trace", ex);
            }
        }

        /// <summary>
        ///  A trace event. <p> A query trace is composed of a list of trace events.</p>
        /// </summary>
        public class Event
        {
            private readonly string _name;
            private readonly IPAddress _source;
            private readonly int _sourceElapsed;
            private readonly string _threadName;
            private readonly DateTimeOffset _timestamp;

            /// <summary>
            ///  The event description, i.e. which activity this event correspond to.
            /// </summary>
            /// 
            /// <returns>the event description.</returns>
            public string Description
            {
                get { return _name; }
            }

            /// <summary>
            ///  The server side timestamp of the event.
            /// </summary>
            /// 
            /// <returns>the server side timestamp of the event.</returns>
            public DateTimeOffset Timestamp
            {
                get { return _timestamp; }
            }

            /// <summary>
            ///  The address of the host having generated this event.
            /// </summary>
            /// 
            /// <returns>the address of the host having generated this event.</returns>
            public IPAddress Source
            {
                get { return _source; }
            }

            /// <summary>
            ///  The number of microseconds elapsed on the source when this event occurred
            ///  since when the source started handling the query.
            /// </summary>
            /// 
            /// <returns>the elapsed time on the source host when that event happened in
            ///  microseconds.</returns>
            public int SourceElapsedMicros
            {
                get { return _sourceElapsed; }
            }

            /// <summary>
            ///  The name of the thread on which this event occured.
            /// </summary>
            /// 
            /// <returns>the name of the thread on which this event occured.</returns>
            public string ThreadName
            {
                get { return _threadName; }
            }

            internal Event(string name, DateTimeOffset timestamp, IPAddress source, int sourceElapsed, string threadName)
            {
                _name = name;
                // Convert the UUID timestamp to an epoch timestamp; I stole this seemingly random value from cqlsh, hopefully it's correct.'
//                this._timestamp = (timestamp - 0x01b21dd213814000L)/10000;
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

// end namespace
