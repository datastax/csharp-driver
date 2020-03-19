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
using System.Linq;

namespace Cassandra.Metrics
{
    /// <summary>
    /// Represents an individual session metric.
    /// </summary>
    public sealed class SessionMetric : IEquatable<SessionMetric>, IEquatable<IMetric>, IMetric
    {
        static SessionMetric()
        {
            SessionMetric.DefaultSessionMetrics = new[]
            {
                Meters.BytesSent,
                Meters.BytesReceived,

                Counters.CqlClientTimeouts,

                Gauges.ConnectedNodes
            };
            
            SessionMetric.AllSessionMetrics =
                SessionMetric.DefaultSessionMetrics.Union(new[] { SessionMetric.Timers.CqlRequests }).ToList();
        }

        private readonly int _hashCode;

        internal SessionMetric(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _hashCode = name.GetHashCode();
        }

        /// <summary>
        /// Metric name. For example, <see cref="Counters.CqlClientTimeouts"/> returns "cql-client-timeouts".
        /// </summary>
        public string Name { get; }

        public bool Equals(SessionMetric other)
        {
            return other != null && StrictEqualsNotNull(other);
        }

        public override bool Equals(object obj)
        {
            return StrictEquals(obj);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }

        public bool Equals(IMetric other)
        {
            return StrictEquals(other);
        }

        public override string ToString()
        {
            return Name;
        }

        private bool StrictEquals(object obj)
        {
            return obj != null && obj is SessionMetric other && StrictEqualsNotNull(other);
        }

        private bool StrictEqualsNotNull(SessionMetric other)
        {
            return string.Equals(Name, other.Name);
        }
        
        /// <summary>
        /// A collection with all session metrics including Timers.
        /// </summary>
        public static readonly IEnumerable<SessionMetric> AllSessionMetrics;

        /// <summary>
        /// A collection with all session metrics except Timers.
        /// </summary>
        public static readonly IEnumerable<SessionMetric> DefaultSessionMetrics;

        public static class Timers
        {
            /// <summary>
            /// Tracks the number/rate and latency of cql requests.
            /// </summary>
            public static readonly SessionMetric CqlRequests = new SessionMetric("cql-requests");
        }

        public static class Meters
        {
            /// <summary>
            /// Tracks the number/rate of bytes sent.
            /// </summary>
            public static readonly SessionMetric BytesSent = new SessionMetric("bytes-sent");
            
            /// <summary>
            /// Tracks the number/rate of bytes received.
            /// </summary>
            public static readonly SessionMetric BytesReceived = new SessionMetric("bytes-received");
        }

        public static class Counters
        {
            /// <summary>
            /// Number of client timeouts (timeout that affects the synchronous API only, see <see cref="Builder.WithQueryTimeout"/>).
            /// </summary>
            public static readonly SessionMetric CqlClientTimeouts = new SessionMetric("cql-client-timeouts");
        }

        public static class Gauges
        {
            /// <summary>
            /// Number of connected hosts.
            /// </summary>
            public static readonly SessionMetric ConnectedNodes = new SessionMetric("connected-nodes");
        }
    }
}