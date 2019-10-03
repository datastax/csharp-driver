//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Represents an individual session metric.
    /// </summary>
    public sealed class SessionMetric : IEquatable<SessionMetric>, IEquatable<IMetric>, IMetric
    {
        private readonly int _hashCode;

        internal SessionMetric(string path)
        {
            Path = path ?? throw new ArgumentNullException(nameof(path));
            _hashCode = path.GetHashCode();
        }

        /// <summary>
        /// Metric path.
        /// Here is what this property will return for <see cref="SessionMetric.Counters.CqlClientTimeouts"/>:
        /// <code>
        /// // Assume this ist the full metric path for the SessionMetric.Counters.CqlClientTimeouts metric (using AppMetricsProvider):
        /// web.app.session.cql-client-timeouts
        ///
        /// // The SessionMetric.Path property will return
        /// cql-client-timeouts
        /// </code>
        /// </summary>
        public string Path { get; }

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
            return Path;
        }

        private bool StrictEquals(object obj)
        {
            return obj != null && obj is SessionMetric other && StrictEqualsNotNull(other);
        }

        private bool StrictEqualsNotNull(SessionMetric other)
        {
            return string.Equals(Path, other.Path);
        }

        public static readonly IEnumerable<SessionMetric> AllSessionMetrics = new[]
        {
            Meters.BytesSent,
            Meters.BytesReceived,

            Counters.CqlClientTimeouts,

            Timers.CqlRequests,

            Gauges.ConnectedNodes
        };

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