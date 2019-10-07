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

namespace Cassandra.Metrics
{
    /// <summary>
    /// Represents an individual node metric.
    /// </summary>
    public sealed class NodeMetric : IEquatable<NodeMetric>, IEquatable<IMetric>, IMetric
    {
        private readonly int _hashCode;

        private NodeMetric(string path)
        {
            Path = path ?? throw new ArgumentNullException(nameof(path));
            _hashCode = path.GetHashCode();
        }

        /// <summary>
        /// Metric path.
        /// Here is what this property will return for <see cref="NodeMetric.Counters.ReadTimeouts"/>:
        /// <code>
        /// // Assume this is the full metric path for the NodeMetric.Counters.ReadTimeouts metric (using the AppMetricsProvider):
        /// web.app.session.nodes.127_0_0_1:9042.errors.request.read-timeout
        ///
        /// // NodeMetric.Path property will return
        /// errors.request.read-timeout
        /// </code>
        /// </summary>
        public string Path { get; }

        public bool Equals(NodeMetric other)
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
            return obj != null && obj is NodeMetric other && StrictEqualsNotNull(other);
        }

        private bool StrictEqualsNotNull(NodeMetric other)
        {
            return string.Equals(Path, other.Path);
        }

        public static readonly IEnumerable<NodeMetric> AllNodeMetrics = new[]
        {
            Meters.BytesSent,
            Meters.BytesReceived,

            Timers.CqlMessages,

            Gauges.OpenConnections,
            Gauges.InFlight,
            
            Counters.SpeculativeExecutions,
            Counters.AuthenticationErrors,
            Counters.ConnectionInitErrors,

            Counters.AbortedRequests,
            Counters.OtherErrors,
            Counters.ReadTimeouts,
            Counters.UnavailableErrors,
            Counters.UnsentRequests,
            Counters.WriteTimeouts,
            Counters.ClientTimeouts,
            Counters.Errors,

            Counters.RetriesOnUnavailable,
            Counters.RetriesOnOtherError,
            Counters.RetriesOnReadTimeout,
            Counters.RetriesOnWriteTimeout,
            Counters.Retries,

            Counters.IgnoresOnReadTimeout,
            Counters.IgnoresOnUnavailable,
            Counters.IgnoresOnOtherError,
            Counters.IgnoresOnWriteTimeout,
            Counters.Ignores
        };

        public static class Counters
        {
            /// <summary>
            /// Number of speculative executions.
            /// </summary>
            public static readonly NodeMetric SpeculativeExecutions = new NodeMetric("speculative-executions");

            /// <summary>
            /// Connection initialization errors.
            /// </summary>
            public static readonly NodeMetric ConnectionInitErrors = new NodeMetric("errors.connection.init");

            /// <summary>
            /// Connection authentication errors.
            /// </summary>
            public static readonly NodeMetric AuthenticationErrors = new NodeMetric("errors.connection.auth");
            
            /// <summary>
            /// Retried attempts on read timeouts due to the retry policy.
            /// </summary>
            public static readonly NodeMetric RetriesOnReadTimeout = new NodeMetric("retries.read-timeout");
            
            /// <summary>
            /// Retried attempts on write timeouts due to the retry policy.
            /// </summary>
            public static readonly NodeMetric RetriesOnWriteTimeout = new NodeMetric("retries.write-timeout");
            
            /// <summary>
            /// Retried attempts on unavailable errors due to the retry policy.
            /// </summary>
            public static readonly NodeMetric RetriesOnUnavailable = new NodeMetric("retries.unavailable");
            
            /// <summary>
            /// Retried attempts on errors other than read timeouts, write timeouts and unavailable errors due to the retry policy.
            /// </summary>
            public static readonly NodeMetric RetriesOnOtherError = new NodeMetric("retries.other");

            /// <summary>
            /// Total retry attempts due to the retry policy.
            /// </summary>
            public static readonly NodeMetric Retries = new NodeMetric("retries.total");
            
            /// <summary>
            /// Ignored read timeouts due to the retry policy.
            /// </summary>
            public static readonly NodeMetric IgnoresOnReadTimeout = new NodeMetric("ignores.read-timeout");
            
            /// <summary>
            /// Ignored write timeouts due to the retry policy.
            /// </summary>
            public static readonly NodeMetric IgnoresOnWriteTimeout = new NodeMetric("ignores.write-timeout");
            
            /// <summary>
            /// Ignored unavailable errors due to the retry policy.
            /// </summary>
            public static readonly NodeMetric IgnoresOnUnavailable = new NodeMetric("ignores.unavailable");
            
            /// <summary>
            /// Ignored errors other than read timeouts, write timeouts and unavailable errors due to the retry policy.
            /// </summary>
            public static readonly NodeMetric IgnoresOnOtherError = new NodeMetric("ignores.other");
            
            /// <summary>
            /// Total ignored errors due to the retry policy.
            /// </summary>
            public static readonly NodeMetric Ignores = new NodeMetric("ignores.total");
            
            /// <summary>
            /// Number of server side read timeout errors.
            /// </summary>
            public static readonly NodeMetric ReadTimeouts = new NodeMetric("errors.request.read-timeout");
            
            /// <summary>
            /// Number of server side write timeout errors.
            /// </summary>
            public static readonly NodeMetric WriteTimeouts = new NodeMetric("errors.request.write-timeout");
            
            /// <summary>
            /// Number of server side unavailable errors.
            /// </summary>
            public static readonly NodeMetric UnavailableErrors = new NodeMetric("errors.request.unavailable");
            
            /// <summary>
            /// Number of server side errors other than Unavailable, ReadTimeout or WriteTimeout.
            /// </summary>
            public static readonly NodeMetric OtherErrors = new NodeMetric("errors.request.other");
            
            /// <summary>
            /// Number of total errors.
            /// </summary>
            public static readonly NodeMetric Errors = new NodeMetric("errors.request.total");
            
            /// <summary>
            /// Number of failed requests without a server response.
            /// </summary>
            public static readonly NodeMetric AbortedRequests = new NodeMetric("errors.request.aborted");
            
            /// <summary>
            /// Number of requests that failed before being sent to the server.
            /// </summary>
            public static readonly NodeMetric UnsentRequests = new NodeMetric("errors.request.unsent");
            
            /// <summary>
            /// Number of failed requests due to socket timeout.
            /// </summary>
            public static readonly NodeMetric ClientTimeouts = new NodeMetric("errors.request.client-timeout");
        }

        public static class Gauges
        {
            /// <summary>
            /// Number of open connections.
            /// </summary>
            public static readonly NodeMetric OpenConnections = new NodeMetric("pool.open-connections");

            /// <summary>
            /// Number of in flight requests.
            /// </summary>
            public static readonly NodeMetric InFlight = new NodeMetric("pool.in-flight");
        }

        public static class Timers
        {
            /// <summary>
            /// Timer that tracks the number/rate of cql requests and the time they take.
            /// </summary>
            public static readonly NodeMetric CqlMessages = new NodeMetric("cql-messages");
        }

        public static class Meters
        {
            /// <summary>
            /// Meter that tracks the number/rate of bytes sent.
            /// </summary>
            public static readonly NodeMetric BytesSent = new NodeMetric("bytes-sent");
            
            /// <summary>
            /// Meter that tracks the number/rate of bytes received.
            /// </summary>
            public static readonly NodeMetric BytesReceived = new NodeMetric("bytes-received");
        }
    }
}