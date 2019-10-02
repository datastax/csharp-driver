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
        /// Here is what this property will return for <see cref="NodeMetric.Meters.ReadTimeouts"/>:
        /// <code>
        /// // Assume the following full metric path the NodeMetric.Meters.ReadTimeouts metric:
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
            Counters.BytesSent,
            Counters.SpeculativeExecutions,
            Counters.BytesReceived,

            Counters.AuthenticationErrors,
            Counters.ConnectionInitErrors,

            Timers.CqlMessages,

            Gauges.OpenConnections,
            Gauges.AvailableStreams,
            Gauges.InFlight,

            Meters.AbortedRequests,
            Meters.OtherErrors,
            Meters.ReadTimeouts,
            Meters.UnavailableErrors,
            Meters.UnsentRequests,
            Meters.WriteTimeouts,
            Meters.ClientTimeouts,
            Meters.Errors,

            Meters.RetriesOnUnavailable,
            Meters.RetriesOnOtherError,
            Meters.RetriesOnReadTimeout,
            Meters.RetriesOnWriteTimeout,
            Meters.Retries,

            Meters.IgnoresOnReadTimeout,
            Meters.IgnoresOnUnavailable,
            Meters.IgnoresOnOtherError,
            Meters.IgnoresOnWriteTimeout,
            Meters.Ignores
        };

        public static class Counters
        {
            public static readonly NodeMetric BytesSent = new NodeMetric("bytes-sent");

            public static readonly NodeMetric SpeculativeExecutions = new NodeMetric("speculative-executions");

            public static readonly NodeMetric BytesReceived = new NodeMetric("bytes-received");

            public static readonly NodeMetric ConnectionInitErrors = new NodeMetric("errors.connection.init");

            public static readonly NodeMetric AuthenticationErrors = new NodeMetric("errors.connection.auth");
        }

        public static class Gauges
        {
            public static readonly NodeMetric OpenConnections = new NodeMetric("pool.open-connections");

            public static readonly NodeMetric AvailableStreams = new NodeMetric("pool.available-streams");

            public static readonly NodeMetric InFlight = new NodeMetric("pool.in-flight");
        }

        public static class Timers
        {
            public static readonly NodeMetric CqlMessages = new NodeMetric("cql-messages");
        }

        public static class Meters
        {
            public static readonly NodeMetric RetriesOnReadTimeout = new NodeMetric("retries.read-timeout");

            public static readonly NodeMetric RetriesOnWriteTimeout = new NodeMetric("retries.write-timeout");

            public static readonly NodeMetric RetriesOnUnavailable = new NodeMetric("retries.unavailable");

            public static readonly NodeMetric RetriesOnOtherError = new NodeMetric("retries.other");

            public static readonly NodeMetric Retries = new NodeMetric("retries.total");
            
            public static readonly NodeMetric IgnoresOnReadTimeout = new NodeMetric("ignores.read-timeout");

            public static readonly NodeMetric IgnoresOnWriteTimeout = new NodeMetric("ignores.write-timeout");

            public static readonly NodeMetric IgnoresOnUnavailable = new NodeMetric("ignores.unavailable");

            public static readonly NodeMetric IgnoresOnOtherError = new NodeMetric("ignores.other");

            public static readonly NodeMetric Ignores = new NodeMetric("ignores.total");
            
            public static readonly NodeMetric ReadTimeouts = new NodeMetric("errors.request.read-timeout");

            public static readonly NodeMetric WriteTimeouts = new NodeMetric("errors.request.write-timeout");

            public static readonly NodeMetric UnavailableErrors = new NodeMetric("errors.request.unavailable");

            public static readonly NodeMetric OtherErrors = new NodeMetric("errors.request.other");

            public static readonly NodeMetric Errors = new NodeMetric("errors.request.total");

            public static readonly NodeMetric AbortedRequests = new NodeMetric("errors.request.aborted");

            public static readonly NodeMetric UnsentRequests = new NodeMetric("errors.request.unsent");

            public static readonly NodeMetric ClientTimeouts = new NodeMetric("errors.request.client-timeout");
        }
    }
}