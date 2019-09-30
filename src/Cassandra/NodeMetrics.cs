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

using System.Collections.Generic;

namespace Cassandra
{
    public static class NodeMetrics
    {
        public static readonly IEnumerable<NodeMetric> AllNodeMetrics = new[]
        {
            Counters.BytesSent,
            Counters.SpeculativeExecutions,
            Counters.BytesReceived,

            Counters.Errors.Connection.Auth,
            Counters.Errors.Connection.Init,

            Timers.CqlMessages,

            Gauges.Pool.OpenConnections,
            Gauges.Pool.AvailableStreams,
            Gauges.Pool.InFlight,
            Gauges.Pool.MaxRequestsPerConnection,

            Meters.Errors.Request.Aborted,
            Meters.Errors.Request.Other,
            Meters.Errors.Request.ReadTimeout,
            Meters.Errors.Request.Unavailable,
            Meters.Errors.Request.Unsent,
            Meters.Errors.Request.WriteTimeout,
            Meters.Errors.Request.ClientTimeout,
            Meters.Errors.Request.Total,

            Meters.Retries.Unavailable,
            Meters.Retries.Other,
            Meters.Retries.ReadTimeout,
            Meters.Retries.WriteTimeout,
            Meters.Retries.Total,

            Meters.Ignores.ReadTimeout,
            Meters.Ignores.Unavailable,
            Meters.Ignores.Other,
            Meters.Ignores.WriteTimeout,
            Meters.Ignores.Total
        };

        public static class Counters
        {
            public static readonly NodeMetric BytesSent = new NodeMetric("bytes-sent");

            public static readonly NodeMetric SpeculativeExecutions = new NodeMetric("speculative-executions");

            public static readonly NodeMetric BytesReceived = new NodeMetric("bytes-received");

            public static class Errors
            {
                public static class Connection
                {
                    public static readonly NodeMetric Init = new NodeMetric(new[] { "errors", "connection" }, "init");

                    public static readonly NodeMetric Auth = new NodeMetric(new[] { "errors", "connection" }, "auth");
                }
            }
        }

        public static class Gauges
        {
            public static class Pool
            {
                public static readonly NodeMetric OpenConnections = new NodeMetric(new[] { "pool" }, "open-connections");

                public static readonly NodeMetric AvailableStreams = new NodeMetric(new[] { "pool" }, "available-streams");

                public static readonly NodeMetric InFlight = new NodeMetric(new[] { "pool" }, "in-flight");

                public static readonly NodeMetric MaxRequestsPerConnection = new NodeMetric(new[] { "pool" }, "max-requests-per-connection");
            }
        }

        public static class Timers
        {
            public static readonly NodeMetric CqlMessages = new NodeMetric("cql-messages");
        }

        public static class Meters
        {
            public static class Retries
            {
                public static readonly NodeMetric ReadTimeout = new NodeMetric(new[] { "retries" }, "read-timeout");

                public static readonly NodeMetric WriteTimeout = new NodeMetric(new[] { "retries" }, "write-timeout");

                public static readonly NodeMetric Unavailable = new NodeMetric(new[] { "retries" }, "unavailable");

                public static readonly NodeMetric Other = new NodeMetric(new[] { "retries" }, "other");
                
                public static readonly NodeMetric Total = new NodeMetric(new[] { "retries" }, "total");
            }

            public static class Ignores
            {
                public static readonly NodeMetric ReadTimeout = new NodeMetric(new[] { "ignores" }, "read-timeout");

                public static readonly NodeMetric WriteTimeout = new NodeMetric(new[] { "ignores" }, "write-timeout");

                public static readonly NodeMetric Unavailable = new NodeMetric(new[] { "ignores" }, "unavailable");

                public static readonly NodeMetric Other = new NodeMetric(new[] { "ignores" }, "other");
                
                public static readonly NodeMetric Total = new NodeMetric(new[] { "ignores" }, "total");
            }

            public static class Errors
            {
                public static class Request
                {
                    public static readonly NodeMetric ReadTimeout = new NodeMetric(new[] { "errors", "request" }, "read-timeout");

                    public static readonly NodeMetric WriteTimeout = new NodeMetric(new[] { "errors", "request" }, "write-timeout");

                    public static readonly NodeMetric Unavailable = new NodeMetric(new[] { "errors", "request" }, "unavailable");

                    public static readonly NodeMetric Other = new NodeMetric(new[] { "errors", "request" }, "other");

                    public static readonly NodeMetric Aborted = new NodeMetric(new[] { "errors", "request" }, "aborted");

                    public static readonly NodeMetric Total = new NodeMetric(new[] { "errors", "request" }, "total");

                    public static readonly NodeMetric Unsent = new NodeMetric(new[] { "errors", "request" }, "unsent");
                    
                    public static readonly NodeMetric ClientTimeout = new NodeMetric(new[] { "errors", "request" }, "client-timeout");
                }
            }
        }
    }
}