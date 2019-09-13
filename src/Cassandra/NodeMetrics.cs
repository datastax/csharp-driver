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
        public static readonly IEnumerable<string> AllNodeMetrics = new[]
        {
            NodeMetrics.BytesSent,
            NodeMetrics.SpeculativeExecutions,
            NodeMetrics.BytesReceived,
            NodeMetrics.CqlMessages,

            Pool.OpenConnections,
            Pool.AvailableStreams,
            Pool.InFlight,
            Pool.MaxRequestsPerConnection,

            Errors.Connection.Auth,
            Errors.Connection.Init,

            Errors.Request.Aborted,
            Errors.Request.Other,
            Errors.Request.ReadTimeout,
            Errors.Request.Unavailable,
            Errors.Request.Unsent,
            Errors.Request.WriteTimeout,

            Retries.Unavailable,
            Retries.Aborted,
            Retries.Other,
            Retries.ReadTimeout,
            Retries.WriteTimeout,

            Ignores.ReadTimeout,
            Ignores.Unavailable,
            Ignores.Aborted,
            Ignores.Other,
            Ignores.WriteTimeout
        };

        public const string BytesSent = "bytes-sent";

        public const string SpeculativeExecutions = "speculative-executions";

        public const string BytesReceived = "bytes-received";

        public const string CqlMessages = "cql-messages";
        
        public static class Pool
        {
            public const string OpenConnections = "pool.open-connections";
            
            public const string AvailableStreams = "pool.available-streams";
            
            public const string InFlight = "pool.in-flight";
            
            public const string MaxRequestsPerConnection = "pool.max-requests-per-connection";
        }

        public static class Errors
        {
            public static class Connection
            {
                public const string Init = "errors.connection.init";

                public const string Auth = "errors.connection.auth";
            }

            public static class Request
            {
                public const string ReadTimeout = "errors.request.read-timeout";

                public const string WriteTimeout = "errors.request.write-timeout";

                public const string Unavailable = "errors.request.unavailable";

                public const string Other = "errors.request.other";

                public const string Aborted = "errors.request.aborted";

                public const string Unsent = "errors.request.unsent";
            }
        }

        public static class Retries
        {
            public const string ReadTimeout = "retries.read-timeout";

            public const string WriteTimeout = "retries.write-timeout";

            public const string Unavailable = "retries.unavailable";

            public const string Other = "retries.other";
            
            public const string Aborted = "retries.aborted";
        }
        
        public static class Ignores
        {
            public const string ReadTimeout = "ignores.read-timeout";

            public const string WriteTimeout = "ignores.write-timeout";

            public const string Unavailable = "ignores.unavailable";

            public const string Other = "ignores.other";
            
            public const string Aborted = "ignores.aborted";
        }
    }
}