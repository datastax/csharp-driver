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
    public static class SessionMetrics
    {
        public static readonly IEnumerable<SessionMetric> AllSessionMetrics = new[]
        {
            Counters.BytesSent,
            Meters.CqlClientTimeouts,
            Counters.BytesReceived,
            Timers.CqlRequests,
            Gauges.ConnectedNodes
        };

        public static class Timers
        {
            public static readonly SessionMetric CqlRequests = new SessionMetric("cql-requests");
        }

        public static class Meters
        {
            public static readonly SessionMetric CqlClientTimeouts = new SessionMetric("cql-client-timeouts");
        }

        public static class Counters
        {
            public static readonly SessionMetric BytesSent = new SessionMetric("bytes-sent");

            public static readonly SessionMetric BytesReceived = new SessionMetric("bytes-received");
        }

        public static class Gauges
        {
            public static readonly SessionMetric ConnectedNodes = new SessionMetric("connected-nodes");
        }
    }
}