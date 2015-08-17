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
using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  Basic information on the execution of a query. <p> This provides the
    ///  following information on the execution of a (successful) query: </p> <ul> <li>The
    ///  list of Cassandra hosts tried in order (usually just one, unless a node has
    ///  been tried but was dead/in error or a timeout provoked a retry (which depends
    ///  on the RetryPolicy)).</li> <li>The consistency level achieved by the query
    ///  (usually the one asked, though some specific RetryPolicy may allow this to be
    ///  different).</li> <li>The query trace recorded by Cassandra if tracing had
    ///  been set for the query.</li> </ul>
    /// </summary>
    public class ExecutionInfo
    {
        public ExecutionInfo()
        {
            AchievedConsistency = ConsistencyLevel.Any;
        }

        /// <summary>
        /// Gets the list of host that were queried before getting a valid response, 
        /// being the last host the one that replied correctly.
        /// </summary>
        public IList<IPEndPoint> TriedHosts { get; private set; }

        /// <summary>
        /// Returns the server-side warnings for this query.
        /// <para>
        /// This feature is only available for Cassandra 2.2 or above; with lower versions, this property always returns null.
        /// </para>
        /// </summary>
        public string[] Warnings { get; internal set; }

        /// <summary>
        /// Returns the incoming custom payload set by the server with its response, or null if the server have not include any custom payload.
        /// <para>
        /// This feature is only available for Cassandra 2.2 or above; with lower versions, this property always returns null.
        /// </para>
        /// </summary>
        public IDictionary<string, byte[]> IncomingPayload { get; internal set; }
        
        /// <summary>
        /// Retrieves the coordinator that responded to the request
        /// </summary>
        public IPEndPoint QueriedHost
        {
            get
            {
                if (TriedHosts == null)
                {
                    throw new NullReferenceException("Tried host is null");
                }
                return TriedHosts.Count > 0 ? TriedHosts[TriedHosts.Count - 1] : null;
            }
        }

        /// <summary>
        /// Gets the trace for the query execution.
        /// </summary>
        public QueryTrace QueryTrace { get; private set; }

        /// <summary>
        /// Gets the final achieved consistency
        /// </summary>
        public ConsistencyLevel AchievedConsistency { get; private set; }

        internal void SetTriedHosts(List<IPEndPoint> triedHosts)
        {
            TriedHosts = triedHosts;
        }

        internal void SetQueryTrace(QueryTrace queryTrace)
        {
            QueryTrace = queryTrace;
        }

        internal void SetAchievedConsistency(ConsistencyLevel achievedConsistency)
        {
            AchievedConsistency = achievedConsistency;
        }
    }
}