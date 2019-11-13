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
using System.Collections.ObjectModel;
using System.Net;

namespace Cassandra
{
    /// <summary>
    /// Represents a server-side failure (non-timeout) during a write query.
    /// </summary>
    public class WriteFailureException : QueryExecutionException
    {
        private static readonly IDictionary<IPAddress, int> DefaultReasons =
            new ReadOnlyDictionary<IPAddress, int>(new Dictionary<IPAddress, int>(0));

        private const string FailureMessage = "Server failure during write query at consistency {0}" +
            " ({1} responses were required but only {2} replica responded, {3} failed)";

        /// <summary>
        /// Gets the type of write operation that timed out.
        /// <para>Possible values: SIMPLE, BATCH, BATCH_LOG, UNLOGGED_BATCH and COUNTER.</para>
        /// </summary>
        public string WriteType { get; }

        /// <summary>
        ///  Gets the consistency level of the operation
        /// </summary>
        public ConsistencyLevel ConsistencyLevel { get; private set; }

        /// <summary>
        /// Gets the number of replica that had acknowledged/responded to the operation
        /// </summary>
        public int ReceivedAcknowledgements { get; private set; }

        /// <summary>
        ///  Gets the minimum number of replica acknowledgements/responses that were required to fulfill the operation. 
        /// </summary>
        public int RequiredAcknowledgements { get; private set; }

        /// <summary>
        /// Gets the number of nodes that experienced a failure while executing the request.
        /// </summary>
        public int Failures { get; private set; }

        /// <summary>
        /// Gets a failure reason code for each node that failed.
        /// <para>
        /// On older protocol versions, an empty map and only the number of <see cref="Failures"/> is provided.
        /// </para>
        /// <list>
        /// <item><term>0x0000</term><description>Unknown reason</description></item>
        /// <item><term>0x0001</term><description>Too many tombstones read (as controlled by the yaml
        /// tombstone_failure_threshold option)</description></item>
        /// <item><term>0x0002</term><description>The query uses an index but that index is not available
        /// (built) on the queried endpoint.</description></item>
        /// <item><term>0x0003</term><description>The query writes on some CDC enabled tables, but the CDC space is
        /// full (CDC data isn't consumed fast enough). Note that this can only happen in Write_failure in practice,
        /// but the reasons are shared between both exception.</description></item>
        /// <item><term>0x0004</term><description>Some failures (one or more) were reported to the replica "leading"
        /// a counter write. The actual error didn't occur on the node that sent this failure, it is is simply the
        /// node reporting it due to how counter writes work; the initial reason for the failure should have been
        /// logged on the actual replica on which the problem occured).</description></item>
        /// <item><term>0x0005</term><description>The table used by the query was not found on at least one of the
        /// replica. This strongly suggest a query was done on either a newly created or newly dropped table with
        /// having waited for schema agreement first.</description></item>
        /// <item><term>0x0006</term><description>The keyspace used by the query was not found on at least one
        /// replica. Same likely cause as for tables above.</description></item>
        /// </list>
        /// </summary>
        public IDictionary<IPAddress, int> Reasons { get; } = DefaultReasons;

        /// <summary>
        /// Creates a new instance of <see cref="WriteFailureException"/>.
        /// </summary>
        public WriteFailureException(ConsistencyLevel consistency, int received, int required, string writeType,
                                     int failures) : base(string.Format(FailureMessage, 
                                        consistency.ToString().ToUpper(), required, received, failures))
        {
            ConsistencyLevel = consistency;
            ReceivedAcknowledgements = received;
            RequiredAcknowledgements = required;
            WriteType = writeType;
            Failures = failures;
        }

        /// <summary>
        /// Creates a new instance of <see cref="WriteFailureException"/> providing the failure reasons dictionary.
        /// </summary>
        public WriteFailureException(ConsistencyLevel consistency, int received, int required, string writeType,
                                     IDictionary<IPAddress, int> reasons) :
                                     this(consistency, received, required, writeType, reasons?.Count ?? 0)
        {
            Reasons = reasons ?? DefaultReasons;
        }
    }
}
