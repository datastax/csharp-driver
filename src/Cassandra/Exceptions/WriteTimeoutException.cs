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

namespace Cassandra
{
    /// <summary>
    /// Represents a server timeout during a write operation.
    /// </summary>
    public class WriteTimeoutException : QueryTimeoutException
    {
        private const string BatchLogMessage = "Server timeout during batchlog write at consistency {0}" +
            " ({1} peer(s) acknowledged the write over {2} required)";
        
        private const string QueryMessage = "Server timeout during write query at consistency {0}" +
            " ({1} peer(s) acknowledged the write over {2} required)";
        
        private const string BatchLogWriteType = "BATCH_LOG";
        
        /// <summary>
        /// Gets the type of write operation that timed out.
        /// <para>Possible values: SIMPLE, BATCH, BATCH_LOG, UNLOGGED_BATCH and COUNTER.</para>
        /// </summary>
        public string WriteType { get; }

        /// <summary>
        /// Creates a new instance of <see cref="WriteTimeoutException"/>
        /// </summary>
        public WriteTimeoutException(ConsistencyLevel consistency, int received, int required,
                                     string writeType) : base(
                                         GetMessage(writeType, consistency, received, required),
                                         consistency,
                                         received,
                                         required)
        {
            WriteType = writeType;
        }

        private static string GetMessage(string writeType, ConsistencyLevel consistency, int received, int required)
        {
            var message = writeType == BatchLogWriteType ? BatchLogMessage : QueryMessage;
            return string.Format(message, consistency.ToString().ToUpper(), received, required);
        }
    }
}
