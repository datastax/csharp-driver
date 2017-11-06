//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
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
