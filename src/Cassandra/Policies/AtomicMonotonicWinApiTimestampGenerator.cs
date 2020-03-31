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
using System.Runtime.InteropServices;

namespace Cassandra
{
    /// <summary>
    /// A timestamp generator that guarantees monotonically increasing timestamps among all client threads
    /// and logs warnings when timestamps drift in the future, using Win API high precision
    /// <see href="https://msdn.microsoft.com/en-us/library/windows/desktop/hh706895.aspx">
    /// GetSystemTimePreciseAsFileTime()</see> method call available in Windows 8+ and Windows Server 2012+.
    /// </summary>
    public class AtomicMonotonicWinApiTimestampGenerator : AtomicMonotonicTimestampGenerator
    {
        [DllImport("Kernel32.dll", CallingConvention = CallingConvention.Winapi)]
        private static extern void GetSystemTimePreciseAsFileTime(out long filetime);
        
        protected sealed override long GetTimestamp()
        {
            GetSystemTimePreciseAsFileTime(out long preciseTime);
            var timestamp = DateTime.FromFileTimeUtc(preciseTime);
            return (timestamp.Ticks - UnixEpochTicks)/TicksPerMicrosecond;
        }

        /// <summary>
        /// Creates a new instance of <see cref="AtomicMonotonicTimestampGenerator"/>.
        /// </summary>
        /// <param name="warningThreshold">
        /// Determines how far in the future timestamps are allowed to drift before a warning is logged, expressed
        /// in milliseconds. Default: <c>1000</c>
        /// </param>
        /// <param name="minLogInterval">
        /// In case of multiple log events, it determines the time separation between log events, expressed in 
        /// milliseconds. Use 0 to disable. Default: <c>1000</c>.
        /// </param>
        /// <exception cref="NotSupportedException" />
        public AtomicMonotonicWinApiTimestampGenerator(
            int warningThreshold = DefaultWarningThreshold,
            int minLogInterval = DefaultMinLogInterval) : this(warningThreshold, minLogInterval, Logger)
        {
            
        }

        internal AtomicMonotonicWinApiTimestampGenerator(int warningThreshold, int minLogInterval, Logger logger)
            : base(warningThreshold, minLogInterval, logger)
        {
            // Try using Win 8+ API
            try
            {
                GetTimestamp();
            }
            catch (EntryPointNotFoundException ex)
            {
                throw new NotSupportedException("Win API method GetSystemTimePreciseAsFileTime() not supported", ex);
            }
        }
    }
}