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
    /// Represents client-side, microsecond-precision query timestamps.
    /// <para>
    /// Given that Apache Cassandra uses those timestamps to resolve conflicts, implementations should generate
    /// monotonically increasing timestamps for successive invocations of <see cref="Next()"/>.
    /// </para>
    /// </summary>
    public interface ITimestampGenerator
    {
        /// <summary>
        /// Returns the next timestamp in microseconds since UNIX epoch.
        /// <para>
        /// Implementers should enforce increasing monotonic timestamps, that is,
        /// a timestamp returned should always be strictly greater that any previously returned
        /// timestamp.
        /// </para>
        /// <para>
        /// Implementers should strive to achieve microsecond precision in the best possible way,
        /// which is usually largely dependent on the underlying operating system's capabilities.
        /// </para>
        /// </summary>
        /// <returns>
        /// The next timestamp (in microseconds). When returning <see cref="long.MinValue"/>, the driver
        /// will not set the timestamp, letting Apache Cassandra generate a server-side timestamp.
        /// </returns>
        long Next();
    }
}
