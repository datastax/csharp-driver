using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents client-side, microsecond-precision query timestamps.
    /// <para>
    /// Given that Apache Cassandra uses those timestamps to resolve conflicts, implementations should generate
    /// monotonically increasing timestamps for successive invocations of <see cref="Next()"/>.
    /// </para>
    /// </summary>
    public interface ITimestampGenerator : IDisposable
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
