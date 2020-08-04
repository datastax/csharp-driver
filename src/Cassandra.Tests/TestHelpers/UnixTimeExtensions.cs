﻿/*
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

using System;

namespace Cassandra.Tests.TestHelpers
{
    /// <summary>
    /// Provides helper methods for working with times in Unix Epoch convention.
    /// </summary>
    internal static class UnixTimeExtensions
    {
        private const long EpochInTicks = 621355968000000000;

        /// <summary>
        /// Gets the seconds elapsed since the Unix Epoch (01-Jan-1970 UTC)
        /// </summary>
        /// <param name="source">The source time.</param>
        /// <returns>Returns the number whole and partial seconds elapsed since the Unix Epoch until the <paramref name="source"/> time.</returns>
        /// <exception cref="ArgumentException">Thrown if the <paramref name="source"/> Kind is <see cref="DateTimeKind.Unspecified"/>.</exception>
        public static double SecondsSinceUnixEpoch(this DateTime source)
        {
            if (source.Kind == DateTimeKind.Unspecified) throw new ArgumentException("DateTime must have kind specified.");
            return (source.ToUniversalTime().Ticks - UnixTimeExtensions.EpochInTicks) / (double)TimeSpan.TicksPerSecond;
        }

        /// <summary>
        /// Gets the milliseconds elapsed since the Unix Epoch (01-Jan-1970 UTC)
        /// </summary>
        /// <param name="source">The source time.</param>
        /// <returns>Returns the number whole milliseconds elapsed since the Unix Epoch until the <paramref name="source"/> time.</returns>
        /// <exception cref="ArgumentException">Thrown if the <paramref name="source"/> Kind is <see cref="DateTimeKind.Unspecified"/>.</exception>
        public static long MillisecondsSinceUnixEpoch(this DateTime source)
        {
            if (source.Kind == DateTimeKind.Unspecified) throw new ArgumentException("DateTime must have kind specified.");
            return (source.ToUniversalTime().Ticks - UnixTimeExtensions.EpochInTicks) / TimeSpan.TicksPerMillisecond;
        }

        /// <summary>
        /// Returns the date and time specified by the seconds since the Unix Epoch
        /// </summary>
        /// <param name="secondsSinceUnixEpoch">The seconds since epoch</param>
        /// <returns>A DateTime value in UTC kind.</returns>
        public static DateTimeOffset ToDateFromSecondsSinceEpoch(this double secondsSinceUnixEpoch)
        {
            var ticks = (long)(secondsSinceUnixEpoch * TimeSpan.TicksPerSecond);
            return new DateTime(UnixTimeExtensions.EpochInTicks + ticks, DateTimeKind.Utc);
        }

        /// <summary>
        /// Returns the date and time specified by the milliseconds since the Unix Epoch
        /// </summary>
        /// <param name="millisecondsSinceUnixEpoch">The milliseconds since epoch</param>
        /// <returns>A DateTime value in UTC kind.</returns>
        public static DateTimeOffset ToDateFromMillisecondsSinceEpoch(this long millisecondsSinceUnixEpoch)
        {
            var ticks = millisecondsSinceUnixEpoch * TimeSpan.TicksPerMillisecond;
            return new DateTime(UnixTimeExtensions.EpochInTicks + ticks, DateTimeKind.Utc);
        }
    }
}