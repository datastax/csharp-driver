using System;

namespace CqlPoco.IntegrationTests.Assertions
{
    public static class DateTimeOffsetExtensions
    {
        /// <summary>
        /// Gets a DateTimeOffset down to millisecond precision using the same method that C* driver does when storing timestamps.
        /// </summary>
        public static DateTimeOffset ToMillisecondPrecision(this DateTimeOffset dateTime)
        {
            return new DateTimeOffset(dateTime.Ticks - (dateTime.Ticks % TimeSpan.TicksPerMillisecond), dateTime.Offset);
        }

        /// <summary>
        /// Gets a nullable DateTimeOffset down to millisecond precision using the same method that C* driver does when storing timestamps.
        /// </summary>
        public static DateTimeOffset? ToMillisecondPrecision(this DateTimeOffset? dateTime)
        {
            if (dateTime.HasValue == false)
                return null;

            return dateTime.Value.ToMillisecondPrecision();
        }
    }
}
