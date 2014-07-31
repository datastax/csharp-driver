using System;

namespace CqlPoco.IntegrationTests.Assertions
{
    public static class DateTimeOffsetExtensions
    {
        /// <summary>
        /// Truncates a DateTimeOffset down to the millisecond.  Useful since C* only stores Timestamps down to millisecond precision.
        /// </summary>
        public static DateTimeOffset TruncateToMillisecond(this DateTimeOffset dateTime)
        {
            return new DateTimeOffset(dateTime.Ticks - (dateTime.Ticks % TimeSpan.TicksPerMillisecond), dateTime.Offset);
        }

        /// <summary>
        /// Truncates a DateTimeOffset? down to the millisecond.  Useful since C* only stores Timestamps down to millisecond precision.
        /// </summary>
        public static DateTimeOffset? TruncateToMillisecond(this DateTimeOffset? dateTime)
        {
            if (dateTime.HasValue == false)
                return null;

            return dateTime.Value.TruncateToMillisecond();
        }
    }
}
