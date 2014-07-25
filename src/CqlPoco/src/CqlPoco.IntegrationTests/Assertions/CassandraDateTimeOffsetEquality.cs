using System;
using FluentAssertions;
using FluentAssertions.Common;
using FluentAssertions.Equivalency;

namespace CqlPoco.IntegrationTests.Assertions
{
    /// <summary>
    /// An assertion rule for dealing with comparing DateTimeOffsets retrieved from C*.  Since C* stores Timestamps using an accuracy
    /// down to the millisecond (i.e. truncates everything below that) we want to take that into account when comparing them.
    /// </summary>
    public class CassandraDateTimeOffsetEquality : AssertionRule<DateTimeOffset?>
    {
        public static CassandraDateTimeOffsetEquality Instance { get; private set; }

        static CassandraDateTimeOffsetEquality()
        {
            Instance = new CassandraDateTimeOffsetEquality();
        }

        private CassandraDateTimeOffsetEquality()
            : base(s => IsDateTimeOffset(s), ShouldBeWithinOneMillisecond)
        {
        }

        private static bool IsDateTimeOffset(ISubjectInfo subject)
        {
            return subject.RuntimeType.IsSameOrInherits(typeof (DateTimeOffset?));
        }

        private static void ShouldBeWithinOneMillisecond(IAssertionContext<DateTimeOffset?> context)
        {
            // Cassandra Timestamps are recorded with an accuracy down to the millisecond
            if (context.Expectation.HasValue)
                context.Subject.Should().BeCloseTo(context.Expectation.Value, 1);
            else
                context.Subject.Should().NotHaveValue();
        }
    }
}
