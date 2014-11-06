using System;
using FluentAssertions;
using FluentAssertions.Common;
using FluentAssertions.Equivalency;

namespace CqlPoco.IntegrationTests.Assertions
{
    /// <summary>
    /// An assertion rule for dealing with comparing DateTimes retrieved from C*.  Since C* stores Timestamps using an accuracy
    /// down to the millisecond (i.e. truncates everything below that) we want to take that into account when comparing them.
    /// </summary>
    public class CassandraDateTimeEquality : AssertionRule<DateTime?>
    {
        public static CassandraDateTimeEquality Instance { get; private set; }

        static CassandraDateTimeEquality()
        {
            Instance = new CassandraDateTimeEquality();
        }

        private CassandraDateTimeEquality()
            : base(s => IsDateTime(s), ShouldBeWithinOneMillisecond)
        {
        }

        private static bool IsDateTime(ISubjectInfo subject)
        {
            return subject.RuntimeType.IsSameOrInherits(typeof(DateTime?));
        }

        private static void ShouldBeWithinOneMillisecond(IAssertionContext<DateTime?> context)
        {
            // Cassandra Timestamps are recorded with an accuracy down to the millisecond
            if (context.Expectation.HasValue)
                context.Subject.Should().BeCloseTo(context.Expectation.Value, 1);
            else
                context.Subject.Should().NotHaveValue();
        }
    }
}