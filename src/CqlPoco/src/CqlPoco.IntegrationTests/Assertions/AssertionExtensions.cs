using FluentAssertions.Equivalency;

namespace CqlPoco.IntegrationTests.Assertions
{
    /// <summary>
    /// Some extensions methods for Fluent Assertions.
    /// </summary>
    public static class AssertionExtensions
    {
        /// <summary>
        /// Tells fluent assertions to account for C* only storing timestamps down to millisecond precision when comparing
        /// objects for equivalency.
        /// </summary>
        public static EquivalencyAssertionOptions<T> AccountForTimestampAccuracy<T>(this EquivalencyAssertionOptions<T> options)
        {
            return options.Using(CassandraDateTimeEquality.Instance)
                          .Using(CassandraDateTimeOffsetEquality.Instance);
        }
    }
}