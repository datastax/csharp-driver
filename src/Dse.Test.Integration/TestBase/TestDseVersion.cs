using System;

namespace Cassandra.IntegrationTests.TestBase
{
    /// <summary>
    /// Quantifies the DSE version to determine whether a test should be run.
    /// </summary>
    public class TestDseVersion : Attribute
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        public int Build { get; set; }

        public Comparison Comparison { get; set; }

        /// <summary>
        /// Creates the TestDseVersion object
        /// </summary>
        /// <param name="comparisonOperator">Determines if the DSE version required should be "greater or equals to" = 1, "equals to" = 0, "less than or equal to " = -1</param>
        public TestDseVersion(int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
        {
            Major = major;
            Minor = minor;
            Build = 0;
            Comparison = comparison;
        }
    }

    public enum Comparison
    {
        LessThan = -1, 
        Equal = 0, 
        GreaterThanOrEqualsTo = 1
    };
}
