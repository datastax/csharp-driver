using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// It determines the Cassandra version required to execute the test
    /// </summary>
    public class TestCassandraVersion : Attribute
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        public Comparison Comparison { get; set; }

        /// <summary>
        /// It determines Cassandra version required to execute
        /// </summary>
        /// <param name="comparisonOperator">Determines if the Cassandra version required should be "greater or equals to" = 1, "equals to" = 0, "less than or equal to " = -1</param>
        public TestCassandraVersion(int mayor, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
        {
            this.Major = mayor;
            this.Minor = minor;
            this.Comparison = comparison;
        }
    }

    public enum Comparison
    {
        LessThan = -1, 
        Equal = 0, 
        GreaterThanOrEqualsTo = 1
    };
}
