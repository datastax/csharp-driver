//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Test.Integration.TestClusterManagement
{
    /// <summary>
    /// Quantifies the Cassandra version to determine whether a test should be run.
    /// </summary>
    public class TestCassandraVersion : Attribute
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        public int Build { get; set; }

        public Comparison Comparison { get; set; }

        /// <summary>
        /// Creates the TestCassandraVersion object
        /// </summary>
        /// <param name="comparison">Determines if the Cassandra version required should be "greater or equals to" = 1, "equals to" = 0, "less than or equal to " = -1</param>
        public TestCassandraVersion(int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
        {
            Major = major;
            Minor = minor;
            Build = 0;
            Comparison = comparison;
        }

        /// <summary>
        /// Creates the TestCassandraVersion object with the option of specifying a Build
        /// </summary>
        /// <param name="comparison">Determines if the Cassandra version required should be "greater or equals to" = 1, "equals to" = 0, "less than or equal to " = -1</param>
        public TestCassandraVersion(int major, int minor, int build, Comparison comparison = Comparison.GreaterThanOrEqualsTo) : this(major, minor, comparison)
        {
            Build = build;
        }
    }

    public enum Comparison
    {
        LessThan = -1, 
        Equal = 0, 
        GreaterThanOrEqualsTo = 1
    };
}
