//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Dse.Test.Integration.TestClusterManagement
{
    /// <summary>
    /// An attribute that filters the test to execute according to the current DSE version.
    /// </summary>
    public class TestDseVersion : NUnitAttribute, IApplyToTest
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        public int Build { get; set; }

        public Comparison Comparison { get; set; }

        /// <summary>
        /// Creates an instance of an attribute that filters the test to execute according to the current DSE version
        /// being used.
        /// </summary>
        /// <param name="major">Major version</param>
        /// <param name="minor">Minor version</param>
        /// <param name="comparison">
        /// Determines if the DSE version required should be "greater or equals to" = 1,
        /// "equals to" = 0, "less than or equal to " = -1
        /// </param>
        public TestDseVersion(int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
            : this(major, minor, 0, comparison)
        {

        }

        /// <summary>
        /// Creates an instance of an attribute that filters the test to execute according to the current DSE version
        /// being used.
        /// </summary>
        /// <param name="major">Major version</param>
        /// <param name="minor">Minor version</param>
        /// <param name="build">Build version</param>
        /// <param name="comparison">
        /// Determines if the DSE version required should be "greater or equals to" = 1,
        /// "equals to" = 0, "less than or equal to " = -1
        /// </param>
        public TestDseVersion(int major, int minor, int build, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
        {
            Major = major;
            Minor = minor;
            Build = build;
            Comparison = comparison;
        }

        /// <summary>
        /// Gets the DSE version that should be used to compare against the running version.
        /// </summary>
        protected virtual Version GetExpectedDseVersion()
        {
            return new Version(Major, Minor);
        }

        public void ApplyToTest(NUnit.Framework.Internal.Test test)
        {
            var executingVersion = TestClusterManager.DseVersion;
            var expectedVersion = GetExpectedDseVersion();
            if (!VersionMatch(expectedVersion, executingVersion, Comparison))
            {
                test.RunState = RunState.Ignored;
                var message = string.Format("Test designed to run with DSE {0} v{1} (executing {2})", 
                    GetComparisonText(Comparison), 
                    expectedVersion, 
                    executingVersion);
                test.Properties.Set("_SKIPREASON", message);
            }
        }

        public static bool VersionMatch(Version expectedVersion, Version executingVersion, Comparison comparison)
        {
            //Compare them as integers
            //var expectedVersion = new Version(versionAttr.Major, versionAttr.Minor, versionAttr.Build);
            var comparisonResult = (Comparison)executingVersion.CompareTo(expectedVersion);

            if (comparisonResult >= Comparison.Equal && comparison == Comparison.GreaterThanOrEqualsTo)
            {
                return true;
            }
            return comparisonResult == comparison;
        }

        private static string GetComparisonText(Comparison comparison)
        {
            string result;
            switch (comparison)
            {
                case Comparison.GreaterThanOrEqualsTo:
                    result = "greater than or equals to";
                    break;
                case Comparison.LessThan:
                    result = "lower than";
                    break;
                default:
                    result = "equals to";
                    break;
            }
            return result;
        }
    }

    /// <summary>
    /// An attribute that filters the test to execute according to the current Cassandra version of the DSE version.
    /// </summary>
    public class TestCassandraVersion : TestDseVersion
    {
        protected override Version GetExpectedDseVersion()
        {
            return TestClusterManager.GetDseVersion(new Version(Major, Minor, Build));
        }

        public TestCassandraVersion(int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo) : base(major, minor, comparison)
        {
        }

        public TestCassandraVersion(int major, int minor, int build, Comparison comparison = Comparison.GreaterThanOrEqualsTo) : base(major, minor, build, comparison)
        {
        }
    }

    public enum Comparison
    {
        LessThan = -1,
        Equal = 0,
        GreaterThanOrEqualsTo = 1
    }
}
