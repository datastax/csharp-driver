//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Cassandra.IntegrationTests.TestBase
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
        protected virtual Version GetExpectedServerVersion()
        {
            return new Version(Major, Minor, Build);
        }

        protected virtual bool IsDseRequired()
        {
            return true;
        }

        public void ApplyToTest(NUnit.Framework.Internal.Test test)
        {
            var expectedVersion = GetExpectedServerVersion();
            if (!TestClusterManager.IsDse && IsDseRequired())
            {
                test.RunState = RunState.Ignored;
                var message = string.Format("Test designed to run with DSE {0} v{1} (executing OSS {2})", 
                    TestDseVersion.GetComparisonText(Comparison), 
                    expectedVersion, 
                    TestClusterManager.CassandraVersion);
                test.Properties.Set("_SKIPREASON", message);
                return;
            }

            var executingVersion = TestClusterManager.IsDse ? TestClusterManager.DseVersion : TestClusterManager.CassandraVersion;
            if (!TestDseVersion.VersionMatch(expectedVersion, executingVersion, Comparison))
            {
                test.RunState = RunState.Ignored;
                var message = string.Format("Test designed to run with DSE {0} v{1} (executing {2})", 
                    TestDseVersion.GetComparisonText(Comparison), 
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
        protected override Version GetExpectedServerVersion()
        {
            return TestClusterManager.IsDse
                ? TestClusterManager.GetDseVersion(new Version(Major, Minor, Build))
                : new Version(Major, Minor, Build);
        }

        protected override bool IsDseRequired()
        {
            return false;
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
