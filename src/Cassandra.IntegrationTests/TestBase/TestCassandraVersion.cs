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
    /// An attribute that filters the test to execute according to the current version.
    /// </summary>
    public class TestCassandraVersion : NUnitAttribute, IApplyToTest
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        public int Build { get; set; }

        public Comparison Comparison { get; set; }

        private bool IsOssRequired { get; set; }

        /// <summary>
        /// Creates an instance of an attribute that filters the test to execute according to the current Cassandra version
        /// being used.
        /// </summary>
        /// <param name="major">Major version</param>
        /// <param name="minor">Minor version</param>
        /// <param name="comparison">
        /// Determines if the Cassandra version required should be "greater or equals to" = 1,
        /// "equals to" = 0, "less than or equal to " = -1
        /// </param>
        public TestCassandraVersion(int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo, bool isOssRequired = false)
            : this(major, minor, 0, comparison)
        {

        }

        /// <summary>
        /// Creates an instance of an attribute that filters the test to execute according to the current Cassandra version
        /// being used.
        /// </summary>
        /// <param name="major">Major version</param>
        /// <param name="minor">Minor version</param>
        /// <param name="build">Build version</param>
        /// <param name="comparison">
        /// Determines if the Cassandra version required should be "greater or equals to" = 1,
        /// "equals to" = 0, "less than or equal to " = -1
        /// </param>
        /// <param name="isOssRequired">Whether OSS C* is required.</param>
        public TestCassandraVersion(int major, int minor, int build, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
        {
            Major = major;
            Minor = minor;
            Build = build;
            Comparison = comparison;
        }

        public void ApplyToTest(NUnit.Framework.Internal.Test test)
        {
            if (!Applies(out string msg))
            {
                test.RunState = RunState.Ignored;
                var message = msg;
                test.Properties.Set("_SKIPREASON", message);
            }
        }

        public bool Applies(out string msg)
        {
            var expectedVersion = new Version(Major, Minor, Build);
            return TestCassandraVersion.VersionMatch(expectedVersion, Comparison, out msg);
        }

        public static bool VersionMatch(Version expectedVersion, Comparison comparison, out string message)
        {
            if (TestClusterManager.IsScylla)
            {
                var scyllaExecutingVersion = TestClusterManager.ScyllaVersion;
                if (expectedVersion.Major >= 4)
                {
                    message =
                            $"Test designed to run with Cassandra {TestCassandraVersion.GetComparisonText(comparison)} v{expectedVersion} (executing Scylla v{scyllaExecutingVersion})";
                    return false;
                }
                if (!TestCassandraVersion.VersionMatch(expectedVersion, new Version(3, 10), comparison))
                {
                    message =
                        $"Test designed to run with Cassandra {TestCassandraVersion.GetComparisonText(comparison)} v{expectedVersion} (executing Scylla v{scyllaExecutingVersion})";
                    return false;
                }

                message = null;
                return true;
            }

            var executingVersion = TestClusterManager.CassandraVersion;
            if (!TestCassandraVersion.VersionMatch(expectedVersion, executingVersion, comparison))
            {
                message = $"Test designed to run with Cassandra {TestCassandraVersion.GetComparisonText(comparison)} v{expectedVersion} (executing {executingVersion})";
                return false;
            }

            message = null;
            return true;
        }

        public static bool VersionMatch(Version expectedVersion, Comparison comparison)
        {
            return TestCassandraVersion.VersionMatch(expectedVersion, comparison, out _);
        }

        public static bool VersionMatch(Version expectedVersion, Version executingVersion, Comparison comparison)
        {
            expectedVersion = AdaptVersion(expectedVersion);
            executingVersion = AdaptVersion(executingVersion);

            var comparisonResult = (Comparison)executingVersion.CompareTo(expectedVersion);

            if (comparisonResult >= Comparison.Equal && comparison == Comparison.GreaterThanOrEqualsTo)
            {
                return true;
            }
            return comparisonResult == comparison;
        }

        /// <summary>
        /// Replace -1 (undefined) with 0 on the version string.
        /// </summary>
        private static Version AdaptVersion(Version v)
        {
            var minor = v.Minor;
            if (minor < 0)
            {
                minor = 0;
            }

            var build = v.Build;
            if (build < 0)
            {
                build = 0;
            }

            var revision = v.Revision;
            if (revision < 0)
            {
                revision = 0;
            }

            return new Version(v.Major, minor, build, revision);
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

    public enum Comparison
    {
        LessThan = -1,
        Equal = 0,
        GreaterThanOrEqualsTo = 1
    }
}
