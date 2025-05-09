using System;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Cassandra.IntegrationTests.TestBase
{
    public class TestScyllaVersion : NUnitAttribute, IApplyToTest
    {
        public int Major { get; set; }

        public int Minor { get; set; }

        public int Build { get; set; }

        public Comparison Comparison { get; set; }

        public TestScyllaVersion(int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
            : this(major, minor, 0, comparison)
        {

        }

        public TestScyllaVersion(int major, int minor, int build, Comparison comparison = Comparison.GreaterThanOrEqualsTo)
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
            return TestScyllaVersion.VersionMatch(expectedVersion, Comparison, out msg);
        }

        public static bool VersionMatch(Version expectedVersion, Comparison comparison, out string message)
        {
            if (!TestClusterManager.IsScylla)
            {
                message = $"Test designed to run with Scylla {TestScyllaVersion.GetComparisonText(comparison)} v{expectedVersion}";
                return false;
            }

            var executingVersion = TestClusterManager.ScyllaVersion;
            if (!TestScyllaVersion.VersionMatch(expectedVersion, executingVersion, comparison))
            {
                message =
                        $"Test designed to run with Scylla {TestScyllaVersion.GetComparisonText(comparison)} v{expectedVersion} (executing {executingVersion})";
                return false;
            }

            message = null;
            return true;
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
}
