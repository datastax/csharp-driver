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

        private bool IsOssRequired { get; set; }

        //static TestDseVersion()
        //{
        //    Environment.SetEnvironmentVariable("CCM_USE_WSL", "true");
        //    Environment.SetEnvironmentVariable("DSE_VERSION", "6.7.7");
        //}

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
        /// <param name="isOssRequired">Whether OSS C* is required.</param>
        public TestDseVersion(int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo, bool isOssRequired = false)
            : this(major, minor, 0, comparison, isOssRequired)
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
        /// <param name="isOssRequired">Whether OSS C* is required.</param>
        public TestDseVersion(int major, int minor, int build, Comparison comparison = Comparison.GreaterThanOrEqualsTo, bool isOssRequired = false)
        {
            Major = major;
            Minor = minor;
            Build = build;
            Comparison = comparison;
            IsOssRequired = isOssRequired;
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
            if (!TestDseVersion.VersionMatch(expectedVersion, IsDseRequired(), IsOssRequired, Comparison, out var msg))
            {
                test.RunState = RunState.Ignored;
                var message = msg;
                test.Properties.Set("_SKIPREASON", message);
            }
        }

        public static bool VersionMatch(Version expectedVersion, bool requiresDse, bool requiresOss, Comparison comparison, out string message)
        {
            if (TestClusterManager.IsDse && requiresOss)
            {
                message = string.Format("Test designed to run with OSS {0} v{1} (executing DSE {2})", 
                    TestDseVersion.GetComparisonText(comparison), 
                    expectedVersion, 
                    TestClusterManager.DseVersion);
                return false;
            }

            if (!TestClusterManager.IsDse && requiresDse)
            {
                message = $"Test designed to run with DSE {TestDseVersion.GetComparisonText(comparison)} " +
                          $"v{expectedVersion} (executing OSS {TestClusterManager.CassandraVersion})";
                return false;
            }

            var executingVersion = TestClusterManager.IsDse ? TestClusterManager.DseVersion : TestClusterManager.CassandraVersion;
            if (!TestDseVersion.VersionMatch(expectedVersion, executingVersion, comparison))
            {
                message =
                    $"Test designed to run with DSE {TestDseVersion.GetComparisonText(comparison)} v{expectedVersion} (executing {executingVersion})";
                return false;
            }

            message = null;
            return true;
        }

        public static bool VersionMatch(Version expectedVersion, bool requiresDse, bool requiresOss, Comparison comparison)
        {
            return TestDseVersion.VersionMatch(expectedVersion, requiresDse, requiresOss, comparison, out _);
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

    /// <summary>
    /// An attribute that filters the test to execute according to the current Cassandra version of the DSE version.
    /// </summary>
    public class TestCassandraVersion : TestDseVersion
    {
        protected override Version GetExpectedServerVersion()
        {
            var version = new Version(Major, Minor, Build);
            return TestClusterManager.IsDse
                ? TestClusterManager.GetDseVersionFromCassandraVersion(version)
                : version;
        }

        protected override bool IsDseRequired()
        {
            return false;
        }

        public TestCassandraVersion(
            int major, int minor, Comparison comparison = Comparison.GreaterThanOrEqualsTo, bool isOssRequired = false) : base(major, minor, comparison, isOssRequired)
        {
        }

        public TestCassandraVersion(int major, int minor, int build, Comparison comparison = Comparison.GreaterThanOrEqualsTo, bool isOssRequired = false) : base(major, minor, build, comparison, isOssRequired)
        {
        }
    }

    public class TestBothServersVersion : TestDseVersion
    {
        public TestBothServersVersion(
            int cassandraVersionMajor, 
            int cassandraVersionMinor, 
            int dseVersionMajor, 
            int dseVersionMinor, 
            Comparison comparison = Comparison.GreaterThanOrEqualsTo) 
            : this(
                TestClusterManager.IsDse 
                    ? new Version(dseVersionMajor, dseVersionMinor) 
                    : new Version(cassandraVersionMajor, cassandraVersionMinor), 
                comparison)
        {
        }

        private TestBothServersVersion(Version version, Comparison comparison) 
            : base(version.Major, version.Minor, comparison)
        {
        }
        
        protected override bool IsDseRequired()
        {
            return TestClusterManager.IsDse;
        }
    }

    public enum Comparison
    {
        LessThan = -1,
        Equal = 0,
        GreaterThanOrEqualsTo = 1
    }
}
