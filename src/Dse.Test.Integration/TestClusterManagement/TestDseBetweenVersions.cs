// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms

using System;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Dse.Test.Integration.TestClusterManagement
{
    /// <summary>
    /// An attribute that filters the test to execute according to the current DSE version.
    /// </summary>
    public class TestDseBetweenVersions : NUnitAttribute, IApplyToTest
    {
        public Version MinVersion { get; }

        public Version MaxVersion { get; }

        public TestDseBetweenVersions(int minMajor, int minMinor, int maxMajor, int maxMinor) :
            this(minMajor, minMinor, 0, maxMajor, maxMinor, 0)
        {
        }

        public TestDseBetweenVersions(int minMajor, int minMinor, int minBuild, int maxMajor, int maxMinor, int maxBuild)
        {
            MinVersion = new Version(minMajor, minMinor, minBuild);
            MaxVersion = new Version(maxMajor, maxMinor, maxBuild);
        }

        public void ApplyToTest(NUnit.Framework.Internal.Test test)
        {
            var executingVersion = TestClusterManager.DseVersion;
            if (!VersionMatch(MaxVersion, MinVersion, executingVersion))
            {
                test.RunState = RunState.Ignored;
                var message = string.Format("Test designed to run with DSE >= v{0} and <= v{1} (executing {2})", 
                    MinVersion,
                    MaxVersion,
                    executingVersion);
                test.Properties.Set("_SKIPREASON", message);
            }
        }

        private static bool VersionMatch(Version maxVersion, Version minVersion, Version executingVersion)
        {
            if (executingVersion >= minVersion && executingVersion <= maxVersion)
            {
                return true;
            }

            return false;
        }
    }
}