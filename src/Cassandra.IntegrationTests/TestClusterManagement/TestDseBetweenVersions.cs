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
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Cassandra.IntegrationTests.TestClusterManagement
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