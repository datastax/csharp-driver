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

using Cassandra.DataStax.Insights;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    /// <summary>
    /// An attribute that filters the test to execute according to whether the current DSE version supports insights.
    /// </summary>
    public class TestInsightsVersion : NUnitAttribute, IApplyToTest
    {
        public void ApplyToTest(NUnit.Framework.Internal.Test test)
        {
            if (!TestClusterManager.IsDse)
            {
                test.RunState = RunState.Ignored;
                test.Properties.Set("_SKIPREASON", $"Test designed to run with DSE version that supports Insights (executing OSS {TestClusterManager.CassandraVersion})");
                return;
            }

            var executingVersion = TestClusterManager.DseVersion;
            var insightsSupportVerifier = new InsightsSupportVerifier();
            if (insightsSupportVerifier.DseVersionSupportsInsights(executingVersion))
            {
                return;
            }

            test.RunState = RunState.Ignored;
            var message = $"Test designed to run with DSE version that supports Insights (executing {executingVersion})";
            test.Properties.Set("_SKIPREASON", message);
        }
    }
}