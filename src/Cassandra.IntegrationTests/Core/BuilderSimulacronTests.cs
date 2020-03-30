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

using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short)]
    internal class BuilderSimulacronTests : SharedSimulacronTests
    {
        public BuilderSimulacronTests() : base(1, false)
        {
        }

        [Test]
        public void Should_GenerateSessionNameCorrectly()
        {
            var cluster1 = GetNewCluster(b => b.WithSessionName("session-name"));
            var cluster2 = GetNewCluster(b => b.WithSessionName("s"));
            var cluster3 = GetNewCluster();

            var sessionWithName1 = cluster1.Connect();
            var sessionWithName2 = cluster1.Connect();

            var sessionWithSmallName1 = cluster2.Connect();
            var sessionWithSmallName2 = cluster2.Connect();

            var sessionWithoutName1 = cluster3.Connect();
            var sessionWithoutName2 = cluster3.Connect();

            Assert.AreEqual("session-name", sessionWithName1.SessionName);
            Assert.AreEqual("session-name1", sessionWithName2.SessionName);

            Assert.AreEqual("s", sessionWithSmallName1.SessionName);
            Assert.AreEqual("s1", sessionWithSmallName2.SessionName);

            Assert.AreEqual("s0", sessionWithoutName1.SessionName);
            Assert.AreEqual("s1", sessionWithoutName2.SessionName);
        }
    }
}