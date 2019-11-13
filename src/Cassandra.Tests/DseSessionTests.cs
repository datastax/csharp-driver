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
using Cassandra.SessionManagement;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class DseSessionTests
    {
        [Test]
        public void Should_GenerateNewSessionId_When_SessionIsCreated()
        {
            using (var cluster = DseCluster.Builder().AddContactPoint("127.0.0.1").Build())
            {
                var target = new DseSession(Mock.Of<IInternalSession>(), cluster);
                var sessionId = target.InternalSessionId;

                var newTarget = new DseSession(Mock.Of<IInternalSession>(), cluster);
                var newSessionId = newTarget.InternalSessionId;
                Assert.AreNotEqual(Guid.Empty, sessionId);
                Assert.AreNotEqual(sessionId, newSessionId);
            }
        }
    }
}