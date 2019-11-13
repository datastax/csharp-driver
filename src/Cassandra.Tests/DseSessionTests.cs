// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
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