// 
//       Copyright DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Threading;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;
using Cassandra.Tasks;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class StatementFactoryTests : MappingTestBase
    {
        [Test]
        public async Task GetStatementAsync_Should_Prepare_Once_And_Cache()
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((q, profile) => Task.FromResult(GetPrepared(q)));

            var cql = Cql.New("Q");
            var sf = new StatementFactory();

            var statement = await sf.GetStatementAsync(sessionMock.Object, cql).ConfigureAwait(false);

            Assert.IsInstanceOf<BoundStatement>(statement);

            var ps = ((BoundStatement) statement).PreparedStatement;

            for (var i = 0; i < 10; i++)
            {
                var bound = (BoundStatement) await sf.GetStatementAsync(sessionMock.Object, cql).ConfigureAwait(false);
                Assert.AreSame(ps, bound.PreparedStatement);
            }
        }

        [Test]
        public async Task GetStatementAsync_Should_Cache_Based_On_Query_Keyspace_And_Session_Instance()
        {
            var sessionMock1 = new Mock<ISession>(MockBehavior.Strict);
            sessionMock1.Setup(s => s.Keyspace).Returns("ks1");
            sessionMock1
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((q, profile) => Task.FromResult(GetPrepared(q)));

            var sessionMock2 = new Mock<ISession>(MockBehavior.Strict);
            sessionMock2.Setup(s => s.Keyspace).Returns("ks2");
            sessionMock2
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((q, profile) => Task.FromResult(GetPrepared(q)));

            var cql1A = Cql.New("Q1");
            var cql1B = Cql.New("Q1");
            var cql2 = Cql.New("Q2");
            var sf = new StatementFactory();

            var statement = await sf.GetStatementAsync(sessionMock1.Object, cql1A).ConfigureAwait(false);
            var ps1 = GetPreparedStatement(statement);

            for (var i = 0; i < 10; i++)
            {
                var bound1 = await sf.GetStatementAsync(sessionMock1.Object, cql1B)
                                    .ConfigureAwait(false);
                Assert.AreSame(ps1, GetPreparedStatement(bound1));
            }

            var bound2 = await sf.GetStatementAsync(sessionMock1.Object, cql2).ConfigureAwait(false);
            // Different CQL should be cached differently
            Assert.AreNotSame(ps1, GetPreparedStatement(bound2));

            sessionMock1.Setup(s => s.Keyspace).Returns("ks2");

            // Different keyspace, same query
            var differentKsStatement = await sf.GetStatementAsync(sessionMock1.Object, cql1A).ConfigureAwait(false);
            var psSession1 = GetPreparedStatement(differentKsStatement);
            Assert.AreNotSame(ps1, psSession1);

            // Different Session instance
            var boundSession2 = await sf.GetStatementAsync(sessionMock2.Object, cql1A).ConfigureAwait(false);
            var psSession2 = GetPreparedStatement(boundSession2);
            Assert.AreNotSame(psSession1, psSession2);

            // Same ks, query and ISession instance
            var bound3 = await sf.GetStatementAsync(sessionMock2.Object, cql1A).ConfigureAwait(false);
            Assert.AreSame(psSession2, GetPreparedStatement(bound3));
        }

        [Test]
        public async Task GetStatementAsync_Should_Reprepare_Each_Time_After_Failed_Attempt()
        {
            var preparationFails = 1;

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((q, profile) =>
                {
                    if (Volatile.Read(ref preparationFails) == 1)
                    {
                        throw new InvalidQueryException("Test temporal invalid query");
                    }

                    return Task.FromResult(GetPrepared(q));
                });

            var sf = new StatementFactory();

            var cql = Cql.New("Q");
            Parallel.For(0, 4, _ =>
            {
                Assert.Throws<InvalidQueryException>(() =>
                    TaskHelper.WaitToComplete(sf.GetStatementAsync(sessionMock.Object, cql)));
            });

            Interlocked.Exchange(ref preparationFails, 0);

            // Should not fail after setting the flag
            var statement = await sf.GetStatementAsync(sessionMock.Object, cql).ConfigureAwait(false);

            Assert.IsInstanceOf<BoundStatement>(statement);

            var ps = ((BoundStatement) statement).PreparedStatement;

            for (var i = 0; i < 10; i++)
            {
                var bound = (BoundStatement) await sf.GetStatementAsync(sessionMock.Object, cql).ConfigureAwait(false);
                Assert.AreSame(ps, bound.PreparedStatement);
            }
        }

        private static PreparedStatement GetPreparedStatement(Statement statement)
        {
            Assert.IsInstanceOf<BoundStatement>(statement);
            return ((BoundStatement) statement).PreparedStatement;
        }
    }
}