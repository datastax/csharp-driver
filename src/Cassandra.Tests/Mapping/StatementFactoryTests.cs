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
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(q => Task.FromResult(GetPrepared(q)));

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
        public async Task GetStatementAsync_Should_Reprepare_Each_Time_After_Failed_Attempt()
        {
            var preparationFails = 1;

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(q =>
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
    }
}