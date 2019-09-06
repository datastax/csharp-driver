// 
//       Copyright (C) DataStax Inc.
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

using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class PreparedStatementTests
    {
        [Test] 
        public void Should_Allow_Bind_With_Empty_Query() 
        { 
            var ps = new PreparedStatement();
            Assert.NotNull(ps.Bind());
        }

        [Test] 
        public void Bind_Should_Create_BoundStatement_With_Provided_Values() 
        {
            var ps = new PreparedStatement();
            ps.SetRoutingKey(new RoutingKey(new byte[] {0x01}));
            ps.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            ps.SetIdempotence(true);
            var statement = ps.Bind();
            Assert.AreEqual(ConsistencyLevel.LocalQuorum, statement.ConsistencyLevel);
            Assert.AreSame(ps.RoutingKey, statement.RoutingKey);
            Assert.True(statement.IsIdempotent);
        }

        [Test] 
        public void Bind_Should_Create_BoundStatement_With_Default_Values() 
        {
            var statement = new PreparedStatement().Bind();
            Assert.Null(statement.ConsistencyLevel);
            Assert.Null(statement.RoutingKey);
            Assert.Null(statement.IsIdempotent);
        }
    }
}