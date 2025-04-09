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
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using Moq;
using NUnit.Framework;
// ReSharper disable CompareOfFloatsByEqualityOperator

namespace Cassandra.Tests.Mapping.Linq
{
    [TestFixture]
    public class LinqStatementPropertiesTests : MappingTestBase
    {
        [Test]
        [TestCaseSource(typeof(IdempotenceTestCasesClass))]
        public async Task Should_ExecuteWithIdempotence_When_SetIdempotenceIsCalled(bool idempotence, TestCase<AllTypesEntity> testCase)
        {
            var session = GetSession((q, v) => { });
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            await testCase.Do(table, "SetIdempotence", new object[] { idempotence }).ConfigureAwait(false);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), It.IsAny<string>()), Times.Once);
            Mock.Get(session)
                .Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(statement => statement.IsIdempotent == idempotence), It.IsAny<string>()),
                    Times.Once);
        }

        [Test]
        [TestCaseSource(typeof(TestCasesClass))]
        public async Task Should_ExecuteWithReadTimeoutMillis_When_SetReadTimeoutMillisIsCalled(TestCase<AllTypesEntity> testCase)
        {
            var session = GetSession((q, v) => { });
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            await testCase.Do(table, "SetReadTimeoutMillis", new object[] { 5000 }).ConfigureAwait(false);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), It.IsAny<string>()), Times.Once);
            Mock.Get(session)
                .Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(statement => statement.ReadTimeoutMillis == 5000), It.IsAny<string>()),
                    Times.Once);
        }

        [Test]
        [TestCaseSource(typeof(TestCasesClass))]
        public async Task Should_ExecuteWithOutgoingPayload_When_SetOutgoingPayloadIsCalled(TestCase<AllTypesEntity> testCase)
        {
            var session = GetSession((q, v) => { });
            var map = new Map<AllTypesEntity>()
                      .ExplicitColumns()
                      .Column(t => t.DoubleValue, cm => cm.WithName("val"))
                      .Column(t => t.IntValue, cm => cm.WithName("id"))
                      .PartitionKey(t => t.IntValue)
                      .KeyspaceName("ks1")
                      .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(session, map);
            await testCase.Do(table, "SetOutgoingPayload", new object[] { new Dictionary<string, byte[]> { { "1", new byte[] { 5 } } } }).ConfigureAwait(false);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), It.IsAny<string>()), Times.Once);
            Mock.Get(session)
                .Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(statement => statement.OutgoingPayload != null && statement.OutgoingPayload["1"][0] == 5), It.IsAny<string>()),
                    Times.Once);
        }

        private static IEnumerable<TestCase<AllTypesEntity>> GetTestCases()
        {
            return new List<TestCase<AllTypesEntity>>
            {
                new TestCase<AllTypesEntity>
                {
                    Func = t => t.Select(_ => new AllTypesEntity { DoubleValue = 1.1D }).Where(a => a.IntValue == 1).UpdateIfExists(),
                    Name = "UpdateIfExists"
                },
                new TestCase<AllTypesEntity>
                {
                    Func = t => t.Select(_ => new AllTypesEntity { DoubleValue = 1.1D }).Where(a => a.IntValue == 1).UpdateIf(a => a.IntValue == 2),
                    Name = "UpdateIf"
                },
                new TestCase<AllTypesEntity>
                {
                    Func = t => t.Select(_ => new AllTypesEntity { DoubleValue = 1.1D }).Where(a => a.IntValue == 1).Update(),
                    Name = "Update"
                },
                new TestCase<AllTypesEntity>
                {
                    Func = t => t.Where(a => a.IntValue == 100).DeleteIf(a => a.IntValue == 100),
                    Name = "DeleteIf"
                },
                new TestCase<AllTypesEntity>
                {
                    Func = t => t.Where(a => a.IntValue == 100).Delete(),
                    Name = "Delete"
                },
                new TestCase<AllTypesEntity>
                {
                    Func = t => t.Insert(new AllTypesEntity()),
                    Name = "Insert"
                },
                new TestCase<AllTypesEntity>
                {
                    Func = t => t.Where(a => a.IntValue == 100).Select(a => new AllTypesEntity { DoubleValue = a.DoubleValue }),
                    Name = "Select"
                },
                new TestCase<AllTypesEntity>
                {
                    Func = t => t,
                    Name = "SelectAll"
                },
            };
        }

        private class IdempotenceTestCasesClass : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                var cases = GetTestCases();
                foreach (var c in cases)
                {
                    yield return new TestCaseData(true, c).SetName("{m}(true, " + c.Name + ")");
                    yield return new TestCaseData(false, c).SetName("{m}(false, " + c.Name + ")");
                }
            }
        }

        private class TestCasesClass : IEnumerable
        {
            public IEnumerator GetEnumerator()
            {
                var cases = GetTestCases();
                foreach (var c in cases)
                {
                    yield return new TestCaseData(c).SetName("{m}(" + c.Name + ")");
                    yield return new TestCaseData(c).SetName("{m}(" + c.Name + ")");
                }
            }
        }


        public class TestCase<TEntity>
        {
            public Func<Table<TEntity>, object> Func { get; set; }

            public string Name { get; set; }


            public Task Do(Table<TEntity> table, string method, object[] parameters)
            {
                var val = Func(table);
                var output = val.GetType().InvokeMember(method, BindingFlags.Public | BindingFlags.InvokeMethod | BindingFlags.Instance, null, val, parameters);
                var executeMethod = output.GetType().GetMethod("ExecuteAsync", new Type[] { });
                if (executeMethod != null)
                {
                    return (Task)executeMethod.Invoke(output, new object[] { });
                }

                // this means the method that was executed is from the Statement class and returns an IStatement so call ExecuteAsync on the previous output from the call chain
                // e.g.
                //      var query = table.Update();
                //      query.SetIdempotence(true);
                //      var result = query.Execute();
                // instead of
                //      var result = table.Update().SetIdempotence(true).Execute();
                // since SetIdempotence() is not defined at CqlUpdate or CqlCommand level, only at Statement level (as of August 2023)
                return (Task)val.GetType().InvokeMember("ExecuteAsync", BindingFlags.Public | BindingFlags.InvokeMethod | BindingFlags.Instance, null, val, new object[] { });
            }
        }
    }
}
