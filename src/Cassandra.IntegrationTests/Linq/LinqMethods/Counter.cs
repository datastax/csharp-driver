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
using System.Collections.Generic;
using System.Linq;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.When;
using Cassandra.Mapping;

using NUnit.Framework;

#pragma warning disable 618
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    public class Counter : SimulacronTest
    {
        private void PrimeLinqCounterQuery(CounterEntityWithLinqAttributes counter)
        {
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"Counter\", \"KeyPart1\", \"KeyPart2\" " +
                          "FROM \"CounterEntityWithLinqAttributes\" " +
                          "WHERE \"KeyPart1\" = ? AND \"KeyPart2\" = ?",
                          when => counter.WithParams(when, "KeyPart1", "KeyPart2"))
                      .ThenRowsSuccess(counter.CreateRowsResult()));
        }

        private RowsResult AddRows(IEnumerable<CounterEntityWithLinqAttributes> counters)
        {
            return counters.Aggregate(CounterEntityWithLinqAttributes.GetEmptyRowsResult(), (current, c) => c.AddRow(current));
        }

        private void PrimeLinqCounterRangeQuery(
            IEnumerable<CounterEntityWithLinqAttributes> counters,
            string tableName = "CounterEntityWithLinqAttributes",
            bool caseSensitive = true)
        {
            var cql = caseSensitive
                ? $"SELECT \"Counter\", \"KeyPart1\", \"KeyPart2\" FROM \"{tableName}\""
                : $"SELECT Counter, KeyPart1, KeyPart2 FROM {tableName}";

            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(b => b.WhenQuery(cql).ThenRowsSuccess(AddRows(counters)));
        }

        [Test]
        public void LinqAttributes_Counter_SelectRange()
        {
            //var mapping = new Map<PocoWithCounter>();
            var mappingConfig = new MappingConfiguration();
            mappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(CounterEntityWithLinqAttributes),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(CounterEntityWithLinqAttributes)));
            var table = new Table<CounterEntityWithLinqAttributes>(Session, mappingConfig);
            table.Create();

            var expectedCounters = new List<CounterEntityWithLinqAttributes>();
            for (var i = 0; i < 10; i++)
            {
                expectedCounters.Add(
                    new CounterEntityWithLinqAttributes
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = Guid.NewGuid().GetHashCode(),
                        Counter = Guid.NewGuid().GetHashCode()
                    });
            }

            PrimeLinqCounterRangeQuery(expectedCounters);

            var countersQueried = table.Select(m => m).Execute().ToList();
            Assert.AreEqual(10, countersQueried.Count);
            foreach (var expectedCounter in expectedCounters)
            {
                var actualCounter = countersQueried.Single(c => c.KeyPart1 == expectedCounter.KeyPart1);
                Assert.AreEqual(expectedCounter.KeyPart2, actualCounter.KeyPart2);
                Assert.AreEqual(expectedCounter.Counter, actualCounter.Counter);
            }
        }

        /// <summary>
        /// Validate expected error message when attempting to insert a row that contains a counter
        /// </summary>
        [Test]
        public void LinqAttributes_Counter_AttemptInsert()
        {
            // Create config that uses linq based attributes
            var mappingConfig = new MappingConfiguration();
            mappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(CounterEntityWithLinqAttributes),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(CounterEntityWithLinqAttributes)));
            var table = new Table<CounterEntityWithLinqAttributes>(Session, mappingConfig);

            CounterEntityWithLinqAttributes pocoAndLinqAttributesLinqPocos = new CounterEntityWithLinqAttributes()
            {
                KeyPart1 = Guid.NewGuid(),
                KeyPart2 = (decimal)123,
            };

            var expectedErrMsg = "INSERT statement(s)? are not allowed on counter tables, use UPDATE instead";

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "INSERT INTO \"CounterEntityWithLinqAttributes\" (\"Counter\", \"KeyPart1\", \"KeyPart2\") VALUES (?, ?, ?)",
                          when => pocoAndLinqAttributesLinqPocos.WithParams(when))
                     .ThenServerError(
                         ServerError.Invalid, expectedErrMsg));

            var e = Assert.Throws<InvalidQueryException>(() => Session.Execute(table.Insert(pocoAndLinqAttributesLinqPocos)));
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        [TestCase(-21)]
        [TestCase(-13)]
        [TestCase(-8)]
        [TestCase(-5)]
        [TestCase(-3)]
        [TestCase(-2)]
        [TestCase(-1)]
        [TestCase(1)]
        [TestCase(2)]
        [TestCase(3)]
        [TestCase(5)]
        [TestCase(8)]
        [TestCase(13)]
        [TestCase(21)]
        public void LinqAttributes_Counter_Increments(int increment)
        {
            // Create config that uses linq based attributes
            var mappingConfig = new MappingConfiguration();
            var counterTable = new Table<CounterEntityWithLinqAttributes>(Session, mappingConfig);

            var counter = new CounterEntityWithLinqAttributes { KeyPart1 = Guid.NewGuid(), KeyPart2 = 1 };

            var updateCounterCql =
                "UPDATE \"CounterEntityWithLinqAttributes\" " +
                "SET \"Counter\" = \"Counter\" + ? " +
                "WHERE \"KeyPart1\" = ? AND \"KeyPart2\" = ?";

            // first update
            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = increment })
                .Update()
                .Execute();

            VerifyBoundStatement(updateCounterCql, 1, (long)increment, counter.KeyPart1, counter.KeyPart2);

            counter.Counter = increment; // counter = increment
            PrimeLinqCounterQuery(counter);

            var updatedCounter =
                counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                            .Execute()
                            .First();

            Assert.AreEqual(increment, updatedCounter.Counter);

            // second update
            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = increment })
                .Update().Execute();

            VerifyBoundStatement(updateCounterCql, 2, (long)increment, counter.KeyPart1, counter.KeyPart2);

            counter.Counter += increment; // counter = increment*2;
            PrimeLinqCounterQuery(counter);

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                                         .Execute().First();
            Assert.AreEqual(increment * 2, updatedCounter.Counter);

            // third update
            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = increment })
                .Update().Execute();

            VerifyBoundStatement(updateCounterCql, 3, (long)increment, counter.KeyPart1, counter.KeyPart2);

            counter.Counter += increment; // counter = increment*3;
            PrimeLinqCounterQuery(counter);

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                                         .Execute().First();
            Assert.AreEqual(increment * 3, updatedCounter.Counter);

            // testing negative values
            var negativeIncrement = -1 * increment;

            // first negative update
            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = negativeIncrement })
                .Update().Execute();

            VerifyBoundStatement(updateCounterCql, 1, (long)negativeIncrement, counter.KeyPart1, counter.KeyPart2);

            counter.Counter += negativeIncrement;
            PrimeLinqCounterQuery(counter);

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                                         .Execute().First();
            Assert.AreEqual(increment * 2, updatedCounter.Counter);

            // second negative update
            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = negativeIncrement })
                .Update().Execute();

            VerifyBoundStatement(updateCounterCql, 2, (long)negativeIncrement, counter.KeyPart1, counter.KeyPart2);

            counter.Counter += negativeIncrement; // counter -= increment = increment
            PrimeLinqCounterQuery(counter);

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                                         .Execute().First();
            Assert.AreEqual(increment, updatedCounter.Counter);

            // third negative update
            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = negativeIncrement })
                .Update().Execute();

            VerifyBoundStatement(updateCounterCql, 3, (long)negativeIncrement, counter.KeyPart1, counter.KeyPart2);

            counter.Counter += negativeIncrement; // counter -= increment = 0
            PrimeLinqCounterQuery(counter);

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                                         .Execute().First();
            Assert.AreEqual(0, updatedCounter.Counter);
        }

        [Test]
        public void LinqCounter_BatchTest()
        {
            var mappingConfig = new MappingConfiguration();
            mappingConfig.Define(new Map<CounterEntityWithLinqAttributes>()
                .ExplicitColumns()
                .Column(t => t.KeyPart1)
                .Column(t => t.KeyPart2)
                .Column(t => t.Counter, map => map.AsCounter())
                .PartitionKey(t => t.KeyPart1, t => t.KeyPart2)
                .TableName("linqcounter_batchtest_table")
            );
            var counterTable = new Table<CounterEntityWithLinqAttributes>(Session, mappingConfig);
            counterTable.CreateIfNotExists();
            var counter = new CounterEntityWithLinqAttributes { KeyPart1 = Guid.NewGuid(), KeyPart2 = 1, Counter = 1 };
            var counter2 = new CounterEntityWithLinqAttributes { KeyPart1 = counter.KeyPart1, KeyPart2 = 2, Counter = 2 };

            var batch = counterTable.GetSession().CreateBatch(BatchType.Counter);

            var update1 = counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = 1 })
                .Update();

            var update2 = counterTable
                .Where(t => t.KeyPart1 == counter2.KeyPart1 && t.KeyPart2 == counter2.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = 2 })
                .Update();

            batch.Append(update1);
            batch.Append(update2);

            batch.Execute();

            VerifyBatchStatement(
                1,
                new[]
                {
                    "UPDATE linqcounter_batchtest_table SET Counter = Counter + ? WHERE KeyPart1 = ? AND KeyPart2 = ?",
                    "UPDATE linqcounter_batchtest_table SET Counter = Counter + ? WHERE KeyPart1 = ? AND KeyPart2 = ?"
                },
                new[]
                {
                    new object[] { 1L, counter.KeyPart1, counter.KeyPart2 },
                    new object[] { 2L, counter2.KeyPart1, counter2.KeyPart2 }
                });

            var expectedCounters = new[] { counter, counter2 };
            PrimeLinqCounterRangeQuery(expectedCounters, "linqcounter_batchtest_table", false);

            var counters = counterTable.Execute().ToList();
            Assert.AreEqual(2, counters.Count);
            Assert.IsTrue(counters.Contains(counter));
            Assert.IsTrue(counters.Contains(counter2));
        }

        [Cassandra.Data.Linq.Table]
        private class CounterEntityWithLinqAttributes
        {
            [Cassandra.Data.Linq.Counter]
            public long Counter;

            [Cassandra.Data.Linq.PartitionKey(1)]
            public Guid KeyPart1;

            [Cassandra.Data.Linq.PartitionKey(2)]
            public Decimal KeyPart2;

            public static IWhenQueryBuilder WithParams(IWhenQueryBuilder builder, params (string, CounterEntityWithLinqAttributes)[] parameters)
            {
                foreach (var (name, value) in parameters)
                {
                    switch (name)
                    {
                        case nameof(CounterEntityWithLinqAttributes.Counter):
                            builder = builder.WithParam(DataType.Counter, value.Counter);
                            break;

                        case nameof(CounterEntityWithLinqAttributes.KeyPart1):
                            builder = builder.WithParam(DataType.Uuid, value.KeyPart1);
                            break;

                        case nameof(CounterEntityWithLinqAttributes.KeyPart2):
                            builder = builder.WithParam(DataType.Decimal, value.KeyPart2);
                            break;

                        default:
                            throw new ArgumentException("parameter not found");
                    }
                }

                return builder;
            }

            public IWhenQueryBuilder WithParams(IWhenQueryBuilder builder, params string[] parameters)
            {
                return WithParams(builder, parameters.Select(p => (p, this)).ToArray());
            }

            public IWhenQueryBuilder WithParams(IWhenQueryBuilder builder)
            {
                return WithParams(builder, new string[0]);
            }

            public RowsResult CreateRowsResult()
            {
                return (RowsResult)AddRow(CounterEntityWithLinqAttributes.GetEmptyRowsResult());
            }

            public static RowsResult GetEmptyRowsResult()
            {
                return new RowsResult(
                    (nameof(CounterEntityWithLinqAttributes.Counter), DataType.Counter),
                    (nameof(CounterEntityWithLinqAttributes.KeyPart1), DataType.Uuid),
                    (nameof(CounterEntityWithLinqAttributes.KeyPart2), DataType.Decimal));
            }

            public RowsResult AddRow(RowsResult rows)
            {
                return (RowsResult)rows.WithRow(Counter, KeyPart1, KeyPart2);
            }

            public override bool Equals(object obj)
            {
                if (obj == null)
                {
                    return false;
                }
                var comp = (CounterEntityWithLinqAttributes)obj;

                return (this.Counter == comp.Counter
                        && this.KeyPart1.Equals(comp.KeyPart1)
                        && this.KeyPart2.Equals(comp.KeyPart2));
            }

            public override int GetHashCode()
            {
                var hash = KeyPart1.GetHashCode();
                hash += KeyPart2.GetHashCode() * 1000;
                hash += (int)Counter * 100000;
                return hash;
            }
        }
    }
}