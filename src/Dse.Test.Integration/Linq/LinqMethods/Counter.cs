//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dse.Data.Linq;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;
#pragma warning disable 618
#pragma warning disable 612

namespace Dse.Test.Integration.Linq.LinqMethods
{
    public class Counter : SharedClusterTest
    {
        ISession _session;
        string _uniqueKsName;

        [SetUp]
        public void SetupTest()
        {
            _session = Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        [Test, Category("short")]
        public void LinqAttributes_Counter()
        {
            //var mapping = new Map<PocoWithCounter>();
            var mappingConfig = new MappingConfiguration();
            mappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(CounterEntityWithLinqAttributes),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(CounterEntityWithLinqAttributes)));
            var table = new Table<CounterEntityWithLinqAttributes>(_session, mappingConfig);
            table.Create();

            List<CounterEntityWithLinqAttributes> counterPocos = new List<CounterEntityWithLinqAttributes>();
            for (int i = 0; i < 10; i++)
            {
                counterPocos.Add(
                    new CounterEntityWithLinqAttributes()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            int counterIncrements = 100;
            string updateStr = String.Format("UPDATE \"{0}\" SET \"{1}\"=\"{1}\" + 1 WHERE \"{2}\"=? and \"{3}\"=?", typeof(CounterEntityWithLinqAttributes).Name, "Counter", "KeyPart1", "KeyPart2");
            var updateSession = _session.Prepare(updateStr);
            foreach (CounterEntityWithLinqAttributes pocoWithCounter in counterPocos)
            {
                var boundStatement = updateSession.Bind(new object[] { pocoWithCounter.KeyPart1, pocoWithCounter.KeyPart2 });
                for (int j = 0; j < counterIncrements; j++)
                    _session.Execute(boundStatement);
                pocoWithCounter.Counter += counterIncrements;
            }

            List<CounterEntityWithLinqAttributes> countersQueried = table.Select(m => m).Execute().ToList();
            foreach (CounterEntityWithLinqAttributes pocoWithCounterExpected in counterPocos)
            {
                bool counterFound = false;
                foreach (CounterEntityWithLinqAttributes pocoWithCounterActual in countersQueried)
                {
                    if (pocoWithCounterExpected.KeyPart1 == pocoWithCounterActual.KeyPart1)
                    {
                        Assert.AreEqual(pocoWithCounterExpected.KeyPart2, pocoWithCounterExpected.KeyPart2);
                        Assert.AreEqual(pocoWithCounterExpected.Counter, pocoWithCounterExpected.Counter);
                        counterFound = true;
                    }
                }
                Assert.IsTrue(counterFound, "Counter with first key part: " + pocoWithCounterExpected.KeyPart1 + " was not found!");
            }
        }


        /// <summary>
        /// Do many counter updates in parallel
        /// </summary>
        [Test, Category("long")]
        public void LinqAttributes_Counter_Parallel()
        {
            //var mapping = new Map<PocoWithCounter>();
            var mappingConfig = new MappingConfiguration();
            mappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(CounterEntityWithLinqAttributes),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(CounterEntityWithLinqAttributes)));
            var table = new Table<CounterEntityWithLinqAttributes>(_session, mappingConfig);
            table.Create();

            List<CounterEntityWithLinqAttributes> counterPocos = new List<CounterEntityWithLinqAttributes>();
            for (int i = 0; i < 100; i++)
            {
                counterPocos.Add(
                    new CounterEntityWithLinqAttributes()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            int counterIncrements = 2000;
            string updateStr = String.Format("UPDATE \"{0}\" SET \"{1}\"=\"{1}\" + 1 WHERE \"{2}\"=? and \"{3}\"=?", typeof(CounterEntityWithLinqAttributes).Name, "Counter", "KeyPart1", "KeyPart2");
            var updateSession = _session.Prepare(updateStr);
            Parallel.ForEach(counterPocos, pocoWithCounter =>
            {
                var boundStatement = updateSession.Bind(new object[] {pocoWithCounter.KeyPart1, pocoWithCounter.KeyPart2});
                for (int j = 0; j < counterIncrements; j++)
                    _session.Execute(boundStatement);
                pocoWithCounter.Counter += counterIncrements;
            });

            List<CounterEntityWithLinqAttributes> countersQueried = table.Select(m => m).Execute().ToList();
            foreach (CounterEntityWithLinqAttributes pocoWithCounterExpected in counterPocos)
            {
                bool counterFound = false;
                foreach (CounterEntityWithLinqAttributes pocoWithCounterActual in countersQueried)
                {
                    if (pocoWithCounterExpected.KeyPart1 == pocoWithCounterActual.KeyPart1)
                    {
                        Assert.AreEqual(pocoWithCounterExpected.KeyPart2, pocoWithCounterExpected.KeyPart2);
                        Assert.AreEqual(pocoWithCounterExpected.Counter, pocoWithCounterExpected.Counter);
                        counterFound = true;
                    }
                }
                Assert.IsTrue(counterFound, "Counter with first key part: " + pocoWithCounterExpected.KeyPart1 + " was not found!");
            }
        }

        /// <summary>
        /// Validate expected error message when attempting to insert a row that contains a counter
        /// </summary>
        [Test, Category("short")]
        public void LinqAttributes_Counter_AttemptInsert()
        {
            // Create config that uses linq based attributes
            var mappingConfig = new MappingConfiguration();
            mappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(CounterEntityWithLinqAttributes),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(CounterEntityWithLinqAttributes)));
            var table = new Table<CounterEntityWithLinqAttributes>(_session, mappingConfig);
            table.Create();

            CounterEntityWithLinqAttributes pocoAndLinqAttributesLinqPocos = new CounterEntityWithLinqAttributes()
            {
                KeyPart1 = Guid.NewGuid(),
                KeyPart2 = (decimal) 123,
            };

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => _session.Execute(table.Insert(pocoAndLinqAttributesLinqPocos)));
            string expectedErrMsg = "INSERT statement(s)? are not allowed on counter tables, use UPDATE instead";
            StringAssert.IsMatch(expectedErrMsg, e.Message);
        }


        [TestCase(-21), Category("short")]
        [TestCase(-13), Category("short")]
        [TestCase(-8), Category("short")]
        [TestCase(-5), Category("short")]
        [TestCase(-3), Category("short")]
        [TestCase(-2), Category("short")]
        [TestCase(-1), Category("short")]
        [TestCase(1), Category("short")]
        [TestCase(2), Category("short")]
        [TestCase(3), Category("short")]
        [TestCase(5), Category("short")]
        [TestCase(8), Category("short")]
        [TestCase(13), Category("short")]
        [TestCase(21), Category("short")]
        public void LinqAttributes_Counter_Increments(int increment)
        {
            // Create config that uses linq based attributes
            var mappingConfig = new MappingConfiguration();
            var counterTable = new Table<CounterEntityWithLinqAttributes>(_session, mappingConfig);
            counterTable.CreateIfNotExists();

            var counter = new CounterEntityWithLinqAttributes { KeyPart1 = Guid.NewGuid(), KeyPart2 = 1};

            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = increment })
                .Update().Execute();

            var updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2).Execute().FirstOrDefault();
            Assert.AreEqual(increment, updatedCounter.Counter);

            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = increment })
                .Update().Execute();

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2).Execute().FirstOrDefault();
            Assert.AreEqual(increment*2, updatedCounter.Counter);

            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = increment })
                .Update().Execute();

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2).Execute().FirstOrDefault();
            Assert.AreEqual(increment*3, updatedCounter.Counter);

            //testing negative values
            int negativeIncrement = -1*increment;
            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = negativeIncrement })
                .Update().Execute();

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2).Execute().FirstOrDefault();
            Assert.AreEqual(increment * 2, updatedCounter.Counter);

            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = negativeIncrement })
                .Update().Execute();

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2).Execute().FirstOrDefault();
            Assert.AreEqual(increment, updatedCounter.Counter);

            counterTable
                .Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2)
                .Select(t => new CounterEntityWithLinqAttributes { Counter = negativeIncrement })
                .Update().Execute();

            updatedCounter = counterTable.Where(t => t.KeyPart1 == counter.KeyPart1 && t.KeyPart2 == counter.KeyPart2).Execute().FirstOrDefault();
            Assert.AreEqual(0, updatedCounter.Counter);
        }


        [Dse.Data.Linq.Table]
        class CounterEntityWithLinqAttributes
        {
            [Dse.Data.Linq.Counter]
            public long Counter;
            [Dse.Data.Linq.PartitionKey(1)]
            public Guid KeyPart1;
            [Dse.Data.Linq.PartitionKey(2)]
            public Decimal KeyPart2;
        }


    }
}
