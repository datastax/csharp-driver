using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
#pragma warning disable 618
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
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

        [Test, Category("short")]
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
            var counterTable = new Table<CounterEntityWithLinqAttributes>(_session, mappingConfig);
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

            var counters = counterTable.Execute().ToList();
            Assert.AreEqual(2, counters.Count);
            Assert.IsTrue(counters.Contains(counter));
            Assert.IsTrue(counters.Contains(counter2));
        }

        [Cassandra.Data.Linq.Table]
        class CounterEntityWithLinqAttributes
        {
            [Cassandra.Data.Linq.Counter]
            public long Counter;
            [Cassandra.Data.Linq.PartitionKey(1)]
            public Guid KeyPart1;
            [Cassandra.Data.Linq.PartitionKey(2)]
            public Decimal KeyPart2;


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
