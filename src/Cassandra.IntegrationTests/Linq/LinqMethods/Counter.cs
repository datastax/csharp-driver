using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    public class Counter : TestGlobals
    {
        ISession _session = null;
        private readonly Logger _logger = new Logger(typeof(Counter));
        string _uniqueKsName;

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        [Test, Category("medium")]
        public void LinqAttributes_Counter()
        {
            //var mapping = new Map<PocoWithCounter>();
            var table = _session.GetTable<EntityWithLinqCounterAttribute>();
            table.Create();

            List<EntityWithLinqCounterAttribute> counterPocos = new List<EntityWithLinqCounterAttribute>();
            for (int i = 0; i < 10; i++)
            {
                counterPocos.Add(
                    new EntityWithLinqCounterAttribute()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            int counterIncrements = 100;
            string updateStr = String.Format("UPDATE \"{0}\" SET \"{1}\"=\"{1}\" + 1 WHERE \"{2}\"=? and \"{3}\"=?", typeof(EntityWithLinqCounterAttribute).Name, "Counter", "KeyPart1", "KeyPart2");
            var updateSession = _session.Prepare(updateStr);
            foreach (EntityWithLinqCounterAttribute pocoWithCounter in counterPocos)
            {
                var boundStatement = updateSession.Bind(new object[] { pocoWithCounter.KeyPart1, pocoWithCounter.KeyPart2 });
                Cql cql = new Cql(boundStatement.ToString());
                for (int j = 0; j < counterIncrements; j++)
                    _session.Execute(boundStatement);
                pocoWithCounter.Counter += counterIncrements;
            }

            List<EntityWithLinqCounterAttribute> countersQueried = table.Select(m => m).Execute().ToList();
            foreach (EntityWithLinqCounterAttribute pocoWithCounterExpected in counterPocos)
            {
                bool counterFound = false;
                foreach (EntityWithLinqCounterAttribute pocoWithCounterActual in countersQueried)
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
        public void LinqAttributes_Counter_CauseInfiniteLoop()
        {
            //var mapping = new Map<PocoWithCounter>();
            var table = _session.GetTable<EntityWithLinqCounterAttribute>();
            table.Create();

            List<EntityWithLinqCounterAttribute> counterPocos = new List<EntityWithLinqCounterAttribute>();
            for (int i = 0; i < 10; i++)
            {
                counterPocos.Add(
                    new EntityWithLinqCounterAttribute()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            int counterIncrements = 100;
            string updateStr = String.Format("UPDATE \"{0}\" SET \"{1}\"=\"{1}\" + 1 WHERE \"{2}\"=? and \"{3}\"=?", typeof(EntityWithLinqCounterAttribute).Name, "Counter", "KeyPart1", "KeyPart2");
            var updateSession = _session.Prepare(updateStr);
            foreach (EntityWithLinqCounterAttribute pocoWithCounter in counterPocos)
            {
                var boundStatement = updateSession.Bind(new object[] { pocoWithCounter.KeyPart1, pocoWithCounter.KeyPart2 });
                Cql cql = new Cql(boundStatement.ToString());
                for (int j = 0; j < counterIncrements; j++)
                    _session.Execute(boundStatement);
                pocoWithCounter.Counter += counterIncrements;
            }

            List<EntityWithLinqCounterAttribute> countersQueried = table.Select(m => m).Execute().ToList();
            foreach (EntityWithLinqCounterAttribute pocoWithCounterExpected in counterPocos)
            {
                bool counterFound = false;
                foreach (EntityWithLinqCounterAttribute pocoWithCounterActual in countersQueried)
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


            //var mapping = new Map<PocoWithCounter>();
            table = _session.GetTable<EntityWithLinqCounterAttribute>();
            table.Create();

            EntityWithLinqCounterAttribute pocoAndLinqAttributesPocos = new EntityWithLinqCounterAttribute()
            {
                KeyPart1 = Guid.NewGuid(),
                KeyPart2 = (decimal)123,
            };

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => _session.Execute(table.Insert(pocoAndLinqAttributesPocos)));
            string expectedErrMsg = "INSERT statement are not allowed on counter tables, use UPDATE instead";
            Assert.AreEqual(expectedErrMsg, e.Message);

            _session.DeleteKeyspace(_uniqueKsName);

            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);


            //var mapping = new Map<PocoWithCounter>();
            counterPocos = new List<EntityWithLinqCounterAttribute>();

            table = _session.GetTable<EntityWithLinqCounterAttribute>();
            table.Create();

            for (int i = 0; i < 100; i++)
            {
                counterPocos.Add(
                    new EntityWithLinqCounterAttribute()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            counterIncrements = 2000;
            updateStr = String.Format("UPDATE \"{0}\" SET \"{1}\"=\"{1}\" + 1 WHERE \"{2}\"=? and \"{3}\"=?", typeof(EntityWithLinqCounterAttribute).Name, "Counter", "KeyPart1", "KeyPart2");
            updateSession = _session.Prepare(updateStr);
            Parallel.ForEach(counterPocos, pocoWithCounter =>
            {
                var boundStatement = updateSession.Bind(new object[] { pocoWithCounter.KeyPart1, pocoWithCounter.KeyPart2 });
                Cql cql = new Cql(boundStatement.ToString());
                for (int j = 0; j < counterIncrements; j++)
                    _session.Execute(boundStatement);
                pocoWithCounter.Counter += counterIncrements;
            });

            countersQueried = table.Select(m => m).Execute().ToList();
            foreach (EntityWithLinqCounterAttribute pocoWithCounterExpected in counterPocos)
            {
                bool counterFound = false;
                foreach (EntityWithLinqCounterAttribute pocoWithCounterActual in countersQueried)
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
            var table = _session.GetTable<EntityWithLinqCounterAttribute>();
            table.Create();

            List<EntityWithLinqCounterAttribute> counterPocos = new List<EntityWithLinqCounterAttribute>();
            for (int i = 0; i < 100; i++)
            {
                counterPocos.Add(
                    new EntityWithLinqCounterAttribute()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            int counterIncrements = 2000;
            string updateStr = String.Format("UPDATE \"{0}\" SET \"{1}\"=\"{1}\" + 1 WHERE \"{2}\"=? and \"{3}\"=?", typeof(EntityWithLinqCounterAttribute).Name, "Counter", "KeyPart1", "KeyPart2");
            var updateSession = _session.Prepare(updateStr);
            Parallel.ForEach(counterPocos, pocoWithCounter =>
            {
                var boundStatement = updateSession.Bind(new object[] {pocoWithCounter.KeyPart1, pocoWithCounter.KeyPart2});
                Cql cql = new Cql(boundStatement.ToString());
                for (int j = 0; j < counterIncrements; j++)
                    _session.Execute(boundStatement);
                pocoWithCounter.Counter += counterIncrements;
            });

            List<EntityWithLinqCounterAttribute> countersQueried = table.Select(m => m).Execute().ToList();
            foreach (EntityWithLinqCounterAttribute pocoWithCounterExpected in counterPocos)
            {
                bool counterFound = false;
                foreach (EntityWithLinqCounterAttribute pocoWithCounterActual in countersQueried)
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
        [Test, Category("medium")]
        public void LinqAttributes_Counter_AttemptInsert()
        {
            //var mapping = new Map<PocoWithCounter>();
            var table = _session.GetTable<EntityWithLinqCounterAttribute>();
            table.Create();

            EntityWithLinqCounterAttribute pocoAndLinqAttributesPocos = new EntityWithLinqCounterAttribute()
            {
                KeyPart1 = Guid.NewGuid(),
                KeyPart2 = (decimal) 123,
            };

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => _session.Execute(table.Insert(pocoAndLinqAttributesPocos)));
            string expectedErrMsg = "INSERT statement are not allowed on counter tables, use UPDATE instead";
            Assert.AreEqual(expectedErrMsg, e.Message);
        }


        class EntityWithLinqCounterAttribute
        {
            [Cassandra.Data.Linq.Counter]
            public long Counter;
            [Cassandra.Data.Linq.PartitionKey(1)]
            public Guid KeyPart1;
            [Cassandra.Data.Linq.PartitionKey(2)]
            public Decimal KeyPart2;
        }


    }
}
