using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
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

        [Test, Category("medium"), NUnit.Framework.Ignore("Counter attribute doesn't seem to be getting used, pending question")]
        public void Counter_LinqAttributes_Success()
        {
            //var mapping = new Map<PocoWithCounter>();
            var definition = new AttributeBasedTypeDefinition(typeof(PocoWithCounterAttribute));
            var table = _session.GetTable<PocoWithCounterAttribute>(definition);
            table.Create();
            var cqlClient = CqlClientConfiguration.ForSession(_session)
                .UseIndividualMappings(definition)
                .BuildCqlClient();

            List<PocoWithCounterAttribute> counterPocos = new List<PocoWithCounterAttribute>();
            for (int i = 0; i < 10; i++)
            {
                counterPocos.Add(
                    new PocoWithCounterAttribute()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            int counterIncrements = 100;
            string updateStr = String.Format("UPDATE \"{0}\" SET \"{1}\"=\"{1}\" + 1 WHERE \"{2}\"=? and \"{3}\"=?", table.Name.ToLower(), "counter", "keypart1", "keypart2");
            var updateSession = _session.Prepare(updateStr);
            foreach (PocoWithCounterAttribute pocoWithCounter in counterPocos)
            {
                var boundStatement = updateSession.Bind(new object[] { pocoWithCounter.KeyPart1, pocoWithCounter.KeyPart2 });
                string bountSessionToStr = boundStatement.ToString();
                Cql cql = new Cql(boundStatement.ToString());
                for (int j = 0; j < counterIncrements; j++)
                    cqlClient.Execute(bountSessionToStr);
                pocoWithCounter.Counter += counterIncrements;
            }

            List<PocoWithCounterAttribute> countersQueried = cqlClient.Fetch<PocoWithCounterAttribute>().ToList();
            foreach (PocoWithCounterAttribute pocoWithCounterExpected in counterPocos)
            {
                bool counterFound = false;
                foreach (PocoWithCounterAttribute pocoWithCounterActual in countersQueried)
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
        [Test, Category("large"), NUnit.Framework.Ignore("Counter attribute doesn't seem to be getting used, pending question")]
        public void Counter_LinqAttributes_Parallel()
        {
            //var mapping = new Map<PocoWithCounter>();
            var definition = new AttributeBasedTypeDefinition(typeof(PocoWithCounterAttribute));
            var table = _session.GetTable<PocoWithCounterAttribute>(definition);
            table.Create();
            var cqlClient = CqlClientConfiguration.ForSession(_session)
                .UseIndividualMappings(definition)
                .BuildCqlClient();

            List<PocoWithCounterAttribute> counterPocos = new List<PocoWithCounterAttribute>();
            for (int i = 0; i < 100; i++)
            {
                counterPocos.Add(
                    new PocoWithCounterAttribute()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            int counterIncrements = 2000;
            string updateStr = String.Format("UPDATE \"{0}\" SET \"{1}\"=\"{1}\" + 1 WHERE \"{2}\"=? and \"{3}\"=?", table.Name.ToLower(), "counter", "keypart1", "keypart2");
            var updateSession = _session.Prepare(updateStr);
            Parallel.ForEach(counterPocos, pocoWithCounter =>
            {
                var boundStatement = updateSession.Bind(new object[] {pocoWithCounter.KeyPart1, pocoWithCounter.KeyPart2});
                string bountSessionToStr = boundStatement.ToString();
                Cql cql = new Cql(boundStatement.ToString());
                for (int j = 0; j < counterIncrements; j++)
                    cqlClient.Execute(bountSessionToStr);
                pocoWithCounter.Counter += counterIncrements;
            });

            List<PocoWithCounterAttribute> countersQueried = cqlClient.Fetch<PocoWithCounterAttribute>().ToList();
            foreach (PocoWithCounterAttribute pocoWithCounterExpected in counterPocos)
            {
                bool counterFound = false;
                foreach (PocoWithCounterAttribute pocoWithCounterActual in countersQueried)
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
        public void Counter_LinqAttributes_AttemptInsert()
        {
            //var mapping = new Map<PocoWithCounter>();
            var definition = new AttributeBasedTypeDefinition(typeof(PocoWithCounterAttribute));
            var table = _session.GetTable<PocoWithCounterAttribute>(definition);
            table.Create();

            PocoWithCounterAttribute pocoAndLinqAttributesPocos = new PocoWithCounterAttribute()
            {
                KeyPart1 = Guid.NewGuid(),
                KeyPart2 = (decimal)123,
            };

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => _session.Execute(table.Insert(pocoAndLinqAttributesPocos)));
            string expectedErrMsg = "INSERT statement are not allowed on counter tables, use UPDATE instead";
            Assert.AreEqual(expectedErrMsg, e.Message);
        }


        class PocoWithCounterAttribute
        {
            [Cassandra.Mapping.Attributes.Counter]
            public long Counter;
            [Cassandra.Mapping.Attributes.PartitionKey(1)]
            public Guid KeyPart1;
            [Cassandra.Mapping.Attributes.PartitionKey(2)]
            public Decimal KeyPart2;
        }


    }
}
