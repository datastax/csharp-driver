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
    [Category("short"), Category("realcluster")]
    public class Counter : SharedClusterTest
    {
        private ISession _session;
        string _uniqueKsName;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        [Test, Category("short")]
        public void Counter_Success()
        {
            var config = new AttributeBasedTypeDefinition(typeof(PocoWithCounterAttribute));
            var table = new Table<PocoWithCounterAttribute>(_session, new MappingConfiguration().Define(config));
            table.CreateIfNotExists();
            var cqlClient = new Mapper(_session, new MappingConfiguration().Define(config));

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
                for (int j = 0; j < counterIncrements; j++)
                    _session.Execute(boundStatement);
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
        /// Validate expected error message when attempting to insert a row that contains a counter
        /// </summary>
        [Test, Category("short")]
        public void Counter_LinqAttributes_AttemptInsert()
        {
            var table = new Table<PocoWithCounterAttribute>(_session, new MappingConfiguration());
            table.Create();

            PocoWithCounterAttribute pocoAndLinqAttributesPocos = new PocoWithCounterAttribute()
            {
                KeyPart1 = Guid.NewGuid(),
                KeyPart2 = (decimal)123,
            };

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => table.Insert(pocoAndLinqAttributesPocos).Execute());
            string expectedErrMsg = "INSERT statement(s)? are not allowed on counter tables, use UPDATE instead";
            StringAssert.IsMatch(expectedErrMsg, e.Message);
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
