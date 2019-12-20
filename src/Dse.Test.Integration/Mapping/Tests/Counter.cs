//
//  Copyright (C) DataStax, Inc.
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
using Dse.Mapping.Attributes;
using Dse.Test.Integration.SimulacronAPI;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then;
using NUnit.Framework;

namespace Dse.Test.Integration.Mapping.Tests
{
    public class Counter : SimulacronTest
    {
        [Test]
        public void Counter_Success()
        {
            var config = new AttributeBasedTypeDefinition(typeof(PocoWithCounterAttribute));
            var table = new Table<PocoWithCounterAttribute>(Session, new MappingConfiguration().Define(config));
            table.CreateIfNotExists();

            VerifyQuery(
                "CREATE TABLE PocoWithCounterAttribute (Counter counter, KeyPart1 uuid, KeyPart2 decimal, " +
                    "PRIMARY KEY ((KeyPart1, KeyPart2)))",
                1);

            var cqlClient = new Mapper(Session, new MappingConfiguration().Define(config));

            var counterPocos = new List<PocoWithCounterAttribute>();
            for (var i = 0; i < 10; i++)
            {
                counterPocos.Add(
                    new PocoWithCounterAttribute()
                    {
                        KeyPart1 = Guid.NewGuid(),
                        KeyPart2 = (decimal)123,
                    });
            }

            var counterIncrements = 100;
            foreach (var pocoWithCounter in counterPocos)
            {
                pocoWithCounter.Counter += counterIncrements;
            }

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT Counter, KeyPart1, KeyPart2 FROM PocoWithCounterAttribute")
                      .ThenRowsSuccess(new[] 
                          {
                              ("Counter", DataType.Counter),
                              ("KeyPart1", DataType.Uuid),
                              ("KeyPart2", DataType.Decimal)
                          },
                          r => r.WithRows(counterPocos.Select(c => new object [] { c.Counter, c.KeyPart1, c.KeyPart2 }).ToArray())));

            var countersQueried = cqlClient.Fetch<PocoWithCounterAttribute>().ToList();
            foreach (var pocoWithCounterExpected in counterPocos)
            {
                var counterFound = false;
                foreach (var pocoWithCounterActual in countersQueried)
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
        [Test]
        public void Counter_LinqAttributes_AttemptInsert()
        {
            var table = new Table<PocoWithCounterAttribute>(Session, new MappingConfiguration());
            table.Create();

            VerifyQuery(
                "CREATE TABLE PocoWithCounterAttribute (Counter counter, KeyPart1 uuid, KeyPart2 decimal, " +
                    "PRIMARY KEY ((KeyPart1, KeyPart2)))",
                1);

            PocoWithCounterAttribute pocoAndLinqAttributesPocos = new PocoWithCounterAttribute()
            {
                KeyPart1 = Guid.NewGuid(),
                KeyPart2 = (decimal)123,
            };

            string expectedErrMsg = "INSERT statement(s)? are not allowed on counter tables, use UPDATE instead";
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "INSERT INTO PocoWithCounterAttribute (Counter, KeyPart1, KeyPart2) VALUES (?, ?, ?)",
                          when => when.WithParams(
                              pocoAndLinqAttributesPocos.Counter, 
                              pocoAndLinqAttributesPocos.KeyPart1, 
                              pocoAndLinqAttributesPocos.KeyPart2))
                      .ThenServerError(ServerError.Invalid, expectedErrMsg));

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => table.Insert(pocoAndLinqAttributesPocos).Execute());
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        private class PocoWithCounterAttribute
        {
            [Dse.Mapping.Attributes.Counter]
            public long Counter;
            [Dse.Mapping.Attributes.PartitionKey(1)]
            public Guid KeyPart1;
            [Dse.Mapping.Attributes.PartitionKey(2)]
            public Decimal KeyPart2;
        }
    }
}