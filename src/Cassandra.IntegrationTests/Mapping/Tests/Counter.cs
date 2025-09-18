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
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Mapping.Tests
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
                          r => r.WithRows(counterPocos.Select(c => new object[] { c.Counter, c.KeyPart1, c.KeyPart2 }).ToArray())));

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
            [Cassandra.Mapping.Attributes.Counter]
            public long Counter;

            [Cassandra.Mapping.Attributes.PartitionKey(1)]
            public Guid KeyPart1;

            [Cassandra.Mapping.Attributes.PartitionKey(2)]
            public Decimal KeyPart2;
        }
    }
}