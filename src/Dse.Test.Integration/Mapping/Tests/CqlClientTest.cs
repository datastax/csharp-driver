//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Linq;
using Dse.Data.Linq;
using Dse.Mapping;
using NUnit.Framework;

#pragma warning disable 618
#pragma warning disable 612

namespace Dse.Test.Integration.Mapping.Tests
{
    public class CqlClientTest : SimulacronTest
    {
        /// <summary>
        /// Verify that two separate instances of the CqlClient object can co-exist
        /// </summary>
        [Test]
        public void CqlClient_TwoInstancesBasedOnSameSession()
        {
            // Setup
            MappingConfiguration config1 = new MappingConfiguration();
            config1.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Poco1), () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Poco1)));
            var table1 = new Table<Poco1>(Session, config1);
            string cqlSelectAll1 = "SELECT * from " + table1.Name;

            MappingConfiguration config2 = new MappingConfiguration();
            config2.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Poco2), () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Poco2)));
            var table2 = new Table<Poco2>(Session, config2);
            string cqlSelectAll2 = "SELECT * from " + table2.Name;

            // Now re-instantiate the cqlClient, but with mapping rule that resolves the missing key issue
            var cqlClient1 = new Mapper(Session, new MappingConfiguration().Define(new Poco1Mapping()));
            var cqlClient2 = new Mapper(Session, new MappingConfiguration().Define(new Poco2Mapping()));

            // insert new record into two separate tables
            Poco1 poco1 = new Poco1();
            poco1.SomeString1 += "1";
            cqlClient1.Insert(poco1);

            VerifyBoundStatement(
                "INSERT INTO poco1 (SomeDouble1, somestring1) VALUES (?, ?)",
                1,
                poco1.SomeDouble1, poco1.SomeString1);

            Poco2 poco2 = new Poco2();
            poco2.SomeString2 += "1";
            cqlClient2.Insert(poco2);

            VerifyBoundStatement(
                "INSERT INTO poco2 (SomeDouble2, somestring2) VALUES (?, ?)",
                1,
                poco2.SomeDouble2, poco2.SomeString2);

            // Select Values from each table

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * from poco1")
                      .ThenRowsSuccess(new[] { "SomeDouble1", "somestring1" }, r => r.WithRow(poco1.SomeDouble1, poco1.SomeString1)));

            List<Poco1> poco1s = cqlClient1.Fetch<Poco1>(cqlSelectAll1).ToList();
            Assert.AreEqual(1, poco1s.Count);
            Assert.AreEqual(poco1.SomeString1, poco1s[0].SomeString1);
            Assert.AreEqual(poco1.SomeDouble1, poco1s[0].SomeDouble1);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * from poco2")
                      .ThenRowsSuccess(new[] { "SomeDouble2", "somestring2" }, r => r.WithRow(poco2.SomeDouble2, poco2.SomeString2)));

            List<Poco2> poco2s = cqlClient2.Fetch<Poco2>(cqlSelectAll2).ToList();
            Assert.AreEqual(1, poco2s.Count);
            Assert.AreEqual(poco2.SomeString2, poco2s[0].SomeString2);
            Assert.AreEqual(poco2.SomeDouble2, poco2s[0].SomeDouble2);

            // Try that again
            poco1s.Clear();
            Assert.AreEqual(0, poco1s.Count);
            poco1s = cqlClient1.Fetch<Poco1>(cqlSelectAll1).ToList();
            Assert.AreEqual(1, poco1s.Count);
            Assert.AreEqual(poco1.SomeString1, poco1s[0].SomeString1);
            Assert.AreEqual(poco1.SomeDouble1, poco1s[0].SomeDouble1);

            poco2s.Clear();
            Assert.AreEqual(0, poco2s.Count);
            poco2s = cqlClient1.Fetch<Poco2>(cqlSelectAll2).ToList();
            Assert.AreEqual(1, poco2s.Count);
            Assert.AreEqual(poco2.SomeString2, poco2s[0].SomeString2);
            Assert.AreEqual(poco2.SomeDouble2, poco2s[0].SomeDouble2);
        }

        ////////////////////////////////////////////////////
        /// Test Classes
        ////////////////////////////////////////////////////

        [Dse.Data.Linq.Table("poco1")]
        private class Poco1
        {
            [Dse.Data.Linq.PartitionKeyAttribute]
            [Dse.Mapping.Attributes.PartitionKey]
            [Dse.Data.Linq.Column("somestring1")]
            public string SomeString1 = "somevalue_1_";

            [Dse.Data.Linq.Column("somedouble1")]
            public double SomeDouble1 = 1;
        }

        [Dse.Data.Linq.Table("poco2")]
        private class Poco2
        {
            [Dse.Data.Linq.PartitionKeyAttribute]
            [Dse.Mapping.Attributes.PartitionKey]
            [Dse.Data.Linq.Column("somestring2")]
            public string SomeString2 = "somevalue_2_";

            [Dse.Data.Linq.Column("somedouble2")]
            public double SomeDouble2 = 2;
        }

        private class Poco1Mapping : Map<Poco1>
        {
            public Poco1Mapping()
            {
                TableName("poco1");
                PartitionKey(u => u.SomeString1);
                Column(u => u.SomeString1, cm => cm.WithName("somestring1"));
            }
        }

        private class Poco2Mapping : Map<Poco2>
        {
            public Poco2Mapping()
            {
                TableName("poco2");
                PartitionKey(u => u.SomeString2);
                Column(u => u.SomeString2, cm => cm.WithName("somestring2"));
            }
        }
    }
}