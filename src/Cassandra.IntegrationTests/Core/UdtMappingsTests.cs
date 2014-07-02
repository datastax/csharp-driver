using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests.Core
{
    public class UdtMappingsTests : SingleNodeClusterTest
    {
        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            if (Options.Default.CassandraVersion >= new Version(2, 1))
            {
                const string cqlType1 = "CREATE TYPE phone (alias text, number text, country_code int)";
                const string cqlType2 = "CREATE TYPE contact (first_name text, last_name text, birth_date timestamp, phones set<phone>, emails set<text>)";
                const string cqlTable1 = "CREATE TABLE users (id int PRIMARY KEY, main_phone phone)";
                const string cqlTable2 = "CREATE TABLE users_contacts (id int PRIMARY KEY, contacts list<contact>)";

                Session.Execute(cqlType1);
                Session.Execute(cqlType2);
                Session.Execute(cqlTable1);
                Session.Execute(cqlTable2);

                //Nullify Session and cluster to force using local instances
                Session = null;
                Cluster = null;
            }
        }

        [TestCassandraVersion(2, 1)]
        [Test]
        public void MappingSimpleExplicitTest()
        {
            //Use a local cluster
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            var localSession = localCluster.Connect("tester");
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone>("phone")
                    .Map(v => v.Alias, "alias")
                    .Map(v => v.CountryCode, "country_code")
                    .Map(v => v.Number, "number")
            );
            localSession.Execute("INSERT INTO users (id, main_phone) values (1, {alias: 'home phone', number: '123', country_code: 34})");
            var rs = localSession.Execute("SELECT * FROM users WHERE id = 1");
            var row = rs.First();
            var value = row.GetValue<Phone>("main_phone");
            Assert.NotNull(value);
            Assert.AreEqual("home phone", value.Alias);
            Assert.AreEqual("123", value.Number);
            Assert.AreEqual(34, value.CountryCode);
        }

        [TestCassandraVersion(2, 1)]
        [Test]
        public void MappingSimpleExplicitNullsTest()
        {
            //Use a local cluster
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            var localSession = localCluster.Connect("tester");
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone>("phone")
                    .Map(v => v.Alias, "alias")
                    .Map(v => v.CountryCode, "country_code")
                    .Map(v => v.Number, "number")
            );
            localSession.Execute("INSERT INTO users (id, main_phone) values (1, {alias: 'empty phone'})");
            var rs = localSession.Execute("SELECT * FROM users WHERE id = 1");
            var row = rs.First();
            var value = row.GetValue<Phone>("main_phone");
            Assert.NotNull(value);
            Assert.AreEqual("empty phone", value.Alias);
            //Default
            Assert.IsNull(value.Number);
            //Default
            Assert.AreEqual(0, value.CountryCode);
        }

        [TestCassandraVersion(2, 1)]
        [Test]
        public void MappingSimpleImplicitTest()
        {
            //Use a local cluster
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            var localSession = localCluster.Connect("tester");
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone>()
            );
            localSession.Execute("INSERT INTO users (id, main_phone) values (1, {alias: 'home phone', number: '123', country_code: 34})");
            var rs = localSession.Execute("SELECT * FROM users WHERE id = 1");
            var row = rs.First();
            var value = row.GetValue<Phone>("main_phone");
            Assert.NotNull(value);
            Assert.AreEqual("home phone", value.Alias);
            Assert.AreEqual("123", value.Number);
            //The property and the field names don't match
            Assert.AreEqual(0, value.CountryCode);
        }

        [TestCassandraVersion(2, 1)]
        [Test]
        public void MappingNestedTypeTest()
        {
            throw new NotImplementedException();
        }

        [TestCassandraVersion(2, 1)]
        [Test]
        public void MappingCaseSensitiveTest()
        {
            throw new NotImplementedException();
        }

        private class Contact
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public DateTime Birth { get; set; }

            public string NotMappedProp { get; set; }

            public List<Phone> Phones { get; set; }

            public List<string> Emails { get; set; }
        }

        private class Phone
        {
            public string Alias { get; set; }

            public string Number { get; set; }

            public int CountryCode { get; set; }
        }
    }
}
