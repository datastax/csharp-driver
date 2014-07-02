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
            localCluster.Dispose();
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
            //Some fields are null
            localSession.Execute("INSERT INTO users (id, main_phone) values (1, {alias: 'empty phone'})");
            var row = localSession.Execute("SELECT * FROM users WHERE id = 1").First();
            var value = row.GetValue<Phone>("main_phone");
            Assert.NotNull(value);
            Assert.AreEqual("empty phone", value.Alias);
            //Default
            Assert.IsNull(value.Number);
            //Default
            Assert.AreEqual(0, value.CountryCode);

            //column value is null
            localSession.Execute("INSERT INTO users (id, main_phone) values (2, null)");
            row = localSession.Execute("SELECT * FROM users WHERE id = 2").First();
            Assert.IsNull(row.GetValue<Phone>("main_phone"));
            localCluster.Dispose();
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
            localCluster.Dispose();
        }

        [TestCassandraVersion(2, 1)]
        [Test]
        public void MappingNestedTypeTest()
        {
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            var localSession = localCluster.Connect("tester");
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone>(),
                UdtMap.For<Contact>()
                    .Map(c => c.FirstName, "first_name")
                    .Map(c => c.LastName, "last_name")
                    .Map(c => c.Phones, "phones")
            );
            var contactsJson =
                "[" +
                "{first_name: 'Jules', last_name: 'Winnfield', phones: {{alias: 'home', number: '123456'}}}," +
                "{first_name: 'Mia', last_name: 'Wallace', phones: {{alias: 'mobile', number: '789'}}}" +
                "]";
            localSession.Execute(String.Format("INSERT INTO users_contacts (id, contacts) values (1, {0})", contactsJson));
            var rs = localSession.Execute("SELECT * FROM users_contacts WHERE id = 1");
            var row = rs.First();

            var contacts = row.GetValue<List<Contact>>("contacts");
            Assert.NotNull(contacts);
            Assert.AreEqual(2, contacts.Count);
            var julesContact = contacts[0];
            Assert.AreEqual("Jules", julesContact.FirstName);
            Assert.AreEqual("Winnfield", julesContact.LastName);
            Assert.IsNotNull(julesContact.Phones);
            Assert.AreEqual(1, julesContact.Phones.Count());
            var miaContact = contacts[1];
            localCluster.Dispose();
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

            public IEnumerable<Phone> Phones { get; set; }

            public IEnumerable<string> Emails { get; set; }
        }

        private class Phone
        {
            public string Alias { get; set; }

            public string Number { get; set; }

            public int CountryCode { get; set; }
        }
    }
}
