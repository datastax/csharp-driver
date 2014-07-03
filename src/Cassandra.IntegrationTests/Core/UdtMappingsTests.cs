using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests.Core
{
    [TestCassandraVersion(2, 1)]
    public class UdtMappingsTests : SingleNodeClusterTest
    {
        /// <summary>
        /// The protocol versions in which udts are supported
        /// </summary>
        private static readonly int[] UdtProtocolVersionSupported = new[] {3};

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

        [Test]
        public void MappingSingleExplicitTest()
        {
            foreach (var protocolVersion in UdtProtocolVersionSupported)
            {
                //Use all possible protocol versions
                Cluster.MaxProtocolVersion = protocolVersion;
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
        }

        [Test]
        public void MappingSingleExplicitNullsTest()
        {
            foreach (var protocolVersion in UdtProtocolVersionSupported)
            {
                Cluster.MaxProtocolVersion = protocolVersion;
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
        }

        [Test]
        public void MappingSingleImplicitTest()
        {
            foreach (var protocolVersion in UdtProtocolVersionSupported)
            {
                Cluster.MaxProtocolVersion = protocolVersion;
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
        }

        [Test]
        public void MappingNestedTypeTest()
        {
            foreach (var protocolVersion in UdtProtocolVersionSupported)
            {
                Cluster.MaxProtocolVersion = protocolVersion;
                var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
                var localSession = localCluster.Connect("tester");
                localSession.UserDefinedTypes.Define(
                    UdtMap.For<Phone>(),
                    UdtMap.For<Contact>()
                          .Map(c => c.FirstName, "first_name")
                          .Map(c => c.LastName, "last_name")
                          .Map(c => c.Birth, "birth_date")
                    );
                const string contactsJson =
                    "[" +
                    "{first_name: 'Jules', last_name: 'Winnfield', birth_date: '1950-02-03 04:05+0000', phones: {{alias: 'home', number: '123456'}}}," +
                    "{first_name: 'Mia', last_name: 'Wallace', phones: {{alias: 'mobile', number: '789'}, {alias: 'office', number: '123'}}}" +
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
                Assert.AreEqual(new DateTimeOffset(1950, 2, 3, 4, 5, 0, 0, TimeSpan.Zero), julesContact.Birth);
                Assert.IsNotNull(julesContact.Phones);
                Assert.AreEqual(1, julesContact.Phones.Count());
                var miaContact = contacts[1];
                Assert.AreEqual("Mia", miaContact.FirstName);
                Assert.AreEqual("Wallace", miaContact.LastName);
                Assert.AreEqual(DateTimeOffset.MinValue, miaContact.Birth);
                Assert.IsNotNull(miaContact.Phones);
                Assert.AreEqual(2, miaContact.Phones.Count());
                Assert.AreEqual("mobile", miaContact.Phones.First().Alias);
                Assert.AreEqual("office", miaContact.Phones.Skip(1).First().Alias);
                localCluster.Dispose();
            }
        }

        [Test]
        public void MappingCaseSensitiveTest()
        {
            foreach (var protocolVersion in UdtProtocolVersionSupported)
            {
                Cluster.MaxProtocolVersion = protocolVersion;
                //Use a local cluster
                var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
                var localSession = localCluster.Connect("tester");
                //Cassandra identifiers are lowercased by default
                localSession.UserDefinedTypes.Define(
                    UdtMap.For<Phone>("phone")
                        .SetIgnoreCase(false)
                        .Map(v => v.Alias, "alias")
                        .Map(v => v.CountryCode, "country_code")
                        .Map(v => v.Number, "number")
                );
                localSession.Execute("INSERT INTO users (id, main_phone) values (101, {alias: 'home phone', number: '123', country_code: 34})");
                var rs = localSession.Execute("SELECT * FROM users WHERE id = 101");
                var row = rs.First();
                var value = row.GetValue<Phone>("main_phone");
                Assert.NotNull(value);
                Assert.AreEqual("home phone", value.Alias);
                Assert.AreEqual("123", value.Number);
                Assert.AreEqual(34, value.CountryCode);

                Assert.Throws<InvalidTypeException>(() => localSession.UserDefinedTypes.Define(
                    //The name should be forced to be case sensitive
                    UdtMap.For<Phone>("PhoNe")
                        .SetIgnoreCase(false)));

                Assert.Throws<InvalidTypeException>(() => localSession.UserDefinedTypes.Define(
                    UdtMap.For<Phone>("phone")
                          .SetIgnoreCase(false)
                          //the field is called 'alias' it should fail
                          .Map(v => v.Alias, "Alias")
                    ));

                localCluster.Dispose();
            }
        }

        [Test]
        public void MappingNotExistingFieldsTest()
        {
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            var localSession = localCluster.Connect("tester");
            Assert.Throws<InvalidTypeException>(() => localSession.UserDefinedTypes.Define(
                //there is no field named like this
                UdtMap.For<Phone>("phone").Map(v => v.Number, "Alias_X_WTF")
                ));
            localCluster.Dispose();
        }

        [Test]
        public void MappingEncodingSingleTest()
        {
            foreach (var protocolVersion in UdtProtocolVersionSupported)
            {
                //Use all possible protocol versions
                Cluster.MaxProtocolVersion = protocolVersion;
                //Use a local cluster
                var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
                var localSession = localCluster.Connect("tester");
                localSession.UserDefinedTypes.Define(
                    UdtMap.For<Phone>("phone")
                        .Map(v => v.Alias, "alias")
                        .Map(v => v.CountryCode, "country_code")
                        .Map(v => v.Number, "number")
                );

                var insert = new SimpleStatement("INSERT INTO users (id, main_phone) values (?, ?)");
                
                //All of the fields null
                var id = 201;
                var phone = new Phone();
                localSession.Execute(insert.Bind(id, phone));
                var rs = localSession.Execute(new SimpleStatement("SELECT * FROM users WHERE id = ?").Bind(id));
                Assert.AreEqual(phone, rs.First().GetValue<Phone>("main_phone"));

                //Some fields null and others with value
                id = 202;
                phone = new Phone() {Alias = "Home phone"};
                localSession.Execute(insert.Bind(id, phone));
                rs = localSession.Execute(new SimpleStatement("SELECT * FROM users WHERE id = ?").Bind(id));
                Assert.AreEqual(phone, rs.First().GetValue<Phone>("main_phone"));

                //All fields filled in
                id = 203;
                phone = new Phone() { Alias = "Mobile phone", CountryCode = 54, Number = "1234567"};
                localSession.Execute(insert.Bind(id, phone));
                rs = localSession.Execute(new SimpleStatement("SELECT * FROM users WHERE id = ?").Bind(id));
                Assert.AreEqual(phone, rs.First().GetValue<Phone>("main_phone"));
            }
        }

        [Test]
        public void MappingEncodingNestedTest()
        {
            foreach (var protocolVersion in UdtProtocolVersionSupported)
            {
                //Use all possible protocol versions
                Cluster.MaxProtocolVersion = protocolVersion;
                //Use a local cluster
                var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
                var localSession = localCluster.Connect("tester");
                localSession.UserDefinedTypes.Define(
                    UdtMap.For<Phone>(),
                    UdtMap.For<Contact>()
                          .Map(c => c.FirstName, "first_name")
                          .Map(c => c.LastName, "last_name")
                          .Map(c => c.Birth, "birth_date")
                    );

                var insert = new SimpleStatement("INSERT INTO users_contacts (id, contacts) values (?, ?)");

                //All of the fields null
                var id = 301;
                var contacts = new List<Contact>
                {
                    new Contact
                    {
                        FirstName = "Vincent", 
                        LastName = "Vega", 
                        Phones = new List<Phone>
                        {
                            new Phone {Alias = "Wat", Number = "0000000000121220000"},
                            new Phone {Alias = "Office", Number = "123"}
                        }
                    }
                };
                localSession.Execute(insert.Bind(id, contacts));
                var rs = localSession.Execute(new SimpleStatement("SELECT * FROM users_contacts WHERE id = ?").Bind(id));
                Assert.AreEqual(contacts, rs.First().GetValue<List<Contact>>("contacts"));
            }
        }

        private class Contact
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public DateTimeOffset Birth { get; set; }

            public string NotMappedProp { get; set; }

            public IEnumerable<Phone> Phones { get; set; }

            public IEnumerable<string> Emails { get; set; }

            public override bool Equals(object obj)
            {
                if (!(obj is Contact))
                {
                    return false;
                }
                var value = (Contact)obj;
                if (value.FirstName == this.FirstName &&
                    value.LastName == this.LastName &&
                    value.Birth == this.Birth &&
                    value.Birth == this.Birth)
                {
                    CollectionAssert.AreEqual(this.Emails, value.Emails);
                    CollectionAssert.AreEqual(this.Phones, value.Phones);
                    return true;
                }
                return false;
            }
        }

        private class Phone
        {
            public string Alias { get; set; }

            public string Number { get; set; }

            public int CountryCode { get; set; }

            public override bool Equals(object obj)
            {
                if (!(obj is Phone))
                {
                    return false;
                }
                var phoneValue = (Phone)obj;
                if (phoneValue.Alias == this.Alias &&
                    phoneValue.Number == this.Number &&
                    phoneValue.CountryCode == this.CountryCode)
                {
                    return true;
                }
                return false;
            }
        }
    }
}
