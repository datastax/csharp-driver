//
//      Copyright (C) 2012-2014 DataStax Inc.
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

using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class UdtMappingsTests : SharedClusterTest
    {
        public override void OneTimeSetUp()
        {
            if (CassandraVersion < Version.Parse("2.1.0"))
                Assert.Ignore("Requires Cassandra version >= 2.1");

            base.OneTimeSetUp();

            const string cqlType1 = "CREATE TYPE phone (alias text, number text, country_code int, verified_at timestamp, phone_type text)";
            const string cqlType2 = "CREATE TYPE contact (first_name text, last_name text, birth_date timestamp, phones set<frozen<phone>>, emails set<text>, nullable_long bigint)";
            const string cqlTable1 = "CREATE TABLE users (id int PRIMARY KEY, main_phone frozen<phone>)";
            const string cqlTable2 = "CREATE TABLE users_contacts (id int PRIMARY KEY, contacts list<frozen<contact>>)";

            Session.Execute(cqlType1);
            Session.Execute(cqlType2);
            Session.Execute(cqlTable1);
            Session.Execute(cqlTable2);
        }

        [Test]
        public void MappingSingleExplicitTest()
        {
            var localSession = GetNewSession(KeyspaceName);
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone>("phone")
                    .Map(v => v.Alias, "alias")
                    .Map(v => v.CountryCode, "country_code")
                    .Map(v => v.Number, "number")
                    .Map(v => v.VerifiedAt, "verified_at")
                    .Map(v => v.PhoneType, "phone_type")
            );
            var date = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(100000);
            localSession.Execute($"INSERT INTO users (id, main_phone) values (1, {{alias: 'home phone', number: '123', country_code: 34, verified_at: '100000', phone_type: 'Home'}})");
            var rs = localSession.Execute("SELECT * FROM users WHERE id = 1");
            var row = rs.First();
            var value = row.GetValue<Phone>("main_phone");
            Assert.NotNull(value);
            Assert.AreEqual("home phone", value.Alias);
            Assert.AreEqual("123", value.Number);
            Assert.AreEqual(34, value.CountryCode);
            Assert.AreEqual(date, value.VerifiedAt);
            Assert.AreEqual(PhoneType.Home, value.PhoneType);
        }

        [Test]
        public async Task MappingSingleExplicitTestAsync()
        {
            var localSession = GetNewSession(KeyspaceName);
            await localSession.UserDefinedTypes.DefineAsync(
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

        [Test]
        public async Task MappingSingleExplicitAsync_AsParameter()
        {
            var localSession = GetNewSession(KeyspaceName);
            await localSession.UserDefinedTypes.DefineAsync(
                UdtMap.For<Phone>("phone")
                      .Map(v => v.Alias, "alias")
                      .Map(v => v.CountryCode, "country_code")
                      .Map(v => v.Number, "number")
            );
            var phone = new Phone
            {
                Alias = "home phone",
                Number = "85 988888888",
                CountryCode = 55
            };
            localSession.Execute(new SimpleStatement("INSERT INTO users (id, main_phone) values (1, ?)", phone));
            var rs = localSession.Execute("SELECT * FROM users WHERE id = 1");
            var row = rs.First();
            var value = row.GetValue<Phone>("main_phone");
            Assert.AreEqual(phone, value);
        }

        [Test]
        public void MappingSingleExplicitNullsTest()
        {
            var localSession = GetNewSession(KeyspaceName);
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

            //first values are null
            localSession.Execute("INSERT INTO users (id, main_phone) values (3, {country_code: 34})");
            row = localSession.Execute("SELECT * FROM users WHERE id = 3").First();
            Assert.IsNotNull(row.GetValue<Phone>("main_phone"));
            Assert.AreEqual(34, row.GetValue<Phone>("main_phone").CountryCode);
            Assert.IsNull(row.GetValue<Phone>("main_phone").Alias);
            Assert.IsNull(row.GetValue<Phone>("main_phone").Number);
        }

        [Test]
        public void MappingSingleImplicitTest()
        {
            var localSession = GetNewSession(KeyspaceName);
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

        [Test]
        public void MappingNestedTypeTest()
        {
            var localSession = GetNewSession(KeyspaceName);
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone>(),
                UdtMap.For<Contact>()
                        .Map(c => c.FirstName, "first_name")
                        .Map(c => c.LastName, "last_name")
                        .Map(c => c.Birth, "birth_date")
                        .Map(c => c.NullableLong, "nullable_long")
                );

            var insertedContacts = new List<Contact>
            {
                new Contact
                {
                    FirstName = "Jules", LastName = "Winnfield", 
                    Birth = new DateTimeOffset(1950, 2, 3, 4, 5, 0, 0, TimeSpan.Zero),
                    NullableLong = null,
                    Phones = new HashSet<Phone>{ new Phone { Alias = "home", Number = "123456" }}
                },
                new Contact
                {
                    FirstName = "Mia", LastName = "Wallace", 
                    Birth = null,
                    NullableLong = 2,
                    Phones = new HashSet<Phone>
                    {
                        new Phone { Alias = "mobile", Number = "789" },
                        new Phone { Alias = "office", Number = "123" }
                    }
                }
            };

            localSession.Execute(new SimpleStatement("INSERT INTO users_contacts (id, contacts) values (?, ?)", 1, insertedContacts));
            var rs = localSession.Execute("SELECT * FROM users_contacts WHERE id = 1");
            var row = rs.First();

            var contacts = row.GetValue<List<Contact>>("contacts");
            Assert.NotNull(contacts);
            Assert.AreEqual(2, contacts.Count);
            Assert.AreEqual(insertedContacts[0], contacts[0]);
            Assert.AreEqual(insertedContacts[1], contacts[1]);
        }

        [Test]
        public void MappingCaseSensitiveTest()
        {
            var localSession = GetNewSession(KeyspaceName);
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
        }

        [Test]
        public void MappingNotExistingFieldsTest()
        {
            var localSession = GetNewSession(KeyspaceName);
            Assert.Throws<InvalidTypeException>(() => localSession.UserDefinedTypes.Define(
                //there is no field named like this
                UdtMap.For<Phone>("phone").Map(v => v.Number, "Alias_X_WTF")
                ));
        }

        [Test]
        public void MappingEncodingSingleTest()
    {
            var localSession = GetNewSession(KeyspaceName);
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone>("phone")
                    .Map(v => v.Alias, "alias")
                    .Map(v => v.CountryCode, "country_code")
                    .Map(v => v.Number, "number")
            );

            const string insertQuery = "INSERT INTO users (id, main_phone) values (?, ?)";
                
            //All of the fields null
            var id = 201;
            var phone = new Phone();
            localSession.Execute(new SimpleStatement(insertQuery, id, phone));
            var rs = localSession.Execute(new SimpleStatement("SELECT * FROM users WHERE id = ?", id));
            Assert.AreEqual(phone, rs.First().GetValue<Phone>("main_phone"));

            //Some fields null and others with value
            id = 202;
            phone = new Phone() {Alias = "Home phone"};
            localSession.Execute(new SimpleStatement(insertQuery, id, phone));
            rs = localSession.Execute(new SimpleStatement("SELECT * FROM users WHERE id = ?", id));
            Assert.AreEqual(phone, rs.First().GetValue<Phone>("main_phone"));

            //All fields filled in
            id = 203;
            phone = new Phone() { Alias = "Mobile phone", CountryCode = 54, Number = "1234567"};
            localSession.Execute(new SimpleStatement(insertQuery, id, phone));
            rs = localSession.Execute(new SimpleStatement("SELECT * FROM users WHERE id = ?", id));
            Assert.AreEqual(phone, rs.First().GetValue<Phone>("main_phone"));
        }

        [Test]
        public void MappingEncodingNestedTest()
        {
            var localSession = GetNewSession(KeyspaceName);
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone>(),
                UdtMap.For<Contact>()
                        .Map(c => c.FirstName, "first_name")
                        .Map(c => c.LastName, "last_name")
                        .Map(c => c.Birth, "birth_date")
                );


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
            var insert = new SimpleStatement("INSERT INTO users_contacts (id, contacts) values (?, ?)", id, contacts);
            localSession.Execute(insert);
            var rs = localSession.Execute(new SimpleStatement("SELECT * FROM users_contacts WHERE id = ?", id));
            Assert.AreEqual(contacts, rs.First().GetValue<List<Contact>>("contacts"));
        }

        /// <summary>
        /// Checks that if no mapping defined, the driver gets out of the way.
        /// </summary>
        [Test]
        public void NoMappingDefinedTest()
        {
            const string cqlType = "CREATE TYPE temp_udt (text_sample text, date_sample timestamp)";
            const string cqlTable = "CREATE TABLE temp_table (id int PRIMARY KEY, sample_udt frozen<temp_udt>, sample_udt_list list<frozen<temp_udt>>)";
            const string cqlInsert = "INSERT INTO temp_table (id, sample_udt, sample_udt_list) VALUES (1, {text_sample: 'one', date_sample: 1}, [{text_sample: 'first'}])";

            var localSession = GetNewSession(KeyspaceName);
            localSession.Execute(cqlType);
            localSession.Execute(cqlTable);
            localSession.Execute(cqlInsert);

            var row = localSession.Execute("SELECT * from temp_table").First();

            Assert.IsNotNull(row.GetValue<object>("sample_udt"));
            Assert.IsInstanceOf<byte[]>(row.GetValue<object>("sample_udt"));

            Assert.IsNotNull(row.GetValue<object>("sample_udt_list"));
            Assert.IsInstanceOf<IEnumerable<byte[]>>(row.GetValue<object>("sample_udt_list"));

            row = localSession.Execute("SELECT id, sample_udt.text_sample from temp_table").First();
            Assert.AreEqual("one", row.GetValue<string>("sample_udt.text_sample"));

            //Trying to encode an unmapped type should throw
            var statement = new SimpleStatement("INSERT INTO temp_table (id, sample_udt) VALUES (?, ?)", 2, new DummyClass());
            Assert.Throws<InvalidTypeException>(() => localSession.Execute(statement));
        }

        [Test, TestCassandraVersion(3, 0, Comparison.LessThan)]
        public void MappingOnLowerProtocolVersionTest()
        {
            using (var cluster = Cluster.Builder()
                .AddContactPoint(TestCluster.InitialContactPoint)
                .WithMaxProtocolVersion(ProtocolVersion.V2)
                .Build())
            {
                var localSession = cluster.Connect(KeyspaceName);
                Assert.Throws<NotSupportedException>(() => localSession.UserDefinedTypes.Define(UdtMap.For<Phone>()));   
            }
        }

        private class Contact : IEquatable<Contact>
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public DateTimeOffset? Birth { get; set; }

            public string NotMappedProp { get; set; }

            public IEnumerable<Phone> Phones { get; set; }

            public IEnumerable<string> Emails { get; set; }

            public long? NullableLong { get; set; }

            public override bool Equals(object obj)
            {
                return Equals(obj as Contact);
            }

            public override int GetHashCode()
            {
                // We are not looking to use equality based on hashcode
                return base.GetHashCode();
            }

            public bool Equals(Contact other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return FirstName == other.FirstName && LastName == other.LastName && Birth == other.Birth && 
                       NotMappedProp == other.NotMappedProp && TestHelper.SequenceEqual(Phones, other.Phones) &&
                       TestHelper.SequenceEqual(Emails, other.Emails) && NullableLong == other.NullableLong;
            }

            public override string ToString()
            {
                return $"{nameof(FirstName)}: {FirstName}, {nameof(LastName)}: {LastName}, {nameof(Birth)}: {Birth}, " +
                       $"{nameof(NotMappedProp)}: {NotMappedProp}, {nameof(Phones)}: {Phones}, " +
                       $"{nameof(Emails)}: {Emails}, {nameof(NullableLong)}: {NullableLong}";
            }
        }

        private class Phone : IEquatable<Phone>
        {
            public string Alias { get; set; }

            public string Number { get; set; }

            public int CountryCode { get; set; }

            public DateTime VerifiedAt { get; set; }

            public PhoneType PhoneType { get; set; }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals(obj as Phone);
            }

            public override int GetHashCode()
            {
                // We are not looking to use equality based on hashcode
                return base.GetHashCode();
            }

            public bool Equals(Phone other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Alias == other.Alias && Number == other.Number && CountryCode == other.CountryCode &&
                       VerifiedAt.Equals(other.VerifiedAt) && PhoneType == other.PhoneType;
            }

            public override string ToString()
            {
                return $"{nameof(Alias)}: {Alias}, {nameof(Number)}: {Number}, {nameof(CountryCode)}: {CountryCode}, " +
                       $"{nameof(VerifiedAt)}: {VerifiedAt}, {nameof(PhoneType)}: {PhoneType}";
            }
        }

        private class DummyClass
        {
            
        }

        private enum PhoneType
        {
            Mobile, Home, Work
        }
    }
}
