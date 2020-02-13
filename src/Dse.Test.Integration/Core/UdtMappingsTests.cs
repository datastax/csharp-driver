//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dse.Test.Unit;

namespace Dse.Test.Integration.Core
{
    [Category("short"), Category("realcluster")]
    public class UdtMappingsTests : SharedClusterTest
    {
        const string CqlType1 = "CREATE TYPE phone (alias text, number text, country_code int, verified_at timestamp, phone_type text)";
        const string CqlType2 = "CREATE TYPE contact (first_name text, last_name text, birth_date timestamp, phones set<frozen<phone>>, emails set<text>, nullable_long bigint)";
        const string CqlTable1 = "CREATE TABLE users (id int PRIMARY KEY, main_phone frozen<phone>)";
        const string CqlTable2 = "CREATE TABLE users_contacts (id int PRIMARY KEY, contacts list<frozen<contact>>)";

        const string CqlType3 = "CREATE TYPE udt_collections (Id int, NullableId int, IntEnumerable list<int>, IntEnumerableSet set<int>, NullableIntEnumerable list<int>, " +
                                "NullableIntList list<int>, IntReadOnlyList list<int>, IntIList list<int>, IntList list<int>)";
        const string CqlTable3 = "CREATE TABLE table_udt_collections (id int PRIMARY KEY, nullable_id int, udt frozen<udt_collections>, udt_list list<frozen<udt_collections>>)";

        public override void OneTimeSetUp()
        {
            if (CassandraVersion < Version.Parse("2.1.0"))
                Assert.Ignore("Requires Cassandra version >= 2.1");

            base.OneTimeSetUp();

            Session.Execute(UdtMappingsTests.CqlType1);
            Session.Execute(UdtMappingsTests.CqlType2);
            Session.Execute(UdtMappingsTests.CqlType3);
            Session.Execute(UdtMappingsTests.CqlTable1);
            Session.Execute(UdtMappingsTests.CqlTable2);
            Session.Execute(UdtMappingsTests.CqlTable3);
        }

        [Test]
        public void MappingSingleExplicitTest()
        {
            var localSession = GetNewTemporarySession(KeyspaceName);
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
            var localSession = GetNewTemporarySession(KeyspaceName);
            await localSession.UserDefinedTypes.DefineAsync(
                UdtMap.For<Phone>("phone")
                      .Map(v => v.Alias, "alias")
                      .Map(v => v.CountryCode, "country_code")
                      .Map(v => v.Number, "number")
            ).ConfigureAwait(false);
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
        public async Task MappingDifferentKeyspaceSingleExplicitAsync_AsParameter()
        {
            const string cqlType1 = "CREATE TYPE phone2 (alias2 text, number2 text, country_code2 int, verified_at timestamp, phone_type text)";
            const string cqlTable1 = "CREATE TABLE users2 (id int PRIMARY KEY, main_phone frozen<phone2>)";

            var cluster = GetNewTemporaryCluster();
            var newKeyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            var session = cluster.Connect();
            session.CreateKeyspaceIfNotExists(newKeyspace);
            session.ChangeKeyspace(newKeyspace);

            session.Execute(cqlType1);
            session.Execute(cqlTable1);

            await session.UserDefinedTypes.DefineAsync(
                UdtMap.For<Phone>("phone", KeyspaceName)
                      .Map(v => v.Alias, "alias")
                      .Map(v => v.CountryCode, "country_code")
                      .Map(v => v.Number, "number"),
                UdtMap.For<Phone2>("phone2")
                      .Map(v => v.Alias, "alias2")
                      .Map(v => v.CountryCode, "country_code2")
                      .Map(v => v.Number, "number2")
            ).ConfigureAwait(false);
            var phone = new Phone
            {
                Alias = "home phone",
                Number = "85 988888888",
                CountryCode = 55
            };
            var phone2 = new Phone2
            {
                Alias = "home phone2",
                Number = "85 988888811",
                CountryCode = 66
            };

            session.Execute(new SimpleStatement($"INSERT INTO {KeyspaceName}.users (id, main_phone) values (1, ?)", phone));
            var rs = session.Execute($"SELECT * FROM {KeyspaceName}.users WHERE id = 1");
            var row = rs.First();
            var value = row.GetValue<Phone>("main_phone");
            session.Execute(new SimpleStatement("INSERT INTO users2 (id, main_phone) values (1, ?)", phone2));
            rs = session.Execute("SELECT * FROM users2 WHERE id = 1");
            row = rs.First();
            var value2 = row.GetValue<Phone2>("main_phone");

            Assert.AreEqual(phone, value);
            Assert.AreEqual(phone2, value2);
        }

        [Test]
        public void MappingDifferentKeyspaceWithoutSpecifyingIt()
        {
            const string cqlType1 = "CREATE TYPE phone2 (alias2 text, number2 text, country_code2 int, verified_at timestamp, phone_type text)";
            const string cqlTable1 = "CREATE TABLE users2 (id int PRIMARY KEY, main_phone frozen<phone2>)";

            var cluster = GetNewTemporaryCluster();
            var newKeyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            var session = cluster.Connect();
            session.CreateKeyspaceIfNotExists(newKeyspace);
            session.ChangeKeyspace(newKeyspace);
            
            session.Execute(cqlType1);
            session.Execute(cqlTable1);

            Assert.ThrowsAsync<InvalidTypeException>(() => session.UserDefinedTypes.DefineAsync(
                UdtMap.For<Phone>("phone")
                      .Map(v => v.Alias, "alias")
                      .Map(v => v.CountryCode, "country_code")
                      .Map(v => v.Number, "number"),
                UdtMap.For<Phone2>("phone2")
                      .Map(v => v.Alias, "alias2")
                      .Map(v => v.CountryCode, "country_code2")
                      .Map(v => v.Number, "number2")
            ));
        }

        [Test]
        public async Task MappingSingleExplicitAsync_AsParameter()
        {
            var localSession = GetNewTemporarySession(KeyspaceName);
            await localSession.UserDefinedTypes.DefineAsync(
                UdtMap.For<Phone>("phone")
                      .Map(v => v.Alias, "alias")
                      .Map(v => v.CountryCode, "country_code")
                      .Map(v => v.Number, "number")
            ).ConfigureAwait(false);
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
            var localSession = GetNewTemporarySession(KeyspaceName);
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
            var localSession = GetNewTemporarySession(KeyspaceName);
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
            var localSession = GetNewTemporarySession(KeyspaceName);
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
            var localSession = GetNewTemporarySession(KeyspaceName);
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
            var localSession = GetNewTemporarySession(KeyspaceName);
            Assert.Throws<InvalidTypeException>(() => localSession.UserDefinedTypes.Define(
                //there is no field named like this
                UdtMap.For<Phone>("phone").Map(v => v.Number, "Alias_X_WTF")
                ));
        }

        [Test]
        public void MappingEncodingSingleTest()
        {
            var localSession = GetNewTemporarySession(KeyspaceName);
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
            var localSession = GetNewTemporarySession(KeyspaceName);
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

            var localSession = GetNewTemporarySession(KeyspaceName);
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

        [Test]
        public void MappingGetChildClassTest()
        {
            var localSession = GetNewTemporarySession(KeyspaceName);
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            localSession.Execute($"CREATE TABLE {tableName} (id uuid PRIMARY KEY, main_phone frozen<phone>, phones list<frozen<phone>>)");
            localSession.UserDefinedTypes.Define(
                UdtMap.For<Phone2>("phone")
                      .Map(v => v.Alias, "alias")
                      .Map(v => v.CountryCode, "country_code")
                      .Map(v => v.Number, "number")
                      .Map(v => v.VerifiedAt, "verified_at")
                      .Map(v => v.PhoneType, "phone_type"));
            var id = Guid.NewGuid();
            var date = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(100000);
            var phoneCql = "{alias: 'home phone', number: '123', country_code: 34, verified_at: '100000', phone_type: 'Home'}";
            localSession.Execute($"INSERT INTO {tableName} (id, phones, main_phone) values ({id}, [{phoneCql}], {phoneCql})");
            var rs = localSession.Execute(new SimpleStatement($"SELECT * FROM {tableName} WHERE id = ?", id));
            var row = rs.First();

            void AssertAreEqualPhone(Phone v)
            {
                Assert.AreEqual("home phone", v.Alias);
                Assert.AreEqual("123", v.Number);
                Assert.AreEqual(34, v.CountryCode);
                Assert.AreEqual(date, v.VerifiedAt);
                Assert.AreEqual(PhoneType.Home, v.PhoneType);
            }

            var value = row.GetValue<Phone>("main_phone");
            Assert.NotNull(value);
            AssertAreEqualPhone(value);
            
            var valueChild = row.GetValue<Phone2>("main_phone");
            Assert.NotNull(valueChild);
            AssertAreEqualPhone(valueChild);

            var valueList = row.GetValue<IEnumerable<Phone>>("phones");
            var valueInsideList = valueList.Single();
            Assert.NotNull(valueInsideList);
            AssertAreEqualPhone(valueInsideList);
            
            var valueListChild = row.GetValue<IEnumerable<Phone2>>("phones");
            var valueInsideListChild = valueListChild.Single();
            Assert.NotNull(valueInsideListChild);
            AssertAreEqualPhone(value);
        }

        [Test]
        public void Should_ExecuteSelectsAndInserts_When_UdtWithCollections()
        {
            var localSession = GetNewTemporarySession(KeyspaceName);
            localSession.UserDefinedTypes.Define(UdtMap.For<UdtWithCollections>("udt_collections"));

            var udtList = new List<UdtWithCollections>
            {
                // nulls
                new UdtWithCollections
                {
                    Id = 1
                },

                // collections with elements
                new UdtWithCollections
                {
                    Id = 2,
                    NullableId = 4,
                    IntEnumerable = new [] { 1, 10 },
                    IntEnumerableSet = new [] { 2, 20 },
                    IntIList = new List<int> { 3, 30 },
                    IntList = new List<int> { 4, 40 },
                    IntReadOnlyList = new List<int> { 5, 50 },
                    NullableIntEnumerable = new int?[] { 6, 60 },
                    NullableIntList = new List<int?> { 7, 70 }
                },

                // empty collections
                new UdtWithCollections
                {
                    Id = 3,
                    NullableId = 5,
                    IntEnumerable = new int[0],
                    IntEnumerableSet = new int[0],
                    IntIList = new List<int>(),
                    IntList = new List<int>(),
                    IntReadOnlyList = new List<int>(),
                    NullableIntEnumerable = new int?[0],
                    NullableIntList = new List<int?>()
                }
            };

            var insert = new SimpleStatement("INSERT INTO table_udt_collections (id, udt, udt_list, nullable_id) values (?, ?, ?, ?)", 1, udtList[0], udtList, 100);
            localSession.Execute(insert);
            insert = new SimpleStatement("INSERT INTO table_udt_collections (id, udt, udt_list) values (?, ?, ?)", 2, udtList[1], udtList);
            localSession.Execute(insert);
            insert = new SimpleStatement("INSERT INTO table_udt_collections (id, udt, udt_list) values (?, ?, ?)", 3, udtList[2], udtList);
            localSession.Execute(insert);
            insert = new SimpleStatement("INSERT INTO table_udt_collections (id, udt, udt_list) values (?, ?, ?)", 4, null, null);
            localSession.Execute(insert);

            var rs = localSession.Execute(new SimpleStatement("SELECT * FROM table_udt_collections WHERE id = ?", 1)).ToList();
            CollectionAssert.AreEqual(udtList, rs.Single().GetValue<List<UdtWithCollections>>("udt_list"));
            Assert.AreEqual(udtList[0], rs.Single().GetValue<UdtWithCollections>("udt"));
            Assert.AreEqual(100, rs.Single().GetValue<int?>("nullable_id"));

            var ps = localSession.Prepare("SELECT * FROM table_udt_collections WHERE id = ?");
            rs = localSession.Execute(ps.Bind(2)).ToList();
            CollectionAssert.AreEqual(udtList, rs.Single().GetValue<List<UdtWithCollections>>("udt_list"));
            Assert.AreEqual(udtList[1], rs.Single().GetValue<UdtWithCollections>("udt"));
            Assert.IsNull(rs.Single().GetValue<int?>("nullable_id"));

            rs = localSession.Execute(ps.Bind(3)).ToList();
            CollectionAssert.AreEqual(udtList, rs.Single().GetValue<List<UdtWithCollections>>("udt_list"));
            Assert.AreEqual(udtList[2], rs.Single().GetValue<UdtWithCollections>("udt"));

            rs = localSession.Execute(ps.Bind(4)).ToList();
            Assert.IsNull(rs.Single().GetValue<List<UdtWithCollections>>("udt_list"));
            Assert.IsNull(rs.Single().GetValue<UdtWithCollections>("udt"));
        }

        [Test]
        public void Should_ThrowException_When_UdtWithCollectionsWithNullValues()
        {
            var localSession = GetNewTemporarySession(KeyspaceName);
            localSession.UserDefinedTypes.Define(UdtMap.For<UdtWithCollections>("udt_collections"));

            var udtList = new List<UdtWithCollections>
            {
                // collections with null elements
                new UdtWithCollections
                {
                    Id = 1111,
                    NullableId = 4444,
                    NullableIntEnumerable = new int?[] { 6, null },
                    NullableIntList = new List<int?> { 7, null }
                },
            };

            var insert = new SimpleStatement("INSERT INTO table_udt_collections (id, udt, udt_list, nullable_id) values (?, ?, ?, ?)", 1111, null, udtList, 1000);
            Assert.Throws<InvalidCastException>(() => localSession.Execute(insert));
            insert = new SimpleStatement("INSERT INTO table_udt_collections (id, udt, udt_list) values (?, ?, ?)", 2222, udtList[0], null);
            Assert.Throws<InvalidCastException>(() => localSession.Execute(insert));
            insert = new SimpleStatement("INSERT INTO table_udt_collections (id, udt, udt_list) values (?, ?, ?)", 3333, null, new List<UdtWithCollections> {null});
            Assert.Throws<ArgumentNullException>(() => localSession.Execute(insert));
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
        
        private class UdtWithCollections : IEquatable<UdtWithCollections>
        {
            public int Id { get; set; }

            public int? NullableId { get; set; }

            public IEnumerable<int> IntEnumerable { get; set; }

            public IEnumerable<int> IntEnumerableSet { get; set; }

            public IEnumerable<int?> NullableIntEnumerable { get; set; }

            public List<int?> NullableIntList { get; set; }

            public IReadOnlyList<int> IntReadOnlyList { get; set; }

            public IList<int> IntIList { get; set; }

            public List<int> IntList { get; set; }

            public bool Equals(UdtWithCollections other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Id == other.Id 
                       && NullableId == other.NullableId
                       && CollectionEquals(IntEnumerable, other.IntEnumerable) 
                       && CollectionEquals(IntEnumerableSet, other.IntEnumerableSet) 
                       && CollectionEquals(NullableIntEnumerable, other.NullableIntEnumerable) 
                       && CollectionEquals(NullableIntList, other.NullableIntList) 
                       && CollectionEquals(IntReadOnlyList, other.IntReadOnlyList) 
                       && CollectionEquals(IntIList, other.IntIList) 
                       && CollectionEquals(IntList, other.IntList);
            }

            private static bool CollectionEquals<T>(IEnumerable<T> list1, IEnumerable<T> list2)
            {
                if (list1 == null)
                {
                    return list2 == null;
                }

                if (list2 == null)
                {
                    return false;
                }

                return list1.SequenceEqual(list2);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((UdtWithCollections) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = Id;
                    hashCode = (hashCode * 397) ^ (NullableId != null ? NullableId.Value : 0);
                    hashCode = (hashCode * 397) ^ (IntEnumerable != null ? IntEnumerable.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (IntEnumerableSet != null ? IntEnumerableSet.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (NullableIntEnumerable != null ? NullableIntEnumerable.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (NullableIntList != null ? NullableIntList.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (IntReadOnlyList != null ? IntReadOnlyList.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (IntIList != null ? IntIList.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (IntList != null ? IntList.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }

        private class Phone2 : Phone, IEquatable<Phone2>
        {
            public bool Equals(Phone2 other)
            {
                return base.Equals(other);
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
