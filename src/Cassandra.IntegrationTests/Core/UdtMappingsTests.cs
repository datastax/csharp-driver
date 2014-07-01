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
            }
        }

        [TestCassandraVersion(2, 1)]
        [Test]
        public void MappingSimpleTest()
        {
            Session.UserDefinedTypes.Define(UdtMap.For<Phone>().Map(v => v.Alias, "alias"));
            Session.Execute("INSERT INTO users (id, main_phone) values (1, {alias: 'home phone'})");
            var rs = Session.Execute("SELECT * FROM users WHERE id = 1");
            var row = rs.First();
            Assert.NotNull(row.GetValue<Phone>("main_phone"));
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
