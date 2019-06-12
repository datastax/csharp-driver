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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
 using Cassandra.IntegrationTests.TestBase;
 using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    [TestCassandraVersion(2, 1)]
    public class TupleTests : SharedClusterTest
    {
        private const string TableName = "users_tuples";

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            if (CassandraVersion < new Version(2, 1))
            {
                return;
            }
            var cqlTable1 = "CREATE TABLE " + TableName + " (id int PRIMARY KEY, phone frozen<tuple<text, text, int>>, achievements list<frozen<tuple<text,int>>>)";
            Session.Execute(cqlTable1);
        }

        [Test]
        public void DecodeTupleValuesSingleTest()
        {
            Session.Execute(
                "INSERT INTO " + TableName + " (id, phone) values " +
                "(1, " +
                "('home', '1234556', 1))");
            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 1").First();
            var phone1 = row.GetValue<Tuple<string, string, int>>("phone");
            var phone2 = row.GetValue<Tuple<string, string, int>>("phone");
            Assert.IsNotNull(phone1);
            Assert.IsNotNull(phone2);
            Assert.AreEqual("home", phone1.Item1);
            Assert.AreEqual("1234556", phone1.Item2);
            Assert.AreEqual(1, phone1.Item3);
        }

        [Test]
        public void DecodeTupleNullValuesSingleTest()
        {
            Session.Execute(
                "INSERT INTO " + TableName + " (id, phone) values " +
                "(11, " +
                "('MOBILE'))");
            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 11").First();
            var phone = row.GetValue<Tuple<string, string, int>>("phone");
            Assert.IsNotNull(phone);
            Assert.AreEqual("MOBILE", phone.Item1);
            Assert.AreEqual(null, phone.Item2);
            Assert.AreEqual(0, phone.Item3);

            Session.Execute(
                "INSERT INTO " + TableName + " (id, phone) values " +
                "(12, " +
                "(null, '1222345'))");
            row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 12").First();
            phone = row.GetValue<Tuple<string, string, int>>("phone");
            Assert.IsNotNull(phone);
            Assert.AreEqual(null, phone.Item1);
            Assert.AreEqual("1222345", phone.Item2);
            Assert.AreEqual(0, phone.Item3);
        }

        [Test]
        public void DecodeTupleAsNestedTest()
        {
            Session.Execute(
                "INSERT INTO " + TableName + " (id, achievements) values " +
                "(21, " +
                "[('Tenacious', 100), ('Altruist', 12)])");
            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 21").First();

            var achievements = row.GetValue<List<Tuple<string, int>>>("achievements");
            Assert.IsNotNull(achievements);
        }

        [Test]
        public void EncodeDecodeTupleAsNestedTest()
        {
            var achievements = new List<Tuple<string, int>>
            {
                new Tuple<string, int>("What", 1),
//                new Tuple<string, int>(null, 100),
//                new Tuple<string, int>(@"¯\_(ツ)_/¯", 150)
            };

            var insert = new SimpleStatement("INSERT INTO " + TableName + " (id, achievements) values (?, ?)", 31, achievements);
            Session.Execute(insert);
            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 31").First();

            Assert.AreEqual(achievements, row.GetValue<List<Tuple<string, int>>>("achievements"));
        }
    }
}
