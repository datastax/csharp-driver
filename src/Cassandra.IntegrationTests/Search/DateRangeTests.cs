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

using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Search;

using Newtonsoft.Json;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Search
{
    [Category("short"), TestDseVersion(5, 1)]
    public class DateRangeTests : SharedClusterTest
    {
        private static readonly string[] Values = new[]
        {
            "[0001-01-01T00:00:00.000Z TO 9999-12-31T23:59:59.999Z]", // min TO max dates supported
            "[0021-04-20T01 TO 0077-10-13T02]", // 4 digits years
            "2010",
            "[2017-02 TO *]",
            "[1 TO 2]",
            "[1200 TO 2017-07-04T16]",
            "[2015-02 TO 2016-02]",
            "[2016-10-01T08 TO *]",
            "[* TO 2016-03-01T16:56:39.999]",
            "[2016-03-01T16:56 TO *]",
            "[* TO *]",
            "*"
        };

        protected override string[] SetupQueries
        {
            get
            {
                return new[]
                {
                    "CREATE TABLE tbl_date_range (pk uuid PRIMARY KEY, c1 'DateRangeType')",
                    "INSERT INTO tbl_date_range (pk, c1) VALUES (uuid(), '[2010-12-03 TO 2010-12-04]')",
                    "CREATE TYPE IF NOT EXISTS test_udt (i int, range 'DateRangeType')",
                    "CREATE TABLE tbl_udt_tuple (k uuid PRIMARY KEY, u test_udt, uf frozen<test_udt>," +
                    " t tuple<'DateRangeType', int>, tf frozen<tuple<'DateRangeType', int>>)",
                    "CREATE TABLE tbl_collection (k uuid PRIMARY KEY, l list<'DateRangeType'>," +
                    " s set<'DateRangeType'>, m0 map<text, 'DateRangeType'>, m1 map<'DateRangeType', text>)",
                    "CREATE TABLE tbl_date_range_pk (k 'DateRangeType' PRIMARY KEY, v int)"
                };
            }
        }

        [Test]
        public void Should_Deserialize_And_Serialize_DateRange()
        {
            const string insertQuery = "INSERT INTO tbl_date_range (pk, c1) VALUES (?, ?)";
            const string selectQuery = "SELECT pk, c1 FROM tbl_date_range WHERE pk = ?";
            foreach (var stringValue in Values)
            {
                var id = Guid.NewGuid();
                var value = DateRange.Parse(stringValue);
                Session.Execute(new SimpleStatement(insertQuery, id, value));
                var rs = Session.Execute(new SimpleStatement(selectQuery, id));
                var row = rs.First();
                Assert.AreEqual(value, row.GetValue<DateRange>("c1"));
                Assert.AreEqual(value.ToString(), row.GetValue<DateRange>("c1").ToString());
            }
        }

        /// <summary>
        /// Test if the driver throws Exception when using a wrong order of dates in DataRange
        /// </summary>
        [Test]
        public void Should_Disallow_Invalid_DateRange_Order()
        {
            const string insertQuery = "INSERT INTO tbl_date_range (pk, c1) VALUES (?, ?)";
            var id = Guid.NewGuid();
            var value = DateRange.Parse("[0077-10-13T02 TO 0021-04-20T01]");
            //should throw ServerErrorException :
            //         java.lang.IllegalArgumentException: Wrong order: 0077-10-13T02 TO 0021-04-20T01
            Assert.Throws<ServerErrorException>(() => Session.Execute(new SimpleStatement(insertQuery, id, value)),
                            "java.lang.IllegalArgumentException: Wrong order: 0077-10-13T02 TO 0021-04-20T01");
        }

        /// <summary>
        /// Test if the DateRange returned in a JSON is parsable
        /// </summary>
        [Test]
        public void Should_Parse_DateRange_In_JSON()
        {
            const string insertQuery = "INSERT INTO tbl_date_range (pk, c1) VALUES (?, ?)";
            const string selectQuery = "SELECT JSON c1 FROM tbl_date_range WHERE pk = ?";
            foreach (var stringValue in Values)
            {
                var id = Guid.NewGuid();
                var value = DateRange.Parse(stringValue);
                Session.Execute(new SimpleStatement(insertQuery, id, value));
                var rs = Session.Execute(new SimpleStatement(selectQuery, id));
                var row = rs.First();
                var jsonString = row.GetValue<string>("[json]");
                dynamic dynamicJsonObj = JsonConvert.DeserializeObject(jsonString);
                Assert.AreEqual(value, DateRange.Parse(dynamicJsonObj.c1.ToString()));
            }
        }

        [Test]
        public void Should_Allow_DataRange_In_Udt_And_Tuple()
        {
            const string insertQuery = "INSERT INTO tbl_udt_tuple (k, u, uf, t, tf) VALUES (?,?,?,?,?)";
            const string selectQuery = "SELECT * FROM tbl_udt_tuple WHERE k = ?";

            var id = Guid.NewGuid();
            var dtExpected = DateRange.Parse("[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]");

            Session.UserDefinedTypes.Define(
                UdtMap.For<UdtDataRange>("test_udt")
                    .Map(v => v.Id, "i")
                    .Map(v => v.DateRange, "range")
            );
            var udtRangeValue = new UdtDataRange
            {
                Id = 1,
                DateRange = dtExpected
            };
            var tuple1 = new Tuple<DateRange, int>(dtExpected, 30);
            var tuple2 = new Tuple<DateRange, int>(dtExpected, 40);
            Session.Execute(new SimpleStatement(insertQuery, id, udtRangeValue, udtRangeValue, tuple1, tuple2));
            var rs = Session.Execute(new SimpleStatement(selectQuery, id));
            var row = rs.First();
            Assert.AreEqual(udtRangeValue, row.GetValue<UdtDataRange>("u"));
            Assert.AreEqual(udtRangeValue, row.GetValue<UdtDataRange>("uf"));
            Assert.AreEqual(tuple1, row.GetValue<Tuple<DateRange, int>>("t"));
            Assert.AreEqual(tuple2, row.GetValue<Tuple<DateRange, int>>("tf"));
        }

        [Test]
        public void Should_Allow_DataRange_In_Collections()
        {
            const string insertQuery = "INSERT INTO tbl_collection (k, l, s, m0, m1) VALUES (?,?,?,?,?)";
            const string selectQuery = "SELECT * FROM tbl_collection WHERE k = ?";

            var id = Guid.NewGuid();
            var dtExpected = DateRange.Parse("[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]");
            var dtExpected2 = DateRange.Parse("[0021-04-20T01 TO 0077-10-13T02]");

            var set = new HashSet<DateRange> { dtExpected, dtExpected2 };
            var list = new List<DateRange> { dtExpected, dtExpected2 };
            var map = new Dictionary<string, DateRange>();
            var mapReverse = new Dictionary<DateRange, string>();
            map.Add("key", dtExpected);
            mapReverse.Add(dtExpected, "value");

            Session.Execute(new SimpleStatement(insertQuery, id, list, set, map, mapReverse));
            var rs = Session.Execute(new SimpleStatement(selectQuery, id));
            var row = rs.First();
            Assert.AreEqual(id, row.GetValue<Guid>("k"));
            Assert.AreEqual(list, row.GetValue<List<DateRange>>("l"));
            Assert.AreEqual(set, row.GetValue<HashSet<DateRange>>("s"));
            Assert.AreEqual(map, row.GetValue<IDictionary<string, DateRange>>("m0"));
            Assert.AreEqual(mapReverse, row.GetValue<IDictionary<DateRange, string>>("m1"));
        }

        [Test]
        public void Should_Allow_DataRange_In_as_Primary_Key()
        {
            //CREATE TABLE tbl_date_range_pk (k 'DateRangeType' PRIMARY KEY, v int)
            var dtExpected = DateRange.Parse("[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]");
            const string insertQuery = "INSERT INTO tbl_date_range_pk (k, v) VALUES (?,?)";
            const string selectQuery = "SELECT * FROM tbl_date_range_pk WHERE k = ?";

            Session.Execute(new SimpleStatement(insertQuery, dtExpected, 1));
            var rs = Session.Execute(new SimpleStatement(selectQuery, dtExpected));
            var row = rs.First();
            Assert.AreEqual(dtExpected, row.GetValue<DateRange>("k"));
            Assert.AreEqual(1, row.GetValue<int>("v"));
        }

        [Test]
        public void Should_Allow_DataRange_In_Prepared_Statements()
        {
            //"INSERT INTO tbl_date_range (pk, c1) VALUES (uuid(), '[2010-12-03 TO 2010-12-04]')"
            var dtExpected = DateRange.Parse("[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]");
            var id = Guid.NewGuid();

            const string insertQuery = "INSERT INTO tbl_date_range (pk, c1) VALUES (?,?)";
            const string selectQuery = "SELECT * FROM tbl_date_range WHERE pk = ?";

            var preparedStatement = Session.Prepare(insertQuery);
            var preparedSelectStatement = Session.Prepare(selectQuery);

            Session.Execute(preparedStatement.Bind(id, dtExpected));
            var rs = Session.Execute(preparedSelectStatement.Bind(id));
            var row = rs.First();
            Assert.AreEqual(id, row.GetValue<Guid>("pk"));
            Assert.AreEqual(dtExpected, row.GetValue<DateRange>("c1"));
        }
    }

    internal class UdtDataRange
    {
        public int Id { get; set; }

        public DateRange DateRange { get; set; }

        public override bool Equals(object obj)
        {
            if (!(obj is UdtDataRange))
            {
                return false;
            }
            var dataRange = (UdtDataRange)obj;
            if (dataRange.Id == this.Id &&
                dataRange.DateRange == this.DateRange)
            {
                return true;
            }
            return false;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}