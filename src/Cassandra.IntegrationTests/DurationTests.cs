//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [Category("short"), TestDseVersion(5, 1)]
    public class DurationTests : SharedDseClusterTest
    {
        public static readonly string[] Values = 
        {
            "1y2mo",
            "-1y2mo",
            "1Y2MO",
            "2w",
            "2d10h",
            "2d",
            "30h",
            "30h20m",
            "20m",
            "56s",
            "567ms",
            "1950us",
            "1950µs",
            "1950000ns",
            "1950000NS",
            "-1950000ns",
            "1y3mo2h10m",
            "P1Y2D",
            "P1Y2M",
            "P2W",
            "P1YT2H",
            "-P1Y2M",
            "P2D",
            "PT30H",
            "PT30H20M",
            "PT20M",
            "PT56S",
            "P1Y3MT2H10M",
            "P0001-00-02T00:00:00",
            "P0001-02-00T00:00:00",
            "P0001-00-00T02:00:00",
            "-P0001-02-00T00:00:00",
            "P0000-00-02T00:00:00",
            "P0000-00-00T30:00:00",
            "P0000-00-00T30:20:00",
            "P0000-00-00T00:20:00",
            "P0000-00-00T00:00:56",
            "P0001-03-00T02:10:00"
        };

        protected override string[] SetupQueries
        {
            get
            {
                return new []
                {
                    "CREATE TABLE tbl_duration (pk uuid PRIMARY KEY, c1 duration)",
                    "CREATE TYPE IF NOT EXISTS test_duration_udt (i int, c1 duration)",
                    "CREATE TABLE tbl_duration_udt_tuple (k uuid PRIMARY KEY, u test_duration_udt, uf frozen<test_duration_udt>," +
                    " t tuple<duration, int>, tf frozen<tuple<duration, int>>)",
                    "CREATE TABLE tbl_duration_collection (k uuid PRIMARY KEY, l list<duration>)"
                };
            }
        }

        [Test]
        public void Should_Deserialize_And_Serialize_DateRange()
        {
            const string insertQuery = "INSERT INTO tbl_duration (pk, c1) VALUES (?, ?)";
            const string selectQuery = "SELECT pk, c1 FROM tbl_duration WHERE pk = ?";
            foreach (var stringValue in Values)
            {
                var id = Guid.NewGuid();
                var value = Duration.Parse(stringValue);
                Session.Execute(new SimpleStatement(insertQuery, id, value));
                var rs = Session.Execute(new SimpleStatement(selectQuery, id));
                var row = rs.First();
                Assert.AreEqual(value, row.GetValue<Duration>("c1"));
                Assert.AreEqual(value.ToString(), row.GetValue<Duration>("c1").ToString());
            }
        }

        [Test]
        public void Should_Retrieve_Table_Metadata()
        {
            var tableMetadata = Cluster.Metadata.GetTable(KeyspaceName, "tbl_duration");
            Assert.NotNull(tableMetadata);
            var column = tableMetadata.ColumnsByName["c1"];
            Assert.AreEqual(ColumnTypeCode.Duration, column.TypeCode);
            Assert.Null(column.TypeInfo);
        }
        [Test]
        public void Should_Allow_Duration_In_Udt_And_Tuple()
        {
            const string insertQuery = "INSERT INTO tbl_duration_udt_tuple (k, u, uf, t, tf) VALUES (?,?,?,?,?)";
            const string selectQuery = "SELECT * FROM tbl_duration_udt_tuple WHERE k = ?";

            foreach (var value in Values)
            {
                var id = Guid.NewGuid();
                var durationExpected = Duration.Parse(value);

                Session.UserDefinedTypes.Define(
                    UdtMap.For<UdtDuration>("test_duration_udt")
                        .Map(v => v.Id, "i")
                        .Map(v => v.C1, "c1")
                );
                var udtRangeValue = new UdtDuration
                {
                    Id = 1,
                    C1 = durationExpected
                };
                var tuple1 = new Tuple<Duration, int>(durationExpected, 30);
                Session.Execute(new SimpleStatement(insertQuery, id, udtRangeValue, udtRangeValue, tuple1, tuple1));
                var rs = Session.Execute(new SimpleStatement(selectQuery, id));
                var row = rs.First();
                Assert.AreEqual(udtRangeValue, row.GetValue<UdtDuration>("u"));
                Assert.AreEqual(udtRangeValue, row.GetValue<UdtDuration>("uf"));
                Assert.AreEqual(tuple1, row.GetValue<Tuple<Duration, int>>("t"));
                Assert.AreEqual(tuple1, row.GetValue<Tuple<Duration, int>>("tf"));
            }
        }

        [Test]
        public void Should_Allow_Duration_In_Collections()
        {
            const string insertQuery = "INSERT INTO tbl_duration_collection (k, l) VALUES (?,?)";
            const string selectQuery = "SELECT * FROM tbl_duration_collection WHERE k = ?";

            foreach (var value in Values)
            {
                var id = Guid.NewGuid();
                var durationExpected = Duration.Parse(value);
                var durationExpected2 = Duration.Parse("1950000ns");

                var list = new List<Duration> { durationExpected, durationExpected2 };
                Session.Execute(new SimpleStatement(insertQuery, id, list));
                var rs = Session.Execute(new SimpleStatement(selectQuery, id));
                var row = rs.First();
                Assert.AreEqual(id, row.GetValue<Guid>("k"));
                Assert.AreEqual(list, row.GetValue<List<Duration>>("l"));
            }
            
        }

        [Test]
        public void Should_Allow_Duration_In_Prepared_Statements()
        {
            foreach (var value in Values)
            {
                var durationExpected = Duration.Parse(value);
                var id = Guid.NewGuid();

                const string insertQuery = "INSERT INTO tbl_duration (pk, c1) VALUES (?,?)";
                const string selectQuery = "SELECT * FROM tbl_duration WHERE pk = ?";

                var preparedStatement = Session.Prepare(insertQuery);
                var preparedSelectStatement = Session.Prepare(selectQuery);

                Session.Execute(preparedStatement.Bind(id, durationExpected));
                var rs = Session.Execute(preparedSelectStatement.Bind(id));
                var row = rs.First();
                Assert.AreEqual(id, row.GetValue<Guid>("pk"));
                Assert.AreEqual(durationExpected, row.GetValue<Duration>("c1"));
            }
        }
    }

    class UdtDuration
    {
        public int Id { get; set; }

        public Duration C1 { get; set; }

        public override bool Equals(object obj)
        {
            if (!(obj is UdtDuration))
            {
                return false;
            }
            var dataRange = (UdtDuration)obj;
            if (dataRange.Id == this.Id &&
                dataRange.C1 == this.C1)
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
