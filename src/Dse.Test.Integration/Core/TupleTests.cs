//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Test.Integration.SimulacronAPI;
using Dse.Test.Integration.SimulacronAPI.Models.Logs;
using Dse.Test.Integration.TestClusterManagement;
 using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    [TestCassandraVersion(2, 1)]
    public class TupleTests : SimulacronTest
    {
        private const string TableName = "users_tuples";

        [Test]
        public void DecodeTupleValuesSingleTest()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 1")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Text, DataType.Text, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Text, DataType.Int)))
                          },
                          r => r.WithRow(1, new Tuple<string, string, int>("home", "1234556", 1), null)));

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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 11")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Text, DataType.Text, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Text, DataType.Int)))
                          },
                          r => r.WithRow(11, new Tuple<string, string, int?>("MOBILE", null, null), null)));
            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 11").First();
            var phone = row.GetValue<Tuple<string, string, int>>("phone");
            Assert.IsNotNull(phone);
            Assert.AreEqual("MOBILE", phone.Item1);
            Assert.AreEqual(null, phone.Item2);
            Assert.AreEqual(0, phone.Item3);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 12")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Text, DataType.Text, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Text, DataType.Int)))
                          },
                          r => r.WithRow(12, new Tuple<string, string, int?>(null, "1222345", null), null)));
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
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 21")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Text, DataType.Text, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Text, DataType.Int)))
                          },
                          r => r.WithRow(
                              21,
                              null,
                              new List<Tuple<string, int?>>
                              {
                                  new Tuple<string, int?>("Tenacious", 100),
                                  new Tuple<string, int?>("Altruist", 12)
                              })));
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
                new Tuple<string, int>(null, 100),
                new Tuple<string, int>(@"¯\_(ツ)_/¯", 150)
            };

            var insert = new SimpleStatement("INSERT INTO " + TableName + " (id, achievements) values (?, ?)", 31, achievements);
            Session.Execute(insert);

            VerifyStatement(
                QueryType.Query,
                "INSERT INTO " + TableName + " (id, achievements) values (?, ?)",
                1,
                31, achievements);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 31")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Varchar, DataType.Ascii, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Varchar, DataType.Int)))
                          },
                          r => r.WithRow(
                              31,
                              null,
                              achievements)));

            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 31").First();

            Assert.AreEqual(achievements, row.GetValue<List<Tuple<string, int>>>("achievements"));
        }
    }
}