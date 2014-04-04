﻿//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [TestClass]
    public class BasicTests
    {
        private Session Session;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder().WithCompression(CompressionType.LZ4));
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

        private void QueryBindingTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.WaitForSchemaAgreement(QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         label text,
         number int,        
         );", tableName)));
            }
            catch (AlreadyExistsException)
            {
            }
            var sst = new SimpleStatement(string.Format("INSERT INTO {0}(tweet_id,label,number) VALUES(?,?,?)", tableName));
            Session.Execute(sst.Bind(new object[] {Guid.NewGuid(), "label", 1}));
        }

        private void QueryFetchingTest(int rowsCount = 5000)
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.WaitForSchemaAgreement(QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         label text,        
         );", tableName)));
            }
            catch (AlreadyExistsException)
            {
            }
            for (int i = 0; i < rowsCount; i++)
                Session.Execute(string.Format("INSERT INTO {2}(tweet_id, label) VALUES({0},'{1}')", Guid.NewGuid(), "LABEL" + i, tableName));

            RowSet rs = Session.Execute("SELECT * FROM " + tableName);
            RowSet rs_without_paging = Session.Execute("SELECT * FROM " + tableName, int.MaxValue);
            int rs1 = 0;
            int rs2 = 0;

            using (rs)
            {
                foreach (Row row in rs.GetRows())
                    rs1++;
            }

            using (rs_without_paging)
            {
                foreach (Row row in rs_without_paging.GetRows())
                    rs2++;
            }

            Assert.True(rs1 == rs2 && rs1 == rowsCount);
        }


        public void ExceedingCassandraType(Type toExceed, Type toExceedWith, bool sameOutput = true)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(toExceed);
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.WaitForSchemaAgreement(QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         label text,
         number {1}
         );", tableName, cassandraDataTypeName)));
            }
            catch (AlreadyExistsException)
            {
            }


            object Minimum = toExceedWith.GetField("MinValue").GetValue(this);
            object Maximum = toExceedWith.GetField("MaxValue").GetValue(this);


            var row1 = new object[3] {Guid.NewGuid(), "Minimum", Minimum};
            var row2 = new object[3] {Guid.NewGuid(), "Maximum", Maximum};
            var toInsert_and_Check = new List<object[]>(2) {row1, row2};

            if (toExceedWith == typeof (Double) || toExceedWith == typeof (Single))
            {
                Minimum = Minimum.GetType().GetMethod("ToString", new[] {typeof (string)}).Invoke(Minimum, new object[1] {"r"});
                Maximum = Maximum.GetType().GetMethod("ToString", new[] {typeof (string)}).Invoke(Maximum, new object[1] {"r"});

                if (!sameOutput) //for ExceedingCassandra_FLOAT() test case
                {
                    toInsert_and_Check[0][2] = Single.NegativeInfinity;
                    toInsert_and_Check[1][2] = Single.PositiveInfinity;
                }
            }

            try
            {
                QueryTools.ExecuteSyncNonQuery(Session,
                                               string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName,
                                                             toInsert_and_Check[0][0], toInsert_and_Check[0][1], Minimum), null);
                QueryTools.ExecuteSyncNonQuery(Session,
                                               string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName,
                                                             toInsert_and_Check[1][0], toInsert_and_Check[1][1], Maximum), null);
            }
            catch (InvalidQueryException)
            {
                if (!sameOutput && toExceed == typeof (Int32)) //for ExceedingCassandra_INT() test case
                {
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
                    Assert.True(true);
                    return;
                }
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.One, toInsert_and_Check);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }


        public void testCounters()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");
            try
            {
                Session.WaitForSchemaAgreement(QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         incdec counter
         );", tableName)));
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();

            Parallel.For(0, 100,
                         i =>
                         {
                             QueryTools.ExecuteSyncNonQuery(Session,
                                                            string.Format(@"UPDATE {0} SET incdec = incdec {2}  WHERE tweet_id = {1};", tableName,
                                                                          tweet_id, (i%2 == 0 ? "-" : "+") + i));
                         });

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(),
                                        new List<object[]> {new object[2] {tweet_id, (Int64) 50}});
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void insertingSingleValue(Type tp)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(tp);
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.WaitForSchemaAgreement(
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         value {1}
         );", tableName, cassandraDataTypeName)));
            }
            catch (AlreadyExistsException)
            {
            }

            var toInsert = new List<object[]>(1);
            object val = Randomm.RandomVal(tp);
            if (tp == typeof (string))
                val = "'" + val.ToString().Replace("'", "''") + "'";
            var row1 = new object[2] {Guid.NewGuid(), val};
            toInsert.Add(row1);

            bool isFloatingPoint = false;

            if (row1[1].GetType() == typeof (string) || row1[1].GetType() == typeof (byte[]))
                QueryTools.ExecuteSyncNonQuery(Session,
                                               string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0],
                                                             row1[1].GetType() == typeof (byte[])
                                                                 ? "0x" + CqlQueryTools.ToHex((byte[]) toInsert[0][1])
                                                                 : "'" + toInsert[0][1] + "'"), null);
                    // rndm.GetType().GetMethod("Next" + tp.Name).Invoke(rndm, new object[] { })
            else
            {
                if (tp == typeof (Single) || tp == typeof (Double))
                    isFloatingPoint = true;
                QueryTools.ExecuteSyncNonQuery(Session,
                                               string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0],
                                                             !isFloatingPoint
                                                                 ? toInsert[0][1]
                                                                 : toInsert[0][1].GetType()
                                                                                 .GetMethod("ToString", new[] {typeof (string)})
                                                                                 .Invoke(toInsert[0][1], new object[] {"r"})), null);
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), toInsert);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }


        public void TimestampTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         ts timestamp
         );", tableName)));

            QueryTools.ExecuteSyncNonQuery(Session,
                                           string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(),
                                                         "2011-02-03 04:05+0000"), null);
            QueryTools.ExecuteSyncNonQuery(Session,
                                           string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(),
                                                         220898707200000), null);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(), 0),
                                           null);

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void createSecondaryIndexTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            string columns = "tweet_id uuid, name text, surname text";

            try
            {
                Session.WaitForSchemaAgreement(
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         {1},
PRIMARY KEY(tweet_id)
         );", tableName, columns))
                    );
            }
            catch (AlreadyExistsException)
            {
            }

            var row1 = new object[3] {Guid.NewGuid(), "Adam", "Małysz"};
            var row2 = new object[3] {Guid.NewGuid(), "Adam", "Miałczyński"};

            var toReturn = new List<object[]>(2) {row1, row2};
            var toInsert = new List<object[]>(2) {row1, row2};

            QueryTools.ExecuteSyncNonQuery(Session,
                                           string.Format("INSERT INTO {0}(tweet_id, name, surname) VALUES({1},'{2}','{3}');", tableName,
                                                         toInsert[0][0], toInsert[0][1], toInsert[0][2]), null, ConsistencyLevel.Quorum);
            QueryTools.ExecuteSyncNonQuery(Session,
                                           string.Format("INSERT INTO {0}(tweet_id, name, surname) VALUES({1},'{2}','{3}');", tableName,
                                                         toInsert[1][0], toInsert[1][1], toInsert[1][2]), null, ConsistencyLevel.Quorum);

            Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, string.Format("CREATE INDEX ON {0}(name);", tableName), null, ConsistencyLevel.Quorum)
                );

            Thread.Sleep(2000);
            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0} WHERE name = 'Adam';", tableName), ConsistencyLevel.Quorum, toReturn);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }


        public void BigInsertTest(int RowsNo = 5000)
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.WaitForSchemaAgreement(
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
		 fval float,
		 dval double,
         PRIMARY KEY(tweet_id))", tableName))
                    );
            }
            catch (AlreadyExistsException)
            {
            }

            var longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            for (int i = 0; i < RowsNo; i++)
            {
                longQ.AppendFormat(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body,
		 fval,
		 dval)
VALUES ({1},'test{2}',{3},'body{2}',{4},{5});", tableName, Guid.NewGuid(), i, i%2 == 0 ? "false" : "true", Randomm.Instance.NextSingle(),
                                   Randomm.Instance.NextDouble());
            }
            longQ.AppendLine("APPLY BATCH;");
            QueryTools.ExecuteSyncNonQuery(Session, longQ.ToString(), "Inserting...");
            QueryTools.ExecuteSyncQuery(Session, string.Format(@"SELECT * from {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"DROP TABLE {0};", tableName));
        }

        [TestMethod]
        [WorksForMe]
        public void QueryBinding()
        {
            QueryBindingTest();
        }

        [TestMethod]
        [WorksForMe]
        public void QueryFetching()
        {
            QueryFetchingTest();
        }

        [TestMethod]
        [WorksForMe]
        public void BigInsert()
        {
            BigInsertTest(3000);
        }

        [TestMethod]
        [WorksForMe]
        public void creatingSecondaryIndex()
        {
            createSecondaryIndexTest();
        }

        [TestMethod]
        [WorksForMe]
        public void testCounter()
        {
            testCounters();
        }

        [TestMethod]
        [WorksForMe]
        public void testBlob()
        {
            insertingSingleValue(typeof (byte));
        }

        [TestMethod]
        [WorksForMe]
        public void testASCII()
        {
            insertingSingleValue(typeof (Char));
        }

        [TestMethod]
        [WorksForMe]
        public void testDecimal()
        {
            insertingSingleValue(typeof (Decimal));
        }

        [TestMethod]
        [WorksForMe]
        public void testVarInt()
        {
            insertingSingleValue(typeof (BigInteger));
        }

        [TestMethod]
        [WorksForMe]
        public void testBigInt()
        {
            insertingSingleValue(typeof (Int64));
        }

        [TestMethod]
        [WorksForMe]
        public void testDouble()
        {
            insertingSingleValue(typeof (Double));
        }

        [TestMethod]
        [WorksForMe]
        public void testFloat()
        {
            insertingSingleValue(typeof (Single));
        }

        [TestMethod]
        [WorksForMe]
        public void testInt()
        {
            insertingSingleValue(typeof (Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void testBoolean()
        {
            insertingSingleValue(typeof (Boolean));
        }

        [TestMethod]
        [WorksForMe]
        public void testUUID()
        {
            insertingSingleValue(typeof (Guid));
        }

        [TestMethod]
        [WorksForMe]
        public void testTimestamp()
        {
            TimestampTest();
        }

        [TestMethod]
        [WorksForMe]
        public void MaxingBoundsOf_INT()
        {
            ExceedingCassandraType(typeof (Int32), typeof (Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void MaxingBoundsOf_BIGINT()
        {
            ExceedingCassandraType(typeof (Int64), typeof (Int64));
        }

        [TestMethod]
        [WorksForMe]
        public void MaxingBoundsOf_FLOAT()
        {
            ExceedingCassandraType(typeof (Single), typeof (Single));
        }

        [TestMethod]
        [WorksForMe]
        public void MaxingBoundsOf_DOUBLE()
        {
            ExceedingCassandraType(typeof (Double), typeof (Double));
        }

        [TestMethod]
        [WorksForMe]
        public void ExceedingCassandra_INT()
        {
            ExceedingCassandraType(typeof (Int32), typeof (Int64), false);
        }

        [TestMethod]
        [WorksForMe]
        public void ExceedingCassandra_FLOAT()
        {
            ExceedingCassandraType(typeof (Single), typeof (Double), false);
        }
    }
}