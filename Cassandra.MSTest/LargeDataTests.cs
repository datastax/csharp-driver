//
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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using System.Threading;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cassandra.MSTest;
#endif

namespace Cassandra.MSTest
{
    [TestClass]
    public class LargeDataTests
    {
        string ksname = "large_data";
        int key = 0;


        Cluster cluster;
        Session session;
        CCMBridge.CCMCluster CCMCluster;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }

        private void SetupDefaultCluster()
        {
            if (CCMCluster != null)
                CCMCluster.Discard();

            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            session = CCMCluster.Session;
            cluster = CCMCluster.Cluster;
            session.CreateKeyspaceIfNotExists(ksname);
            session.ChangeKeyspace(ksname);
        }

        [TestCleanup]
        public void Dispose()
        {
            if(CCMCluster!=null)
                CCMCluster.Discard();
        }
        
        /*
         * Test a wide row of size 1,000,000
         * @param c The cluster object
         * @param key The key value that will receive the data
         * @throws Exception
         */
        private void testWideRows()
        {
            // Write data
            for (int i = 0; i < 500; ++i) //1000000
                session.Execute("INSERT INTO wide_rows(k,i) VALUES(" + key + "," + i + ")", ConsistencyLevel.Quorum);

            // Read data        
            using (var rs = session.Execute("SELECT i FROM wide_rows WHERE k = " + key.ToString(), ConsistencyLevel.Quorum))
            {
                // Verify data
                int j = 0;
                foreach (Row row in rs.GetRows())
                    Assert.True((int)row["i"] == j++);

            }
        }

        /*
         * Test a batch that writes a row of size 10,000
     * @param c The cluster object
     * @param key The key value that will receive the data
     * @throws Throwable
     */
        private void testWideBatchRows()
        {
            // Write data        
            StringBuilder sb = new StringBuilder("BEGIN BATCH ");
            for (int i = 0; i < 500; ++i) // 10000
                sb.AppendLine(string.Format("INSERT INTO wide_batch_rows(k,i) VALUES({0},{1})", key, i));
            sb.Append("APPLY BATCH");
            session.Execute(sb.ToString(), ConsistencyLevel.Quorum);

            // Read data
            using (var rs = session.Execute("SELECT i FROM wide_batch_rows WHERE k = " + key.ToString(), ConsistencyLevel.Quorum))
            {
                // Verify data
                int j = 0;
                foreach (Row row in rs.GetRows())
                    Assert.True((int)row["i"] == j++);
            }
        }

        /*
         * Test a wide row of size 1,000,000 consisting of a ByteBuffer
         */
        private void testByteRows()
        {
            // Build small ByteBuffer sample
            BEBinaryWriter bw = new BEBinaryWriter();
            for (int i = 0; i < 56; i++)
                bw.WriteByte(0);
            bw.WriteUInt16((ushort)0xCAFE);
            byte[] bb = new byte[58];
            Array.Copy(bw.GetBuffer(), bb, 58);

            // Write data
            for (int i = 0; i < 500; ++i)//1000000
                session.Execute(string.Format("INSERT INTO wide_byte_rows(k,i) values({0},0x{1})", key, Cassandra.CqlQueryTools.ToHex(bb)), ConsistencyLevel.Quorum);

            // Read data
            using (var rs = session.Execute("SELECT i FROM wide_byte_rows WHERE k = " + key.ToString(), ConsistencyLevel.Quorum))
            {
                // Verify data            
                foreach (Row row in rs.GetRows())
                    Assert.ArrEqual((byte[])row["i"], bb);
            }
        }

        /*
         * Test a row with a single extra large text value
         */
        private void testLargeText()
        {
            // Write data
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < 1000; ++i)
                b.Append(i.ToString());// Create ultra-long text

            session.Execute(string.Format("INSERT INTO large_text(k,i) VALUES({0},'{1}')", key, b.ToString()), ConsistencyLevel.Quorum);

            // Read data
            using (var rs = session.Execute("SELECT * FROM large_text WHERE k = " + key.ToString(), ConsistencyLevel.Quorum))
            {
                Row row = rs.GetRows().FirstOrDefault();// select().all().from("large_text").where(eq("k", key))).one();
                // Verify data
                Assert.True(b.ToString().Equals(row["i"]));
            }
        }
        
        /*
         * Converts an integer to an string of letters
         */
        private static String createColumnName(int i)
        {
            String[] letters = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" };

            StringBuilder columnName;
            int currentI;

            currentI = i;
            columnName = new StringBuilder();
            while (true)
            {
                columnName.Append(letters[currentI % 10]);
                currentI /= 10;
                if (currentI == 0)
                    break;
            }

            return columnName.ToString();
        }

        /*
         * Creates a table with 330 columns
         */
        private void testWideTable()
        {
            // Write data            
            StringBuilder insrt = new StringBuilder("INSERT INTO wide_table(k");
            StringBuilder valus = new StringBuilder(" VALUES(" + key.ToString());
            for (int i = 0; i < 330; ++i)
            {
                insrt.Append(",\"" + createColumnName(i)+"\"");
                valus.Append("," + i.ToString());
            }
            insrt.Append(") " + valus.ToString() + ")");
            session.Execute(insrt.ToString(), ConsistencyLevel.Quorum);

            // Read data
            using (var rs = session.Execute("SELECT * FROM wide_table WHERE k = " + key.ToString(), ConsistencyLevel.Quorum))
            {
                Row row = rs.GetRows().FirstOrDefault();

                Assert.True(row != null, "row is null");

                Assert.True(row.Length >= 330, "not enough columns");

                // Verify data
                for (int i = 0; i < 330; ++i)
                {
                    var cn = createColumnName(i);
                    Assert.True(row[cn] != null, "column is null");
                    Assert.True(row[cn] is int, "column is not int");
                    Assert.True((int)row[cn] == i);
                }
            }
        }



        private void largeDataTest(string tableName, string cqlType = "int")
        {
            try
            {
                session.WaitForSchemaAgreement(
                    session.Execute("DROP TABLE " + tableName));
            }
            catch (InvalidConfigurationInQueryException ex) { }

            if (tableName == "wide_table")                            
                session.WaitForSchemaAgreement(
                    session.Execute(GetTableDeclaration()));            
            else                           
                session.WaitForSchemaAgreement(
                    session.Execute(String.Format("CREATE TABLE {0} (k INT, i {1}, PRIMARY KEY(k,i))", tableName, cqlType)));
            
            try
            {
                switch (tableName)
                {
                    case "wide_rows":
                        testWideRows();
                        break;
                    case "wide_batch_rows":
                        testWideBatchRows();
                        break;
                    case "wide_byte_rows":
                        testByteRows();
                        break;
                    case "large_text":
                        testLargeText();
                        break;
                    case "wide_table":
                        testWideTable();
                        break;

                    default:
                        throw new InvalidOperationException();
                }

            }
            catch (Exception e)
            {
                throw e;
            }
        }


        /// <summary>
        ///  Test a wide row of size 1,000,000
        /// </summary>
        /// <throws name="Exception"></throws>

        [TestMethod]
		[WorksForMe]
        public void wideRows()
        {
            SetupDefaultCluster();
            session.ChangeKeyspace(ksname);
            largeDataTest("wide_rows");
        }

        /// <summary>
        ///  Test a batch that writes a row of size 10,000
        /// </summary>
        /// <throws name="Exception"></throws>

        [TestMethod]
		[WorksForMe]
        public void wideBatchRows()
        {
            SetupDefaultCluster();
            session.ChangeKeyspace(ksname);
            largeDataTest("wide_batch_rows");
        }

        /// <summary>
        ///  Test a wide row of size 1,000,000 consisting of a ByteBuffer
        /// </summary>
        /// <throws name="Exception"></throws>

        [TestMethod]
		[WorksForMe]
        public void wideByteRows()
        {
            SetupDefaultCluster();
            session.ChangeKeyspace(ksname);
            largeDataTest("wide_byte_rows", "blob");
        }

        /// <summary>
        ///  Test a row with a single extra large text value
        /// </summary>
        /// <throws name="Exception"></throws>

        [TestMethod]
		[WorksForMe]
        public void largeText()
        {
            SetupDefaultCluster();
            largeDataTest("large_text", "text");
        }

        /// <summary>
        ///  Creates a table with 330 columns
        /// </summary>
        /// <throws name="Exception"></throws>

        [TestMethod]
		[WorksForMe]
        public void wideTable()
        {
            SetupDefaultCluster();
            largeDataTest("wide_table");
        }

        private static String GetTableDeclaration()
        {
            StringBuilder tableDeclaration = new StringBuilder();
            tableDeclaration.Append("CREATE TABLE wide_table (");
            tableDeclaration.Append("k INT PRIMARY KEY");
            for (int i = 0; i < 330; ++i)
            {
                tableDeclaration.Append(String.Format(", \"{0}\" INT", createColumnName(i)));
            }
            tableDeclaration.Append(")");
            return tableDeclaration.ToString();
        }

        /// <summary>
        ///  Tests 10 random tests consisting of the other methods in this class
        /// </summary>
        /// <throws name="Exception"></throws>
        [TestMethod]    
        [WorksForMe]
        public void mixedDurationTestCCM()
        {
            if (CCMCluster != null)
                CCMCluster.Discard();
            CCMCluster = CCMBridge.CCMCluster.Create(3, Cluster.Builder());
            cluster = CCMCluster.Cluster;
            session = CCMCluster.Session;

            session.CreateKeyspace("large_data", ReplicationStrategies.CreateSimpleStrategyReplicationProperty(3));
            session.ChangeKeyspace("large_data");
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k INT, i INT, PRIMARY KEY(k, i))", "wide_rows")));
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k INT, i INT, PRIMARY KEY(k, i))", "wide_batch_rows")));
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k INT, i BLOB, PRIMARY KEY(k, i))", "wide_byte_rows")));
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i text)", "large_text")));

            // Create the extra wide table definition
            StringBuilder tableDeclaration = new StringBuilder();
            tableDeclaration.Append("CREATE TABLE wide_table (");
            tableDeclaration.Append("k INT PRIMARY KEY");
            for (int i = 0; i < 330; ++i)
            {
                tableDeclaration.Append(String.Format(", \"{0}\" INT", createColumnName(i)));
            }
            tableDeclaration.Append(")");
            session.WaitForSchemaAgreement(
                session.Execute(tableDeclaration.ToString())
            );

            Random rndm = new Random(DateTime.Now.Millisecond);
            try
            {
                for (int i = 0; i < 10; ++i)
                {
                    switch ((int)rndm.Next(0,5))
                    {
                        case 0: testWideRows(); break;
                        case 1: testWideBatchRows(); break;
                        case 2: testByteRows(); break;
                        case 3: testLargeText(); break;
                        case 4: testWideTable(); break;
                        default: break;
                    }
                }
            }
            catch (Exception e)
            {
                CCMCluster.ErrorOut();
                throw e;
            }
            finally
            {
                CCMCluster.Discard();
            }
        }

		private void createTable(string tableName, string cqlType)
		{
            try
            {
                session.WaitForSchemaAgreement(
                    session.Execute("DROP TABLE " + tableName));
            }
            catch (InvalidConfigurationInQueryException) { }

            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k INT, i {1}, PRIMARY KEY(k))", tableName, cqlType)));
		}

        /// <summary>
        ///  Test list with a single large text value
        /// </summary>
        /// <throws name="Exception"></throws>
        [TestMethod]
        [WorksForMe]
        public void largeListText()
        {
            SetupDefaultCluster();
			createTable("large_list_text", "list<text>");

            string b = new string('8', UInt16.MaxValue);
            session.Execute(string.Format("INSERT INTO large_list_text(k,i) VALUES({0},['{1}'])", key, b), ConsistencyLevel.Quorum);

            using (var rs = session.Execute("SELECT * FROM large_list_text WHERE k = " + key.ToString(), ConsistencyLevel.Quorum))
            {
                Row row = rs.GetRows().FirstOrDefault();
                Assert.True(b.Equals(((List<string>)row["i"])[0]));
            }
        }

        /// <summary>
        ///  Test set with a single large text value
        /// </summary>
        /// <throws name="Exception"></throws>
        [TestMethod]
        [WorksForMe]
        public void largeSetText()
        {
            SetupDefaultCluster();
            createTable("large_set_text", "set<text>");

            string b = new string('8', UInt16.MaxValue - 8); //according to specs it should accept  full UInt16.MaxValue, but for some reason it throws "The sum of all clustering columns is too long"
            session.Execute(string.Format("INSERT INTO large_set_text(k,i) VALUES({0},{{'{1}'}})", key, b), ConsistencyLevel.Quorum);

            using (var rs = session.Execute("SELECT * FROM large_set_text WHERE k = " + key.ToString(), ConsistencyLevel.Quorum))
            {
                Row row = rs.GetRows().FirstOrDefault();
                Assert.True(b.Equals(((List<string>)row["i"]).First()));
            }
        }

            /// <summary>
    ///  Test map with a large text key and large text value
        /// </summary>
        /// <throws name="Exception"></throws>
        [TestMethod]
        [WorksForMe]
        public void largeMapText()
        {
            SetupDefaultCluster();
            createTable("large_map_text", "map<text, text>");

            string b = new string('8', UInt16.MaxValue - 8); //according to specs it should accept  full UInt16.MaxValue, but for some reason it throws "The sum of all clustering columns is too long"
            session.Execute(string.Format("INSERT INTO large_map_text(k,i) VALUES({0},{{ '{1}' : '{1}' }})", key, b), ConsistencyLevel.Quorum);

            using (var rs = session.Execute("SELECT * FROM large_map_text WHERE k = " + key.ToString(), ConsistencyLevel.Quorum))
            {
                Row row = rs.GetRows().FirstOrDefault();
                Assert.True(b.Equals(((SortedDictionary<string, string>)row["i"]).First().Key));
                Assert.True(b.Equals(((SortedDictionary<string, string>)row["i"]).First().Value));
            }
        }
    }
}
