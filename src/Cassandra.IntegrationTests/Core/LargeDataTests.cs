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

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class LargeDataTests : TestBase
    {
        private const int KEY_CONST = 0;
        private const string KEYSPACE_NAME_DEFAULT = "LargeDataTests";
        ISession session = null; 

        [SetUp]
        public void SetupFixture()
        {
            session = testClusterManager.getTestCluster(2).session;
        }

        /// <summary>
        ///  Test a wide row 
        /// </summary>
        [Test]
        public void LargeDataTests_WideRows()
        {
            string uniqueTableName = "wide_rows_" + Randomm.RandomAlphaNum(16);
            testWideRows(session, uniqueTableName);
        }

        /// <summary>
        ///  Test a batch that writes a row of size 10,000
        /// </summary>
        [Test]
        public void LargeDataTests_WideBatchRows()
        {
            string uniqueTableName = "wide_batch_rows" + Randomm.RandomAlphaNum(16);
            testWideBatchRows(session, uniqueTableName);
        }

        /// <summary>
        ///  Test a wide row consisting of a ByteBuffer
        /// </summary>
        [Test]
        public void LargeDataTests_WideByteRows()
        {
            string uniqueTableName = "wide_byte_rows" + Randomm.RandomAlphaNum(16); ;
            testByteRows(session, uniqueTableName);
        }

        /// <summary>
        ///  Test a row with a single extra large text value
        /// </summary>
        [Test]
        public void LargeDataTests_LargeText()
        {
            string uniqueTableName = "large_text_" + Randomm.RandomAlphaNum(16); ;
            testLargeText(session, uniqueTableName);
        }

        /// <summary>
        ///  Creates a table with 330 columns
        /// </summary>
        [Test]
        public void LargeDataTests_WideTable()
        {
            string uniqueTableName = "wide_table" + Randomm.RandomAlphaNum(16); ;
            testWideTable(session, uniqueTableName);
        }

        /// <summary>
        ///  Test list with a single large text value
        /// </summary>
        [Test]
        public void LargeDataTests_LargeListText()
        {
            string uniqueTableName = getUniqueTableName();
            createTable(session, uniqueTableName, "list<text>");

            string b = new string('8', UInt16.MaxValue);
            session.Execute(string.Format("INSERT INTO {0}(k,i) VALUES({1},['{2}'])", uniqueTableName, KEY_CONST, b), ConsistencyLevel.Quorum);

            using (var rs = session.Execute(string.Format("SELECT * FROM {0} WHERE k = {1}", uniqueTableName, KEY_CONST.ToString()), ConsistencyLevel.Quorum))
            {
                Row row = rs.GetRows().FirstOrDefault();
                Assert.True(b.Equals(((List<string>)row["i"])[0]));
            }
        }

        /// <summary>
        ///  Test set with max allowed value size
        /// </summary>
        [Test]
        public void LargeDataTests_Set_Val_Max()
        {
            string uniqueTableName = getUniqueTableName();
            createTable(session, uniqueTableName, "set<text>");

            // according to specs it should accept  full UInt16.MaxValue, but for some reason it throws "The sum of all clustering columns is too long"
            string setVal = new string('a', UInt16.MaxValue - 9);
            session.Execute(string.Format("INSERT INTO {0}(k,i) VALUES({1},{{'{2}'}})", uniqueTableName, KEY_CONST, setVal, ConsistencyLevel.Quorum));

            using (var rs = session.Execute(string.Format("SELECT * FROM {0} WHERE k = {1}", uniqueTableName, KEY_CONST.ToString()), ConsistencyLevel.Quorum))
            {
                Row row = rs.GetRows().FirstOrDefault();
                Assert.AreEqual(setVal, ((List<string>)row["i"]).First());
            }
        }

        /// <summary>
        ///  Test set with max allowed value size plus one
        /// </summary>
        [Test]
        public void LargeDataTests_Set_Val_MaxPlusOne()
        {
            string uniqueTableName = getUniqueTableName();
            createTable(session, uniqueTableName, "set<text>");

            // given MAX = 65535, map string key max = MAX - 9 and map string value max = MAX 
            string setVal = new string('a', UInt16.MaxValue - 8);
            try
            {
                session.Execute(string.Format("INSERT INTO {0}(k,i) VALUES({1},{{'{2}'}})", uniqueTableName, KEY_CONST, setVal, ConsistencyLevel.Quorum));
                Assert.Fail("Expected exception was not thrown!");
            }
            catch (Cassandra.InvalidQueryException e)
            {
                string expectedErrMsg = "The sum of all clustering columns is too long";
                Assert.True(e.Message.Contains(expectedErrMsg), "Exception message {0} did not contain expected error message {1}.", e.Message, expectedErrMsg);
            }
        }

        /// <summary>
        ///  Test map with max allowed key and value size
        /// </summary>
        [Test]
        public void LargeDataTests_Map_Key_Max_Val_Max()
        {
            string uniqueTableName = getUniqueTableName();
            createTable(session, uniqueTableName, "map<text, text>");

            // given MAX = 65535, map string key max = MAX - 9 and map string value max = MAX 
            string mapKey = new string('a', UInt16.MaxValue - 9);
            string mapVal = new string('b', UInt16.MaxValue);
            session.Execute(string.Format("INSERT INTO {0}(k,i) VALUES({1},{{ '{2}' : '{3}' }})", uniqueTableName, KEY_CONST, mapKey, mapVal), ConsistencyLevel.Quorum);

            using (var rs = session.Execute(string.Format("SELECT * FROM {0} WHERE k = {1}", uniqueTableName, KEY_CONST.ToString()), ConsistencyLevel.Quorum))
            {
                Row row = rs.GetRows().FirstOrDefault();
                Assert.AreEqual(mapKey, ((SortedDictionary<string, string>)row["i"]).First().Key);
                Assert.AreEqual(mapVal, ((SortedDictionary<string, string>)row["i"]).First().Value);
            }
        }

        /// <summary>
        ///  Test map with max allowed key size + 1
        /// </summary>
        [Test]
        public void LargeDataTests_MapText_Key_MaxPlusOne()
        {
            string uniqueTableName = getUniqueTableName();
            createTable(session, uniqueTableName, "map<text, text>");

            // given MAX = 65535, map string key max = MAX - 9 and map string value max = MAX 
            string mapKey = new string('a', UInt16.MaxValue - 8);
            string mapVal = new string('b', 1); // something safe
            try
            {
                session.Execute(string.Format("INSERT INTO {0}(k,i) VALUES({1},{{ '{2}' : '{3}' }})", uniqueTableName, KEY_CONST, mapKey, mapVal), ConsistencyLevel.Quorum);
                Assert.Fail("Expected exception was not thrown!");
            }
            catch (Cassandra.InvalidQueryException e)
            {
                string expectedErrMsg = "The sum of all clustering columns is too long";
                Assert.True(e.Message.Contains(expectedErrMsg),
                    string.Format("Exception message: '{0}' did not contain error message '{1}'", e.Message, expectedErrMsg));
            }
        }

        /// <summary>
        ///  Test map with max allowed value size + 1
        /// </summary>
        [Test]
        public void LargeDataTests_Map_Value_MaxPlusOne()
        {
            string uniqueTableName = getUniqueTableName();
            createTable(session, uniqueTableName, "map<text, text>");

            // given MAX = 65535, map string key max = MAX - 9 and map string value max = MAX 
            string mapKey = new string('a', UInt16.MaxValue - 9);
            string mapVal = new string('b', UInt16.MaxValue + 1);
            try
            {
                session.Execute(string.Format("INSERT INTO {0}(k,i) VALUES({1},{{ '{2}' : '{3}' }})", uniqueTableName, KEY_CONST, mapKey, mapVal), ConsistencyLevel.Quorum);
                Assert.Fail("Expected exception was not thrown!");
            }
            catch (Cassandra.InvalidQueryException e)
            {
                string expectedErrMsg = "Map value is too long.";
                Assert.True(e.Message.Contains(expectedErrMsg),
                    string.Format("Exception message: '{0}' did not contain error message '{1}'", e.Message, expectedErrMsg));
            }
        }

        ///////////////////////////////////////
        // Test Helpers
        ///////////////////////////////////////

        private static void createTable(ISession session, string tableName, string cqlType)
        {
            session.CreateKeyspaceIfNotExists(KEYSPACE_NAME_DEFAULT);
            session.ChangeKeyspace(KEYSPACE_NAME_DEFAULT);
            session.WaitForSchemaAgreement(
                session.Execute(string.Format("CREATE TABLE {0} (k INT, i {1}, PRIMARY KEY(k))", tableName, cqlType)));
        }

        // Test a wide row
        private static void testWideRows(ISession session, string tableName)
        {
            string cql = string.Format("CREATE TABLE {0} (i INT, str {1}, PRIMARY KEY(i,str))", tableName, "text");
            session.WaitForSchemaAgreement(session.Execute(cql));

            // Write data
            //Use a row length of 1024, we are testing the driver not Cassandra itself
            List<string> expectedStrings = new List<string>();
            for (int str = 0; str < 1024; ++str)
            {
                string insertCql = string.Format("INSERT INTO {0} (i,str) VALUES({1},'{2}')", tableName, KEY_CONST, str);
                expectedStrings.Add(str.ToString());
                session.Execute(insertCql, ConsistencyLevel.Quorum);
            }

            // Read data       
            expectedStrings.Sort();
            var rs = session.Execute(string.Format("SELECT str FROM {0} WHERE i = {1}", tableName, KEY_CONST), ConsistencyLevel.Quorum);
            {
                // Verify data
                List<Row> rows = rs.GetRows().ToList();
                for (int j = 0; j < rows.Count; j++)
                    Assert.AreEqual(expectedStrings[j].ToString(), rows[j]["str"]);
            }
        }

        // Test a batch that writes a row of size
        private static void testWideBatchRows(ISession session, string tableName)
        {
            string cql = String.Format("CREATE TABLE {0} (i INT, str {1}, PRIMARY KEY(i,str))", tableName, "text");
            session.WaitForSchemaAgreement(session.Execute(cql));

            // Write data        
            List<string> expectedStrings = new List<string>();
            var sb = new StringBuilder("BEGIN BATCH ");
            for (int str = 0; str < 1024; ++str)
            {
                string insertCql = string.Format("INSERT INTO {0} (i,str) VALUES({1},'{2}')", tableName, KEY_CONST, str);
                expectedStrings.Add(str.ToString());
                sb.AppendLine(insertCql);
            }
            sb.Append("APPLY BATCH");
            session.Execute(sb.ToString(), ConsistencyLevel.Quorum);

            // Read data
            expectedStrings.Sort();
            var rs = session.Execute(string.Format("SELECT str FROM {0} WHERE i = {1}", tableName, KEY_CONST), ConsistencyLevel.Quorum);
            {
                // Verify data
                List<Row> rows = rs.GetRows().ToList();
                for (int j = 0; j < rows.Count; j++)
                    Assert.AreEqual(expectedStrings[j].ToString(), rows[j]["str"]);
            }
        }

        // Test a wide row consisting of a ByteBuffer
        private static void testByteRows(ISession session, string tableName)
        {
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k INT, i {1}, PRIMARY KEY(k,i))", tableName, "BLOB")));

            // Build small ByteBuffer sample
            var bw = new BEBinaryWriter();
            for (int i = 0; i < 56; i++)
                bw.WriteByte(0);
            bw.WriteUInt16(0xCAFE);
            var bb = new byte[58];
            Array.Copy(bw.GetBuffer(), bb, 58);

            // Write data
            for (int i = 0; i < 1024; ++i)
                session.Execute(string.Format("INSERT INTO {0}(k,i) values({1},0x{2})", tableName, KEY_CONST, CqlQueryTools.ToHex(bb)),
                                ConsistencyLevel.Quorum);

            // Read data
            var rs = session.Execute("SELECT i FROM " + tableName + " WHERE k = " + KEY_CONST, ConsistencyLevel.Quorum);
            // Verify data            
            foreach (var row in rs)
                Assert.AreEqual((byte[])row["i"], bb);
        }

        // Test a row with a single extra large text value
        private static void testLargeText(ISession session, string tableName)
        {
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k INT, i {1}, PRIMARY KEY(k,i))", tableName, "text")));

            // Write data
            var b = new StringBuilder();
            for (int i = 0; i < 1000; ++i)
                b.Append(i); // Create ultra-long text

            session.Execute(string.Format("INSERT INTO {0}(k,i) VALUES({1},'{2}')", tableName, KEY_CONST, b), ConsistencyLevel.Quorum);

            // Read data
            var rs = session.Execute("SELECT * FROM " + tableName + " WHERE k = " + KEY_CONST, ConsistencyLevel.Quorum);
            {
                Row row = rs.GetRows().FirstOrDefault(); // select().all().from("large_text").where(eq("k", key))).one();
                // Verify data
                Assert.True(b.ToString().Equals(row["i"]));
            }
        }

        // Converts an integer to an string of letters
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

        // Creates a table with 330 columns
        private static void testWideTable(ISession session, String tableName)
        {
            session.WaitForSchemaAgreement(session.Execute(GetTableDeclaration(tableName)));

            // Write data            
            var insrt = new StringBuilder("INSERT INTO " + tableName + "(k");
            var valus = new StringBuilder(" VALUES(" + KEY_CONST);
            for (int i = 0; i < 330; ++i)
            {
                insrt.Append(",\"" + createColumnName(i) + "\"");
                valus.Append("," + i);
            }
            insrt.Append(") " + valus + ")");
            session.Execute(insrt.ToString(), ConsistencyLevel.Quorum);

            // Read data
            var rs = session.Execute("SELECT * FROM " + tableName + " WHERE k = " + KEY_CONST, ConsistencyLevel.Quorum);
            {
                Row row = rs.GetRows().FirstOrDefault();
                Assert.True(row != null, "row is null");
                Assert.True(row.Length >= 330, "not enough columns");

                // Verify data
                for (int i = 0; i < 330; ++i)
                {
                    string cn = createColumnName(i);
                    Assert.True(row[cn] != null, "column is null");
                    Assert.True(row[cn] is int, "column is not int");
                    Assert.True((int)row[cn] == i);
                }
            }
        }

        private static string getUniqueTableName()
        {
            return "LgDataTsts_" + Randomm.RandomAlphaNum(16);
        }

        private static String GetTableDeclaration(string tableName)
        {
            var tableDeclaration = new StringBuilder();
            tableDeclaration.Append("CREATE TABLE " + tableName + " (");
            tableDeclaration.Append("k INT PRIMARY KEY");
            for (int i = 0; i < 330; ++i)
            {
                tableDeclaration.Append(String.Format(", \"{0}\" INT", createColumnName(i)));
            }
            tableDeclaration.Append(")");
            return tableDeclaration.ToString();
        }

    }
}
