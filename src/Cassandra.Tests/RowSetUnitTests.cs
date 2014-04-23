using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.Tests
{
    [TestFixture]
    public class RowSetUnitTests
    {
        [Test]
        public void RowIteratesThroughValues()
        {
            var rs = CreateStringsRowset(4, 1);
            var row = rs.First();
            //Use Linq's IEnumerable ToList: it iterates and maps to a list
            var cellValues = row.ToList();
            Assert.AreEqual("row_0_col_0", cellValues[0]);
            Assert.AreEqual("row_0_col_1", cellValues[1]);
            Assert.AreEqual("row_0_col_2", cellValues[2]);
            Assert.AreEqual("row_0_col_3", cellValues[3]);
        }

        /// <summary>
        /// Test that all possible ways to get the value from the row gets the same value
        /// </summary>
        [Test]
        public void RowGetTheSameValues()
        {
            var row = CreateStringsRowset(3, 1).First();

            var value00 = row[0];
            var value01 = row.GetValue<object>(0);
            var value02 = row.GetValue(typeof(object), 0);
            Assert.True(value00.Equals(value01) && value01.Equals(value02), "Row values do not match");

            var value10 = (string)row[1];
            var value11 = row.GetValue<string>(1);
            var value12 = (string)row.GetValue(typeof(string), 1);
            Assert.True(value10.Equals(value11) && value11.Equals(value12), "Row values do not match");

            var value20 = (string)row["col_2"];
            var value21 = row.GetValue<string>("col_2");
            var value22 = (string)row.GetValue(typeof(string), "col_2");
            Assert.True(value20.Equals(value21) && value21.Equals(value22), "Row values do not match");
        }

        [Test]
        public void RowSetIteratesTest()
        {
            var rs = CreateStringsRowset(2, 3);

            //Use Linq's IEnumerable ToList to iterate and map it to a list
            var rowList = rs.ToList();
            Assert.AreEqual(3, rowList.Count);
            Assert.AreEqual("row_0_col_0", rowList[0].GetValue<string>("col_0"));
            Assert.AreEqual("row_1_col_1", rowList[1].GetValue<string>("col_1"));
            Assert.AreEqual("row_2_col_0", rowList[2].GetValue<string>("col_0"));
        }

        [Test]
        public void RowSetCallsFetchNextTest()
        {
            //Create a rowset with 1 row
            var rs = CreateStringsRowset(1, 1, "a_");
            //It has paging state, stating that there are more pages
            rs.PagingState = new byte[] { 0 };
            //There is a subscriber to the event to fetch next
            rs.FetchNextPage += (pagingState) =>
            {
                return CreateStringsRowset(1, 1, "b_");
            };

            //use linq to iterate and map it to a list
            var rowList = rs.ToList();
            Assert.AreEqual(2, rowList.Count);
            Assert.AreEqual("a_row_0_col_0", rowList[0].GetValue<string>("col_0"));
            Assert.AreEqual("b_row_0_col_0", rowList[1].GetValue<string>("col_0"));
        }

        /// <summary>
        /// Ensures that in case there is an exception while retrieving the next page, it propagates.
        /// </summary>
        [Test]
        public void RowSetFetchNextPropagatesExceptionTest()
        {
            var rs = CreateStringsRowset(1, 1);
            //It has paging state, stating that there are more pages.
            rs.PagingState = new byte[] { 0 };
            //Throw a test exception when fetching the next page.
            rs.FetchNextPage += (pagingState) =>
            {
                throw new TestException();
            };

            //use linq to iterate and map it to a list
            try 
            {
                //The row set should throw an exception when getting the next page.
                var rowList = rs.ToList();
                Assert.Fail("It should throw a TestException");
            }
            catch (TestException)
            {
                //An exception of type TestException is expected. 
            }
            catch (Exception ex)
            {
                Assert.Fail("Expected exception of type TestException, got: " + ex.GetType());
            }
        }

        /// <summary>
        /// Tests that when multi threading, all enumerators of the same rowset wait for the fetching.
        /// </summary>
        [Test]
        public void RowSetFetchNextAllEnumeratorsWait()
        {
            var rowLength = 10;
            var rs = CreateStringsRowset(10, rowLength);
            rs.PagingState = new byte[0];
            var fetchCounter = 0;
            rs.FetchNextPage += (pagingState) =>
            {
                fetchCounter++;
                //fake a 
                Thread.Sleep(1500);
                return CreateStringsRowset(10, rowLength);
            };
            var counterList = new List<int>();
            Action iteration = () =>
            {
                var counter = 0;
                foreach (var row in rs)
                {
                    counter++;
                    if (counter == rowLength ||counter == rowLength-1)
                    {
                        //Try to synchronize, all the threads will try to fetch at the almost same time.
                        Thread.Sleep(200);
                    }
                }
                counterList.Add(counter);
            };
            Parallel.Invoke(iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration);
            
            //Assert that the fetch was called just 1 time
            Assert.AreEqual(1, fetchCounter);
            foreach (var counter in counterList)
            {
                Assert.AreEqual(rowLength * 2, counter);
            }
        }

        /// <summary>
        /// Tests that multiple threads do not affect the current enumerator
        /// </summary>
        [Test]
        public void RowSetEnumeratorAreDifferentInstances()
        {
            var rowLength = 10;
            var rs = CreateStringsRowset(10, rowLength);
            Action iteration = () =>
            {
                var counter = 0;
                foreach (var row in rs)
                {
                    Thread.Sleep(25);
                    counter++;
                }
                Assert.AreEqual(rowLength, counter);
            };
            //Invoke the actions in parallel
            Parallel.Invoke(iteration, iteration, iteration, iteration);
        }

        /// <summary>
        /// Creates a rowset.
        /// The columns are named: col_0, ..., col_n
        /// The rows values are: row_0_col_0, ..., row_m_col_n
        /// </summary>
        public RowSet CreateStringsRowset(int columnLength, int rowLength, string valueModifier = null)
        {
            var columns = new List<CqlColumn>();
            var columnIndexes = new Dictionary<string, int>();
            for (var i = 0; i < columnLength; i++)
            {
                var c = new CqlColumn()
                {
                    Index = i,
                    Name = "col_" + i,
                    TypeCode = ColumnTypeCode.Text,
                    Type = typeof(string)
                };
                columns.Add(c);
                columnIndexes.Add(c.Name, c.Index);
            }
            var rs = new RowSet();
            for (var j = 0; j < rowLength; j++)
            {
                var rowValues = new List<byte[]>();
                foreach (var c in columns)
                {
                    var value = valueModifier + "row_" + j + "_col_" + c.Index;
                    rowValues.Add(Encoding.UTF8.GetBytes(value));
                }
                rs.AddRow(new Row(rowValues.ToArray(), columns.ToArray(), columnIndexes));
            }
            return rs;
        }

        public class TestException : Exception { }
    }
}
