using NUnit.Framework;
using System;
using System.Collections.Concurrent;
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
            //Add a handler to fetch next
            rs.FetchNextPage = (pagingState) =>
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
            rs.FetchNextPage = (pagingState) =>
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
        /// Tests that once iterated, it can not be iterated any more.
        /// </summary>
        [Test]
        public void RowSetMustDequeue()
        {
            var rowLength = 10;
            var rs = CreateStringsRowset(2, rowLength);
            rs.FetchNextPage = (pagingState) =>
            {
                Assert.Fail("Event to get next page must not be called as there is no paging state.");
                return null;
            };
            //Use Linq to iterate
            var rowsFirstIteration = rs.ToList();
            Assert.AreEqual(rowLength, rowsFirstIteration.Count);

            //Following iterations must yield 0 rows
            var rowsSecondIteration = rs.ToList();
            var rowsThridIteration = rs.ToList();
            Assert.AreEqual(0, rowsSecondIteration.Count);
            Assert.AreEqual(0, rowsThridIteration.Count);

            Assert.IsTrue(rs.IsExhausted());
            Assert.IsTrue(rs.IsFullyFetched);
        }

        /// <summary>
        /// Tests that when multi threading, all enumerators of the same rowset wait for the fetching.
        /// </summary>
        [Test]
        public void RowSetFetchNextAllEnumeratorsWait()
        {
            var pageSize = 10;
            var rs = CreateStringsRowset(10, pageSize);
            rs.PagingState = new byte[0];
            var fetchCounter = 0;
            rs.FetchNextPage = (pagingState) =>
            {
                fetchCounter++;
                //fake a fetch
                Thread.Sleep(1000);
                return CreateStringsRowset(10, pageSize);
            };
            var counterList = new ConcurrentBag<int>();
            Action iteration = () =>
            {
                var counter = 0;
                foreach (var row in rs)
                {
                    counter++;
                    //Try to synchronize, all the threads will try to fetch at the almost same time.
                    Thread.Sleep(300);
                }
                counterList.Add(counter);
            };
            //Invoke it in parallel more than 10 times
            Parallel.Invoke(iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration, iteration);

            //Assert that the fetch was called just 1 time
            Assert.AreEqual(1, fetchCounter);

            //Sum all rows dequeued from the different threads 
            var totalRows = counterList.Sum();
            //Check that the total amount of rows dequeued are the same as pageSize * number of pages. 
            Assert.AreEqual(pageSize * 2, totalRows);
        }

        [Test]
        public void RowSetFetchNext3Pages()
        {
            var rowLength = 10;
            var rs = CreateStringsRowset(10, rowLength, "page_0_");
            rs.PagingState = new byte[0];
            var fetchCounter = 0;
            rs.FetchNextPage = (pagingState) =>
            {
                fetchCounter++;
                var pageRowSet = CreateStringsRowset(10, rowLength, "page_" + fetchCounter + "_");
                if (fetchCounter < 3)
                {
                    //when retrieving the pages, state that there are more results
                    pageRowSet.PagingState = new byte[0];
                }
                else
                {
                    //On the 3rd page, state that there aren't any more pages.
                    pageRowSet.PagingState = null;
                }
                return pageRowSet;
            };

            //Use Linq to iterate
            var rows = rs.ToList();

            Assert.AreEqual(3, fetchCounter, "Fetch must have been called 3 times");

            Assert.AreEqual(rows.Count, rowLength * 4, "RowSet must contain 4 pages in total");

            //Check the values are in the correct order
            Assert.AreEqual(rows[0].GetValue<string>(0), "page_0_row_0_col_0");
            Assert.AreEqual(rows[rowLength].GetValue<string>(0), "page_1_row_0_col_0");
            Assert.AreEqual(rows[rowLength * 2].GetValue<string>(0), "page_2_row_0_col_0");
            Assert.AreEqual(rows[rowLength * 3].GetValue<string>(0), "page_3_row_0_col_0");
        }

        [Test]
        public void RowSetFetchNext3PagesExplicitFetch()
        {
            var rowLength = 10;
            var rs = CreateStringsRowset(10, rowLength, "page_0_");
            rs.PagingState = new byte[0];
            var fetchCounter = 0;
            rs.FetchNextPage = (pagingState) =>
            {
                fetchCounter++;
                var pageRowSet = CreateStringsRowset(10, rowLength, "page_" + fetchCounter + "_");
                if (fetchCounter < 3)
                {
                    //when retrieving the pages, state that there are more results
                    pageRowSet.PagingState = new byte[0];
                }
                else if (fetchCounter == 3)
                {
                    //On the 3rd page, state that there aren't any more pages.
                    pageRowSet.PagingState = null;
                }
                else
                {
                    throw new Exception("It should not be called more than 3 times.");
                }
                return pageRowSet;
            };
            Assert.AreEqual(rowLength * 1, rs.InnerQueueCount);
            rs.FetchMoreResults();
            Assert.AreEqual(rowLength * 2, rs.InnerQueueCount);
            rs.FetchMoreResults();
            Assert.AreEqual(rowLength * 3, rs.InnerQueueCount);
            rs.FetchMoreResults();
            Assert.AreEqual(rowLength * 4, rs.InnerQueueCount);

            //Use Linq to iterate: 
            var rows = rs.ToList();

            Assert.AreEqual(rows.Count, rowLength * 4, "RowSet must contain 4 pages in total");

            //Check the values are in the correct order
            Assert.AreEqual(rows[0].GetValue<string>(0), "page_0_row_0_col_0");
            Assert.AreEqual(rows[rowLength].GetValue<string>(0), "page_1_row_0_col_0");
            Assert.AreEqual(rows[rowLength * 2].GetValue<string>(0), "page_2_row_0_col_0");
            Assert.AreEqual(rows[rowLength * 3].GetValue<string>(0), "page_3_row_0_col_0");
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
