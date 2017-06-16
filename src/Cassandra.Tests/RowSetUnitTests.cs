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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;
using Moq;

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
            Assert.True(rs.AutoPage);
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

        [Test]
        public void RowSetDoesNotCallFetchNextWhenAutoPageFalseTest()
        {
            //Create a rowset with 1 row
            var rs = CreateStringsRowset(1, 1, "a_");
            //Set to not to automatically page
            rs.AutoPage = false;
            //It has paging state, stating that there are more pages
            rs.PagingState = new byte[] { 0 };
            //Add a handler to fetch next
            var called = false;
            rs.FetchNextPage = (pagingState) =>
            {
                called = true;
                return CreateStringsRowset(1, 1, "b_");
            };

            //use linq to iterate and map it to a list
            var rowList = rs.ToList();
            Assert.False(called);
            Assert.AreEqual(1, rowList.Count);
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
            //The row set should throw an exception when getting the next page.
            Assert.Throws<TestException>(() => { rs.ToList(); });

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

        [Test]
        public void NotExistentColumnThrows()
        {
            var row = CreateSampleRowSet().First();
            var ex = Assert.Throws<ArgumentException>(() => row.GetValue<string>("not_existent_col"));
            StringAssert.Contains("Column", ex.Message);
            StringAssert.Contains("not found", ex.Message);
        }

        [Test]
        public void NullValuesWithStructTypeColumnThrows()
        {
            //Row with all null values
            var row = CreateSampleRowSet().Last();
            Assert.IsNull(row.GetValue<string>("text_sample"));
            Assert.Throws<NullReferenceException>(() => row.GetValue<int>("int_sample"));
            Assert.DoesNotThrow(() => row.GetValue<int?>("int_sample"));
        }

        [Test]
        public void RowsetIsMockable()
        {
            var rowMock = new Mock<Row>();
            rowMock.Setup(r => r.GetValue<int>(It.Is<string>(n => n == "int_value"))).Returns(100);
            var rows = new Row[]
            {
                rowMock.Object
            };
            var mock = new Mock<RowSet>();
            mock
                .Setup(r => r.GetEnumerator()).Returns(() => ((IEnumerable<Row>)rows).GetEnumerator());

            var rs = mock.Object;
            var rowArray = rs.ToArray();
            Assert.AreEqual(rowArray.Length, 1);
            Assert.AreEqual(rowArray[0].GetValue<int>("int_value"), 100);
        }

        /// <summary>
        /// Creates a rowset.
        /// The columns are named: col_0, ..., col_n
        /// The rows values are: row_0_col_0, ..., row_m_col_n
        /// </summary>
        private static RowSet CreateStringsRowset(int columnLength, int rowLength, string valueModifier = null)
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
                rs.AddRow(new Row(columns.Select(c => valueModifier + "row_" + j + "_col_" + c.Index).Cast<object>().ToArray(), columns.ToArray(), columnIndexes));
            }
            return rs;
        }

        /// <summary>
        /// Creates a RowSet with few rows with int, text columns (null values in the last row)
        /// </summary>
        private static RowSet CreateSampleRowSet()
        {
            var columns = new List<CqlColumn>
            {
                new CqlColumn()
                {
                    Index = 0,
                    Name = "text_sample",
                    TypeCode = ColumnTypeCode.Text,
                    Type = typeof (string)
                },
                new CqlColumn()
                {
                    Index = 1,
                    Name = "int_sample",
                    TypeCode = ColumnTypeCode.Int,
                    Type = typeof(int)
                }
            };
            var columnIndexes = columns.ToDictionary(c => c.Name, c => c.Index);
            var rs = new RowSet();
            var rowValues = new object[]
            {
                "text value",
                100
            };
            rs.AddRow(new Row(rowValues, columns.ToArray(), columnIndexes));
            rs.AddRow(new Row(new object[] { null, null}, columns.ToArray(), columnIndexes));
            return rs;
        }

        [Test]
        public void RowSet_Empty_Returns_Iterable_Instace()
        {
            var rs = RowSet.Empty();
            Assert.AreEqual(0, rs.Columns.Length);
            Assert.True(rs.IsExhausted());
            Assert.True(rs.IsFullyFetched);
            Assert.AreEqual(0, rs.Count());
            //iterate a second time
            Assert.AreEqual(0, rs.Count());
            //Different instances
            Assert.AreNotSame(RowSet.Empty(), rs);
            Assert.DoesNotThrow(() => rs.FetchMoreResults());
            Assert.AreEqual(0, rs.GetAvailableWithoutFetching());
        }

        public void RowSet_Empty_Call_AddRow_Throws()
        {
            var rs = RowSet.Empty();
            Assert.Throws<InvalidOperationException>(() => rs.AddRow(new Row()));
        }

        [Test]
        public void Row_TryConvertToType_Should_Convert_Timestamps()
        {
            var timestampTypeInfo = new CqlColumn {TypeCode = ColumnTypeCode.Timestamp};
            var values = new[]
            {
                //column desc, value, type and expected type
                new object[] {DateTimeOffset.Now, timestampTypeInfo, typeof(DateTime)},
                new object[] {DateTimeOffset.Now, timestampTypeInfo, typeof(DateTimeOffset)},
                new object[] {DateTimeOffset.Now, timestampTypeInfo, typeof(object), typeof(DateTimeOffset)},
                new object[] {DateTimeOffset.Now, timestampTypeInfo, typeof(IConvertible), typeof(DateTime)}
            };
            foreach (var item in values)
            {
                var value = Row.TryConvertToType(item[0], (CqlColumn)item[1], (Type)item[2]);
                Assert.AreEqual(item.Length > 3 ? item[3] : item[2], value.GetType());
            }
        }

        [Test]
        public void Row_TryConvertToType_Should_Convert_Lists()
        {
            var listIntTypeInfo = new CqlColumn
            {
                TypeCode = ColumnTypeCode.List, 
                TypeInfo = new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Int },
                Type = typeof(IEnumerable<int>)
            };
            var values = new[]
            {
                new object[] {new [] {1, 2, 3}, listIntTypeInfo, typeof(int[])},
                new object[] {new [] {1, 2, 3}, listIntTypeInfo, typeof(object), typeof(int[])},
                new object[] {new [] {1, 2, 3}, listIntTypeInfo, typeof(IEnumerable<int>), typeof(int[])},
                new object[] {new [] {1, 2, 3}, listIntTypeInfo, typeof(List<int>)},
                new object[] {new [] {1, 2, 3}, listIntTypeInfo, typeof(IList<int>), typeof(List<int>)}
            };
            foreach (var item in values)
            {
                var value = Row.TryConvertToType(item[0], (CqlColumn)item[1], (Type)item[2]);
                Assert.AreEqual(item.Length > 3 ? item[3] : item[2], value.GetType());
                CollectionAssert.AreEqual((int[])item[0], (IEnumerable<int>)value);
            }
        }

        [Test]
        public void Row_TryConvertToType_Should_Convert_Sets()
        {
            var setIntTypeInfo = new CqlColumn
            {
                TypeCode = ColumnTypeCode.Set, 
                TypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Int },
                Type = typeof(IEnumerable<int>)
            };
            var values = new[]
            {
                new object[] {new [] {1, 2, 3}, setIntTypeInfo, typeof(int[])},
                new object[] {new [] {1, 2, 3}, setIntTypeInfo, typeof(object), typeof(int[])},
                new object[] {new [] {1, 2, 3}, setIntTypeInfo, typeof(IEnumerable<int>), typeof(int[])},
                new object[] {new [] {1, 2, 3}, setIntTypeInfo, typeof(HashSet<int>)},
                new object[] {new [] {1, 2, 3}, setIntTypeInfo, typeof(SortedSet<int>)},
                new object[] {new [] {1, 2, 3}, setIntTypeInfo, typeof(ISet<int>), typeof(SortedSet<int>)}
            };
            foreach (var item in values)
            {
                var value = Row.TryConvertToType(item[0], (CqlColumn)item[1], (Type)item[2]);
                Assert.AreEqual(item.Length > 3 ? item[3] : item[2], value.GetType());
                CollectionAssert.AreEqual((int[]) item[0], (IEnumerable<int>) value);
            }
        }

        [Test]
        public void Row_TryConvertToType_Should_Convert_Uuid_Collections()
        {
            var setTypeInfo = new CqlColumn
            {
                TypeCode = ColumnTypeCode.Set,
                TypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Timeuuid }
            };
            var listTypeInfo = new CqlColumn
            {
                TypeCode = ColumnTypeCode.List,
                TypeInfo = new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Timeuuid }
            };
            var values = new[]
            {
                Tuple.Create(new Guid[] { TimeUuid.NewId() }, setTypeInfo, typeof(TimeUuid[])),
                Tuple.Create(new Guid[] { TimeUuid.NewId() }, setTypeInfo, typeof(SortedSet<TimeUuid>)),
                Tuple.Create(new Guid[] { TimeUuid.NewId() }, listTypeInfo, typeof(List<TimeUuid>)),
                Tuple.Create(new Guid[] { TimeUuid.NewId() }, setTypeInfo, typeof(HashSet<TimeUuid>)),
                Tuple.Create(new Guid[] { TimeUuid.NewId() }, setTypeInfo, typeof(ISet<TimeUuid>)),
                Tuple.Create(new Guid[] { Guid.NewGuid() }, setTypeInfo, typeof(HashSet<Guid>)),
                Tuple.Create(new Guid[] { Guid.NewGuid() }, setTypeInfo, typeof(SortedSet<Guid>)),
                Tuple.Create(new Guid[] { Guid.NewGuid() }, listTypeInfo, typeof(List<Guid>)),
                Tuple.Create(new Guid[] { Guid.NewGuid() }, listTypeInfo, typeof(Guid[])),
                Tuple.Create(new Guid[] { Guid.NewGuid() }, listTypeInfo, typeof(IList<Guid>))
            };
            foreach (var item in values)
            {
                var value = Row.TryConvertToType(item.Item1, item.Item2, item.Item3);
                Assert.True(item.Item3.GetTypeInfo().IsInstanceOfType(value), "{0} is not assignable from {1}",
                    item.Item3, value.GetType());
                Assert.AreEqual(item.Item1.First().ToString(),
                    (from object v in (IEnumerable)value select v.ToString()).FirstOrDefault());
            }
        }

        [Test]
        public void Row_TryConvertToType_Should_Convert_Nested_Collections()
        {
            var setTypeInfo = new CqlColumn
            {
                TypeCode = ColumnTypeCode.Set,
                TypeInfo = new SetColumnInfo
                {
                    KeyTypeCode = ColumnTypeCode.Set,
                    KeyTypeInfo = new SetColumnInfo {  KeyTypeCode = ColumnTypeCode.Int }
                }
            };
            var listTypeInfo = new CqlColumn
            {
                TypeCode = ColumnTypeCode.List,
                TypeInfo = new ListColumnInfo
                {
                    ValueTypeCode = ColumnTypeCode.Set,
                    ValueTypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Timeuuid }
                }
            };
            var values = new[]
            {
                Tuple.Create((IEnumerable)new [] { new Guid[] { TimeUuid.NewId() } }, listTypeInfo, typeof(TimeUuid[][])),
                Tuple.Create((IEnumerable)new [] { new [] { Guid.NewGuid() } }, listTypeInfo, typeof(Guid[][])),
                Tuple.Create((IEnumerable)new [] { new Guid[] { TimeUuid.NewId() } }, listTypeInfo, typeof(SortedSet<TimeUuid>[])),
                Tuple.Create((IEnumerable)new [] { new [] { Guid.NewGuid() } }, listTypeInfo, typeof(HashSet<Guid>[])),
                Tuple.Create((IEnumerable)new [] { new [] { 314 } }, setTypeInfo, typeof(HashSet<int>[])),
                Tuple.Create((IEnumerable)new [] { new [] { 213 } }, setTypeInfo, typeof(int[][])),
                Tuple.Create((IEnumerable)new [] { new [] { 111 } }, setTypeInfo, typeof(SortedSet<SortedSet<int>>))
            };
            foreach (var item in values)
            {
                var value = Row.TryConvertToType(item.Item1, item.Item2, item.Item3);
                Assert.True(item.Item3.GetTypeInfo().IsInstanceOfType(value), "{0} is not assignable from {1}",
                    item.Item3, value.GetType());
                Assert.AreEqual(TestHelper.FirstString(item.Item1), TestHelper.FirstString((IEnumerable) value));
            }
        }

        [Test]
        public void Row_TryConvertToType_Should_Convert_Dictionaries()
        {
            var mapTypeInfo1 = new CqlColumn
            {
                TypeCode = ColumnTypeCode.Map,
                TypeInfo = new MapColumnInfo
                {
                    KeyTypeCode = ColumnTypeCode.Timeuuid,
                    ValueTypeCode = ColumnTypeCode.Set,
                    ValueTypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Int }
                }
            };
            var values = new[]
            {
                Tuple.Create(
                    (IDictionary) new SortedDictionary<Guid, IEnumerable<int>>
                    {
                        { Guid.NewGuid(), new[] { 1, 2, 3}}
                    },
                    mapTypeInfo1, typeof(SortedDictionary<Guid, IEnumerable<int>>)),
                Tuple.Create(
                    (IDictionary) new SortedDictionary<Guid, IEnumerable<int>>
                    {
                        { TimeUuid.NewId(), new[] { 1, 2, 3}}
                    },
                    mapTypeInfo1, typeof(IDictionary<TimeUuid, int[]>))
            };
            foreach (var item in values)
            {
                var value = Row.TryConvertToType(item.Item1, item.Item2, item.Item3);
                Assert.True(item.Item3.GetTypeInfo().IsInstanceOfType(value), "{0} is not assignable from {1}",
                    item.Item3, value.GetType());
                Assert.AreEqual(TestHelper.FirstString(item.Item1), TestHelper.FirstString((IEnumerable)value));
            }
        }

        // From DB!
        //System.Collections.Generic.SortedDictionary`2[System.String,System.Collections.Generic.IEnumerable`1[System.String]]

        [Test]
        public void Row_TryConvertToType_Should_Convert_Timestamp_Collections()
        {
            var setTypeInfo = new CqlColumn
            {
                TypeCode = ColumnTypeCode.Set,
                TypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Timestamp },
                Type = typeof(IEnumerable<DateTimeOffset>)
            };
            var listTypeInfo = new CqlColumn
            {
                TypeCode = ColumnTypeCode.List,
                TypeInfo = new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Timestamp }
            };
            var values = new[]
            {
                Tuple.Create(new [] { DateTimeOffset.UtcNow }, setTypeInfo, typeof(DateTime[])),
                Tuple.Create(new [] { DateTimeOffset.UtcNow }, setTypeInfo, typeof(SortedSet<DateTime>)),
                Tuple.Create(new [] { DateTimeOffset.UtcNow }, listTypeInfo, typeof(List<DateTime>)),
                Tuple.Create(new [] { DateTimeOffset.UtcNow }, setTypeInfo, typeof(HashSet<DateTime>)),
                Tuple.Create(new [] { DateTimeOffset.UtcNow }, setTypeInfo, typeof(HashSet<DateTimeOffset>)),
                Tuple.Create(new [] { DateTimeOffset.UtcNow }, setTypeInfo, typeof(SortedSet<DateTimeOffset>)),
                Tuple.Create(new [] { DateTimeOffset.UtcNow }, listTypeInfo, typeof(List<DateTimeOffset>)),
                Tuple.Create(new [] { DateTimeOffset.UtcNow }, listTypeInfo, typeof(DateTimeOffset[]))
            };
            foreach (var item in values)
            {
                var value = Row.TryConvertToType(item.Item1, item.Item2, item.Item3);
                Assert.True(item.Item3.GetTypeInfo().IsInstanceOfType(value), "{0} is not assignable from {1}",
                    item.Item3, value.GetType());
                Assert.AreEqual(item.Item1.First().Ticks,
                    (from object v in (IEnumerable)value select (v is DateTime ? ((DateTime)v).Ticks : ((DateTimeOffset)v).Ticks)).FirstOrDefault());
            }
        }

        private class TestException : Exception { }
    }
}
