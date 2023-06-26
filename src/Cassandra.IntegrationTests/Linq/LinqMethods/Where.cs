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

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;

using NUnit.Framework;

#pragma warning disable 618
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    public class Where : SimulacronTest
    {
        private readonly List<Movie> _movieList = Movie.GetDefaultMovieList();
        private readonly List<ManyDataTypesEntity> _manyDataTypesEntitiesList = ManyDataTypesEntity.GetDefaultAllDataTypesList();
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Table<ManyDataTypesEntity> _manyDataTypesEntitiesTable;
        private readonly List<Tuple<int, long>> _tupleList = new List<Tuple<int, long>> { Tuple.Create(0, 0L), Tuple.Create(1, 1L) };
        private static readonly List<Tuple<int, long>> TupleList = new List<Tuple<int, long>> { Tuple.Create(0, 0L), Tuple.Create(1, 1L) };
        private const short ExpectedShortValue = 11;

        public override void SetUp()
        {
            base.SetUp();
            Session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var movieMappingConfig = new MappingConfiguration();
            _movieTable = new Table<Movie>(Session, movieMappingConfig);
            _manyDataTypesEntitiesTable = new Table<ManyDataTypesEntity>(Session, movieMappingConfig);
        }

        [TestCase(true), TestCase(false)]
        [Test]
        public void LinqWhere_Execute(bool async)
        {
            var expectedMovie = _movieList.First();
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? ALLOW FILTERING",
                          rows => rows.WithParams(expectedMovie.Title, expectedMovie.MovieMaker))
                      .ThenRowsSuccess(expectedMovie.CreateRowsResult()));

            // test
            var query = _movieTable.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker);
            var movies = async ? query.ExecuteAsync().GetAwaiter().GetResult().ToList() : query.Execute().ToList();
            Assert.AreEqual(1, movies.Count);

            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }
        
        [Test]
        public void LinqWhere_NoSuchRecord()
        {
            var existingMovie = _movieList.Last();
            var randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? ALLOW FILTERING",
                          rows => rows.WithParams(existingMovie.Title, randomStr))
                      .ThenRowsSuccess(Movie.GetColumns()));

            var movies = _movieTable.Where(m => m.Title == existingMovie.Title && m.MovieMaker == randomStr).Execute().ToList();
            Assert.AreEqual(0, movies.Count);
        }

        [Test]
        public void LinqTable_TooManyEqualsClauses()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", " +
                            "\"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" " +
                          "WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? " +
                            "AND \"unique_movie_title\" = ? AND \"movie_maker\" = ? " +
                          "ALLOW FILTERING",
                          rows => rows.WithParams("doesntmatter", "doesntmatter", "doesntmatter", "doesntmatter"))
                      .ThenServerError(ServerError.Invalid, "msg"));

            var movieQuery = _movieTable
                .Where(m => m.Title == "doesntmatter" && m.MovieMaker == "doesntmatter")
                .Where(m => m.Title == "doesntmatter" && m.MovieMaker == "doesntmatter");
            Assert.Throws<InvalidQueryException>(() => movieQuery.Execute());
        }

        [Test]
        [TestCassandraVersion(3, 0, Comparison.LessThan)]
        public void LinqWhere_Exception()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", " +
                          "\"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" " +
                          "WHERE \"yearMade\" = ? " +
                          "ALLOW FILTERING",
                          rows => rows.WithParams(100))
                      .ThenServerError(ServerError.Invalid, "msg"));
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", " +
                          "\"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" " +
                          "WHERE \"mainGuy\" = ? " +
                          "ALLOW FILTERING",
                          rows => rows.WithParams("13"))
                      .ThenServerError(ServerError.Invalid, "msg"));
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", " +
                          "\"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" " +
                          "WHERE \"movie_maker\" = ? " +
                          "ALLOW FILTERING",
                          rows => rows.WithParams("dum").WithConsistency(ConsistencyLevel.Serial))
                      .ThenServerError(ServerError.Invalid, "msg"));

            //No translation in CQL
            Assert.Throws<CqlLinqNotSupportedException>(() => _movieTable.Where(m => m.Year is int).Execute());
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.MainActor == "13").Execute());
            //No execute
            Assert.Throws<InvalidOperationException>(() => _movieTable.Where(m => m.MovieMaker == "dum").GetEnumerator());

            //Wrong consistency level
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.MovieMaker == "dum").SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        [Test]
        [TestCassandraVersion(3, 0, Comparison.LessThan)]
        public void LinqWhere_NoPartitionKey()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", " +
                          "\"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" " +
                          "WHERE \"yearMade\" = ? " +
                          "ALLOW FILTERING",
                          rows => rows.WithParams(100))
                      .ThenServerError(ServerError.Invalid, "msg"));
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", " +
                          "\"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" " +
                          "WHERE \"mainGuy\" = ? " +
                          "ALLOW FILTERING",
                          rows => rows.WithParams("31"))
                      .ThenServerError(ServerError.Invalid, "msg"));
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.MainActor == "31").Execute());
        }

        [Test]
        public void LinqWhere_NoTranslationFromLinqToCql()
        {
            Assert.Throws<CqlLinqNotSupportedException>(() => _movieTable.Where(m => m.Year is int).Execute());
        }

        [Test]
        public void LinqWhere_ExecuteStepOmitted()
        {
            Assert.Throws<InvalidOperationException>(() => _movieTable.Where(m => m.MovieMaker == "dum").GetEnumerator());
        }

        [Test]
        public void LinqWhere_With_LocalSerial_ConsistencyLevel_Does_Not_Throw()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"movie_maker\" = ? AND \"unique_movie_title\" = ? ALLOW FILTERING",
                          rows => rows.WithParams("dum", "doesnt_matter").WithConsistency(ConsistencyLevel.LocalSerial))
                      .ThenRowsSuccess(Movie.GetColumns()));

            Assert.DoesNotThrow(() =>
                _movieTable.Where(m => m.MovieMaker == "dum" && m.Title == "doesnt_matter")
                    .SetConsistencyLevel(ConsistencyLevel.LocalSerial).Execute());
        }

        /// <summary>
        /// Successfully append to existing query with multiple "where" clauses
        ///
        /// @jira CSHARP-43 https://datastax-oss.atlassian.net/browse/CSHARP-43
        ///
        /// </summary>
        [Test]
        public void LinqWhere_AppendMultipleTimes()
        {
            int userId = 1;
            int date = 2;
            long time = 3;

            var config = new MappingConfiguration();
            var table = new Table<TestTable>(Session, config);

            var data = new List<TestTable>
            {
                new TestTable { UserId = 1, Date = 2, TimeColumn = 1 },
                new TestTable { UserId = 1, Date = 2, TimeColumn = 2 },
                new TestTable { UserId = 1, Date = 2, TimeColumn = 3 },
                new TestTable { UserId = 1, Date = 2, TimeColumn = 4 },
                new TestTable { UserId = 1, Date = 2, TimeColumn = 5 }
            };
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT \"date\", \"time\", \"user\" FROM \"{TestTable.TableName}\" " +
                          "WHERE \"user\" = ? AND \"date\" = ? ALLOW FILTERING",
                          rows => rows.WithParams(userId, date))
                      .ThenRowsSuccess(
                          new [] { "date", "time", "user" },
                          rows => rows.WithRows(data.Select(
                              t => new object [] { t.Date, t.TimeColumn, t.UserId }).ToArray())));
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT \"date\", \"time\", \"user\" FROM \"{TestTable.TableName}\" " +
                          "WHERE \"user\" = ? AND \"date\" = ? AND \"time\" >= ? ORDER BY \"time\" ALLOW FILTERING",
                          rows => rows.WithParams(userId, date, time))
                      .ThenRowsSuccess(
                          new [] { "date", "time", "user" },
                          rows => rows.WithRows(data.Where(t => t.TimeColumn >= time).OrderBy(t => t.TimeColumn).Select(
                              t => new object [] { t.Date, t.TimeColumn, t.UserId }).ToArray())));
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT \"date\", \"time\", \"user\" FROM \"{TestTable.TableName}\" " +
                          "WHERE \"user\" = ? AND \"date\" = ? AND \"time\" <= ? ORDER BY \"time\" DESC ALLOW FILTERING",
                          rows => rows.WithParams(userId, date, time))
                      .ThenRowsSuccess(
                          new [] { "date", "time", "user" },
                          rows => rows.WithRows(data.Where(t => t.TimeColumn <= time).OrderByDescending(t => t.TimeColumn).Select(
                              t => new object [] { t.Date, t.TimeColumn, t.UserId }).ToArray())));

            var query1Actual = table.Where(i => i.UserId == userId && i.Date == date);

            var query2Actual = query1Actual.Where(i => i.TimeColumn >= time);
            query2Actual = query2Actual.OrderBy(i => i.TimeColumn); // ascending

            var query3Actual = query1Actual.Where(i => i.TimeColumn <= time);
            query3Actual = query3Actual.OrderByDescending(i => i.TimeColumn);

            var query1Expected = "SELECT \"date\", \"time\", \"user\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? ALLOW FILTERING";
            var query2Expected = "SELECT \"date\", \"time\", \"user\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? AND \"time\" >= ? ORDER BY \"time\" ALLOW FILTERING";
            var query3Expected = "SELECT \"date\", \"time\", \"user\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? AND \"time\" <= ? ORDER BY \"time\" DESC ALLOW FILTERING";

            Assert.AreEqual(query1Expected, query1Actual.ToString());
            Assert.AreEqual(query2Expected, query2Actual.ToString());
            Assert.AreEqual(query3Expected, query3Actual.ToString());

            var result2Actual = query2Actual.Execute().ToList();
            var result3Actual = query3Actual.Execute().ToList();

            Assert.AreEqual(3, result2Actual.First().TimeColumn);
            Assert.AreEqual(5, result2Actual.Last().TimeColumn);
            Assert.AreEqual(3, result3Actual.First().TimeColumn);
            Assert.AreEqual(1, result3Actual.Last().TimeColumn);
        }

        [Test]
        public void LinqWhere_TupleWithClusteringKeys()
        {
            var tableName = TestUtils.GetUniqueTableName();
            var map = new Map<TestTable>()
                .ExplicitColumns()
                .Column(t => t.UserId, cm => cm.WithName("id"))
                .Column(t => t.Date, cm => cm.WithName("date"))
                .Column(t => t.TimeColumn, cm => cm.WithName("time"))
                .PartitionKey(t => t.UserId)
                .ClusteringKey(t => t.Date)
                .TableName(tableName);

            var table = new Table<TestTable>(Session, new MappingConfiguration().Define(map));
            var data = new TestTable
            {
                UserId = 1,
                Date = 1,
                TimeColumn = 1
            };
            var localList = new List<int> { 0, 2 };
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT date, id, time FROM {tableName} " +
                          "WHERE id = ? AND date IN ?",
                          rows => rows.WithParams(1, localList))
                      .ThenRowsSuccess(
                          ("date", DataType.GetDataType(typeof(int))), 
                          ("id",DataType.GetDataType(typeof(int))), 
                          ("time",DataType.GetDataType(typeof(long)))));

            var emptyResults = table.Where(t => t.UserId == 1 && localList.Contains(t.Date)).Execute();
            Assert.NotNull(emptyResults);
            Assert.AreEqual(0, emptyResults.ToArray().Length);

            localList.Add(1); //adding to list existent tuple

            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT date, id, time FROM {tableName} " +
                          "WHERE id = ? AND date IN ?",
                          rows => rows.WithParams(1, localList))
                      .ThenRowsSuccess(
                          new [] { "date", "id", "time" },
                          rows => rows.WithRow(data.Date, data.UserId, data.TimeColumn)));

            var tCreateResults = table.Where(t => t.UserId == 1 && localList.Contains(t.Date)).Execute();
            Assert.NotNull(tCreateResults);
            var tCreateResultsArr = tCreateResults.ToArray();
            Assert.AreEqual(1, tCreateResultsArr.Length);
            var tCreateResultObj = tCreateResultsArr[0];
            Assert.AreEqual(1, tCreateResultObj.UserId);
            Assert.AreEqual(1, tCreateResultObj.Date);
            Assert.AreEqual(1, tCreateResultObj.UserId);

            //invalid case: string.Contains
            Assert.Throws<InvalidOperationException>(() =>
                table.Where(t => t.UserId == 1 && "error".Contains($"{t.Date}")).Execute());
        }

        [Test]
        public void LinqWhere_TupleWithCompositeKeys()
        {
            var tableName = TestUtils.GetUniqueTableName();
            var map = new Map<TestTable>()
                .ExplicitColumns()
                .Column(t => t.UserId, cm => cm.WithName("id"))
                .Column(t => t.Date, cm => cm.WithName("date"))
                .Column(t => t.TimeColumn, cm => cm.WithName("time"))
                .PartitionKey(t => t.UserId)
                .ClusteringKey(t => t.Date)
                .ClusteringKey(t => t.TimeColumn)
                .TableName(tableName);

            var table = new Table<TestTable>(Session, new MappingConfiguration().Define(map));
            var localList = new List<Tuple<int, long>> { Tuple.Create(0, 0L), Tuple.Create(0, 2L) };
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT date, id, time FROM {tableName} " +
                          "WHERE id = ? AND (date, time) IN ?",
                          rows => rows.WithParams(1, localList))
                      .ThenRowsSuccess(
                          ("date", DataType.GetDataType(typeof(int))), 
                          ("id",DataType.GetDataType(typeof(int))), 
                          ("time",DataType.GetDataType(typeof(long)))));

            var emptyResults = table.Where(t => t.UserId == 1 && localList.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(emptyResults);
            Assert.AreEqual(0, emptyResults.ToArray().Length);
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT date, id, time FROM {tableName} " +
                          "WHERE id = ? AND (date, time) IN ?",
                          rows => rows.WithParams(1, new List<Tuple<int, long>>()))
                      .ThenRowsSuccess(
                          ("date", DataType.GetDataType(typeof(int))), 
                          ("id",DataType.GetDataType(typeof(int))), 
                          ("time",DataType.GetDataType(typeof(long)))));

            emptyResults = table.Where(t => t.UserId == 1 && new List<Tuple<int, long>>().Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(emptyResults);
            Assert.AreEqual(0, emptyResults.ToArray().Length);

            localList.Add(Tuple.Create(1, 1L)); //adding to list existent tuple
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT date, id, time FROM {tableName} " +
                          "WHERE id = ? AND (date, time) IN ?",
                          rows => rows.WithParams(1, localList))
                      .ThenRowsSuccess(new[] { "date", "id", "time" }, rows => rows.WithRow(1, 1, 1L)));

            var tCreateResults = table.Where(t => t.UserId == 1 && localList.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(tCreateResults);
            var tCreateResultsArr = tCreateResults.ToArray();
            Assert.AreEqual(1, tCreateResultsArr.Length);
            var tCreateResultObj = tCreateResultsArr[0];
            Assert.AreEqual(1, tCreateResultObj.UserId);
            Assert.AreEqual(1, tCreateResultObj.Date);
            Assert.AreEqual(1, tCreateResultObj.UserId);
        }

        [Test]
        public void LinqWhere_TupleWithCompositeKeys_Scopes()
        {
            var tableName = TestUtils.GetUniqueTableName();
            var map = new Map<TestTable>()
                .ExplicitColumns()
                .Column(t => t.UserId, cm => cm.WithName("id"))
                .Column(t => t.Date, cm => cm.WithName("date"))
                .Column(t => t.TimeColumn, cm => cm.WithName("time"))
                .PartitionKey(t => t.UserId)
                .ClusteringKey(t => t.Date)
                .ClusteringKey(t => t.TimeColumn)
                .TableName(tableName);

            var table = new Table<TestTable>(Session, new MappingConfiguration().Define(map));
            var anomObj = new
            {
                list = new List<Tuple<int, long>> { Tuple.Create(0, 0L), Tuple.Create(1, 1L), Tuple.Create(0, 2L) }
            };

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT date, id, time FROM {tableName} " +
                          "WHERE id = ? AND (date, time) IN ?",
                          rows => rows.WithParams(1, anomObj.list))
                      .ThenRowsSuccess(new[] { "date", "id", "time" }, rows => rows.WithRow(1, 1, 1L)));

            var listInsideObjResults = table.Where(t => t.UserId == 1 && anomObj.list.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(listInsideObjResults);
            Assert.AreEqual(1, listInsideObjResults.Count());
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT date, id, time FROM {tableName} " +
                          "WHERE id = ? AND (date, time) IN ?",
                          rows => rows.WithParams(1, _tupleList))
                      .ThenRowsSuccess(new[] { "date", "id", "time" }, rows => rows.WithRow(1, 1, 1L)));

            var listOuterScopeResults = table.Where(t => t.UserId == 1 && _tupleList.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(listOuterScopeResults);
            Assert.AreEqual(1, listOuterScopeResults.Count());
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT date, id, time FROM {tableName} " +
                          "WHERE id = ? AND (date, time) IN ?",
                          rows => rows.WithParams(1, TupleList))
                      .ThenRowsSuccess(new[] { "date", "id", "time" }, rows => rows.WithRow(1, 1, 1L)));

            var listOuterStaticScopeResults = table.Where(t => t.UserId == 1 && TupleList.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(listOuterStaticScopeResults);
            Assert.AreEqual(1, listOuterStaticScopeResults.Count());
        }

        [Test]
        [TestCassandraVersion(3, 0)]
        public void LinqWhere_Boolean()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"BooleanType\", \"DateTimeOffsetType\", \"DateTimeType\", \"DecimalType\", " +
                          "\"DictionaryStringLongType\", \"DictionaryStringStringType\", \"DoubleType\", \"FloatType\", " +
                          "\"GuidType\", \"Int64Type\", \"IntType\", \"ListOfGuidsType\", \"ListOfStringsType\", " +
                          "\"NullableIntType\", \"StringType\" " +
                          $"FROM \"{ManyDataTypesEntity.TableName}\" " +
                          $"WHERE \"BooleanType\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(true))
                      .ThenRowsSuccess(ManyDataTypesEntity.GetColumnsWithTypes()));

            //there are no records with BooleanType == true
            var rs = _manyDataTypesEntitiesTable.Where(m => m.BooleanType).Execute();
            Assert.NotNull(rs);
            var rows = rs.ToArray();
            Assert.AreEqual(0, rows.Length);
            var guid = Guid.NewGuid();
            var data = new ManyDataTypesEntity
            {
                BooleanType = true,
                DateTimeOffsetType = DateTimeOffset.Now,
                DateTimeType = DateTime.Now,
                DecimalType = 10,
                DoubleType = 10.0,
                FloatType = 10.0f,
                GuidType = guid,
                IntType = 10,
                StringType = "Boolean True"
            };

            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"BooleanType\", \"DateTimeOffsetType\", \"DateTimeType\", \"DecimalType\", " +
                          "\"DictionaryStringLongType\", \"DictionaryStringStringType\", \"DoubleType\", \"FloatType\", " +
                          "\"GuidType\", \"Int64Type\", \"IntType\", \"ListOfGuidsType\", \"ListOfStringsType\", " +
                          "\"NullableIntType\", \"StringType\" " +
                          $"FROM \"{ManyDataTypesEntity.TableName}\" " +
                          $"WHERE \"BooleanType\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(true))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));

            rs = _manyDataTypesEntitiesTable.Where(m => m.BooleanType).Execute();
            Assert.NotNull(rs);
            rows = rs.ToArray();
            Assert.AreEqual(1, rows.Length);
            Assert.AreEqual(guid, rows[0].GuidType);
            Assert.AreEqual("Boolean True", rows[0].StringType);
            Assert.IsTrue(rows[0].BooleanType);
        }

        [Test]
        [TestCassandraVersion(3, 0)]
        public void LinqWhere_BooleanScopes()
        {
            //Get no records
            const bool all = true;
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"BooleanType\", \"DateTimeOffsetType\", \"DateTimeType\", \"DecimalType\", " +
                          "\"DictionaryStringLongType\", \"DictionaryStringStringType\", \"DoubleType\", \"FloatType\", " +
                          "\"GuidType\", \"Int64Type\", \"IntType\", \"ListOfGuidsType\", \"ListOfStringsType\", " +
                          "\"NullableIntType\", \"StringType\" " +
                          $"FROM \"{ManyDataTypesEntity.TableName}\" " +
                          $"WHERE \"BooleanType\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(all))
                      .ThenRowsSuccess(ManyDataTypesEntity.GetColumnsWithTypes()));
            var rs = _manyDataTypesEntitiesTable.Where(m => m.BooleanType == all).Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(0, rs.Count());
            
            //get all records
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"BooleanType\", \"DateTimeOffsetType\", \"DateTimeType\", \"DecimalType\", " +
                          "\"DictionaryStringLongType\", \"DictionaryStringStringType\", \"DoubleType\", \"FloatType\", " +
                          "\"GuidType\", \"Int64Type\", \"IntType\", \"ListOfGuidsType\", \"ListOfStringsType\", " +
                          "\"NullableIntType\", \"StringType\" " +
                          $"FROM \"{ManyDataTypesEntity.TableName}\" " +
                          $"WHERE \"BooleanType\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(false))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(),
                          r => r.WithRow(_manyDataTypesEntitiesList.First().GetColumnValues())));
            rs = _manyDataTypesEntitiesTable.Where(m => m.BooleanType == bool.Parse("false")).Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
        }

        [Test, TestCassandraVersion(3, 0)]
        public void LinqWhere_ShortScopes()
        {
            var cqlSelect =
                "SELECT \"BooleanType\", \"DateTimeOffsetType\", \"DateTimeType\", \"DecimalType\", " +
                "\"DictionaryStringLongType\", \"DictionaryStringStringType\", \"DoubleType\", \"FloatType\", " +
                "\"GuidType\", \"Int64Type\", \"IntType\", \"ListOfGuidsType\", \"ListOfStringsType\", " +
                "\"NullableIntType\", \"StringType\" " +
                $"FROM \"{ManyDataTypesEntity.TableName}\" ";

            var guid = Guid.NewGuid();
            const string pk = "Boolean True";
            var data = new ManyDataTypesEntity
            {
                BooleanType = true,
                DateTimeOffsetType = DateTimeOffset.Now,
                DateTimeType = DateTime.Now,
                DecimalType = 11,
                DoubleType = 11.0,
                FloatType = 11.0f,
                GuidType = guid,
                IntType = 11,
                Int64Type = 11,
                StringType = pk
            };

            //Get poco using constant short
            const short expectedShortValue = 11;
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          cqlSelect +
                          "WHERE \"StringType\" = ? AND \"IntType\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(pk, (int)expectedShortValue))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));

            var rs = _manyDataTypesEntitiesTable
                     .Where(m => m.StringType == pk && m.IntType == expectedShortValue).AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          cqlSelect +
                          "WHERE \"StringType\" = ? AND \"IntType\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(pk, (int)expectedShortValue))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));

            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && m.IntType == ExpectedShortValue)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          cqlSelect +
                          "WHERE \"StringType\" = ? AND \"IntType\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(pk, (int)expectedShortValue))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && ExpectedShortValue == m.IntType)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          cqlSelect +
                          "WHERE \"StringType\" = ? AND \"IntType\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(pk, (int)expectedShortValue))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && expectedShortValue == m.IntType)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());

            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          cqlSelect +
                          "WHERE \"StringType\" = ? AND \"Int64Type\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(pk, (long)expectedShortValue))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && m.Int64Type == expectedShortValue)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          cqlSelect +
                          "WHERE \"StringType\" = ? AND \"Int64Type\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(pk, (long)expectedShortValue))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && expectedShortValue == m.Int64Type)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            
            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          cqlSelect +
                          "WHERE \"StringType\" = ? AND \"Int64Type\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(pk, (long)expectedShortValue))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && m.Int64Type == ExpectedShortValue)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());

            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          cqlSelect +
                          "WHERE \"StringType\" = ? AND \"Int64Type\" = ? " +
                          "ALLOW FILTERING",
                          p => p.WithParams(pk, (long)expectedShortValue))
                      .ThenRowsSuccess(
                          ManyDataTypesEntity.GetColumnsWithTypes(), 
                          r => r.WithRow(data.GetColumnValues())));
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && ExpectedShortValue == m.Int64Type)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
        }

        [Test]
        public void LinqWhere_Recovers_From_Invalid_Query_Exception()
        {
            var table = new Table<Song>(Session, new MappingConfiguration().Define(
                new Map<Song>().TableName("tbl_recovers_invalid_test").PartitionKey(x => x.Title)));
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT Artist, Id, ReleaseDate, Title FROM tbl_recovers_invalid_test" +
                          " WHERE Title = ?",
                          p => p.WithParams("Do I Wanna Know"))
                      .ThenServerError(ServerError.Invalid, "msg"));

            Assert.Throws<InvalidQueryException>(() => table.Where(x => x.Title == "Do I Wanna Know").Execute());

            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT Artist, Id, ReleaseDate, Title FROM tbl_recovers_invalid_test" +
                          " WHERE Title = ?",
                          p => p.WithParams("Do I Wanna Know"))
                      .ThenRowsSuccess(
                          ("Artist", DataType.GetDataType(typeof(string))), 
                          ("Id", DataType.GetDataType(typeof(Guid))), 
                          ("ReleaseDate", DataType.GetDataType(typeof(DateTimeOffset))), 
                          ("Title", DataType.GetDataType(typeof(string)))));

            Assert.AreEqual(0, table.Where(x => x.Title == "Do I Wanna Know").Execute().Count());
        }

        [AllowFiltering]
        [Table(TestTable.TableName)]
        public class TestTable
        {
            public const string TableName = "test1";

            [PartitionKey(1)]
            [Column("user")]
            public int UserId { get; set; }

            [PartitionKey(2)]
            [Column("date")]
            public int Date { get; set; }

            [ClusteringKey(1)]
            [Column("time")]
            public long TimeColumn { get; set; }
        }
    }
}