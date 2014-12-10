using System;
using System.Collections.Generic;
using System.Linq;
using System.Management.Instrumentation;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Tests
{
    [Category("short")]
    public class Where : TestGlobals
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var table = _session.GetTable<Movie>();
            table.Create();

            //Insert some data
            foreach (var movie in _movieList)
                table.Insert(movie).Execute();
        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        [Test]
        public void LinqWhere_ExecuteAsync()
        {
            // Setup
            var table = _session.GetTable<Movie>();
            var expectedMovie = _movieList.First();

            // test
            var taskSelect = table.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).ExecuteAsync();
            List<Movie> movies = taskSelect.Result.ToList();
            Assert.AreEqual(1, movies.Count);

            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void LinqWhere_ExecuteSync()
        {
            // Setup
            var table = _session.GetTable<Movie>();
            var expectedMovie = _movieList.First();

            // test
            List<Movie> movies = table.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute().ToList();
            Assert.AreEqual(1, movies.Count);

            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void LinqWhere_NoSuchRecord()
        {
            var table = _session.GetTable<Movie>();
            Movie existingMovie = _movieList.Last();
            string randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            List<Movie> movies = table.Where(m => m.Title == existingMovie.Title && m.MovieMaker == randomStr).Execute().ToList();
            Assert.AreEqual(0, movies.Count);
        }

        [Test]
        public void LinqTable_TooManyEqualsClauses()
        {
            var table = _session.GetTable<Movie>();
            table.CreateIfNotExists();
            var movieQuery = table
                .Where(m => m.Title == "doesntmatter" && m.MovieMaker == "doesntmatter")
                .Where(m => m.Title == "doesntmatter" && m.MovieMaker == "doesntmatter");
            Assert.Throws<InvalidQueryException>(() => movieQuery.Execute());
        }

        [Test]
        public void LinqWhere_Exception()
        {
            var table = _session.GetTable<Movie>();
            //No translation in CQL
            Assert.Throws<SyntaxError>(() => table.Where(m => m.Year is int).Execute());
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.MainActor == null).Execute());
            //No execute
            Assert.Throws<InvalidOperationException>(() => table.Where(m => m.MovieMaker == "dum").GetEnumerator());

            //Wrong consistency level
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.MovieMaker == "dum").SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        [Test]
        public void LinqWhere_NoPartitionKey()
        {
            var table = _session.GetTable<Movie>();
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.MainActor == null).Execute());
        }

        [Test]
        public void LinqWhere_NoTranslationFromLinqToCql()
        {
            var table = _session.GetTable<Movie>();
            Assert.Throws<SyntaxError>(() => table.Where(m => m.Year is int).Execute());
        }

        [Test]
        public void LinqWhere_ExecuteStepOmitted()
        {
            var table = _session.GetTable<Movie>();
            Assert.Throws<InvalidOperationException>(() => table.Where(m => m.MovieMaker == "dum").GetEnumerator());
        }

        [Test]
        public void LinqWhere_WrongConsistencyLevel_Serial()
        {
            var table = _session.GetTable<Movie>();
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.MovieMaker == "dum").SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        [Test]
        public void LinqWhere_WrongConsistencyLevel_LocalSerial()
        {
            var table = _session.GetTable<Movie>();
            Assert.Throws<InvalidQueryException>(() => table.Where(m => m.MovieMaker == "dum").SetConsistencyLevel(ConsistencyLevel.LocalSerial).Execute());
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
            int time = 3;

            Table<TestTable> table = _session.GetTable<TestTable>();
            table.CreateIfNotExists();

            table.Insert(new TestTable { UserId = 1, Date = 2, Token = 1 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, Token = 2 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, Token = 3 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, Token = 4 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, Token = 5 }).Execute();

            CqlQuery<TestTable> query1Actual = table.Where(i => i.UserId == userId && i.Date == date);

            CqlQuery<TestTable> query2Actual = query1Actual.Where(i => i.Token >= time);
            query2Actual = query2Actual.OrderBy(i => i.Token); // ascending

            CqlQuery<TestTable> query3Actual = query1Actual.Where(i => i.Token <= time);
            query3Actual = query3Actual.OrderByDescending(i => i.Token);

            string query1Expected = "SELECT * FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? ALLOW FILTERING";
            string query2Expected = "SELECT * FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? AND \"time\" >= ? ORDER BY \"time\" ALLOW FILTERING";
            string query3Expected = "SELECT * FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? AND \"time\" <= ? ORDER BY \"time\" DESC ALLOW FILTERING";

            Assert.AreEqual(query1Expected, query1Actual.ToString());
            Assert.AreEqual(query2Expected, query2Actual.ToString());
            Assert.AreEqual(query3Expected, query3Actual.ToString());

            List<TestTable> result2Actual = query2Actual.Execute().ToList();
            List<TestTable> result3Actual = query3Actual.Execute().ToList();

            Assert.AreEqual(3, result2Actual.First().Token);
            Assert.AreEqual(5, result2Actual.Last().Token);
            Assert.AreEqual(3, result3Actual.First().Token);
            Assert.AreEqual(1, result3Actual.Last().Token);
        }

        [AllowFiltering]
        [Table("test1")]
        public class TestTable
        {
            [PartitionKey(1)]
            [Column("user")]
            public int UserId { get; set; }

            [PartitionKey(2)]
            [Column("date")]
            public int Date { get; set; }

            [ClusteringKey(1)]
            [Column("time")]
            public long Token { get; set; }
        }
    }
}
