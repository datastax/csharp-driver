﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
#pragma warning disable 618
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Where : SharedClusterTest
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            MappingConfiguration movieMappingConfig = new MappingConfiguration();
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(_session, movieMappingConfig);
            _movieTable.Create();

            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [Test]
        public void LinqWhere_ExecuteAsync()
        {
            var expectedMovie = _movieList.First();

            // test
            var taskSelect = _movieTable.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).ExecuteAsync();
            List<Movie> movies = taskSelect.Result.ToList();
            Assert.AreEqual(1, movies.Count);

            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void LinqWhere_ExecuteSync()
        {
            var expectedMovie = _movieList.First();

            // test
            List<Movie> movies = _movieTable.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute().ToList();
            Assert.AreEqual(1, movies.Count);

            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void LinqWhere_NoSuchRecord()
        {
            Movie existingMovie = _movieList.Last();
            string randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            List<Movie> movies = _movieTable.Where(m => m.Title == existingMovie.Title && m.MovieMaker == randomStr).Execute().ToList();
            Assert.AreEqual(0, movies.Count);
        }

        [Test]
        public void LinqTable_TooManyEqualsClauses()
        {
            _movieTable.CreateIfNotExists();
            var movieQuery = _movieTable
                .Where(m => m.Title == "doesntmatter" && m.MovieMaker == "doesntmatter")
                .Where(m => m.Title == "doesntmatter" && m.MovieMaker == "doesntmatter");
            Assert.Throws<InvalidQueryException>(() => movieQuery.Execute());
        }

        [Test]
        [TestCassandraVersion(3, 0, Comparison.LessThan)]
        public void LinqWhere_Exception()
        {
            //No translation in CQL
            Assert.Throws<SyntaxError>(() => _movieTable.Where(m => m.Year is int).Execute());
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.MainActor == null).Execute());
            //No execute
            Assert.Throws<InvalidOperationException>(() => _movieTable.Where(m => m.MovieMaker == "dum").GetEnumerator());

            //Wrong consistency level
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.MovieMaker == "dum").SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        [Test]
        [TestCassandraVersion(3, 0, Comparison.LessThan)]
        public void LinqWhere_NoPartitionKey()
        {
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => _movieTable.Where(m => m.MainActor == null).Execute());
        }

        [Test]
        public void LinqWhere_NoTranslationFromLinqToCql()
        {
            Assert.Throws<SyntaxError>(() => _movieTable.Where(m => m.Year is int).Execute());
        }

        [Test]
        public void LinqWhere_ExecuteStepOmitted()
        {
            Assert.Throws<InvalidOperationException>(() => _movieTable.Where(m => m.MovieMaker == "dum").GetEnumerator());
        }

        [Test]
        public void LinqWhere_With_LocalSerial_ConsistencyLevel_Does_Not_Throw()
        {
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

            MappingConfiguration config = new MappingConfiguration();
            config.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(TestTable), () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(TestTable)));
            Table<TestTable> table = new Table<TestTable>(_session, config);
            table.CreateIfNotExists();

            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 1 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 2 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 3 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 4 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 5 }).Execute();

            CqlQuery<TestTable> query1Actual = table.Where(i => i.UserId == userId && i.Date == date);

            CqlQuery<TestTable> query2Actual = query1Actual.Where(i => i.TimeColumn >= time);
            query2Actual = query2Actual.OrderBy(i => i.TimeColumn); // ascending

            CqlQuery<TestTable> query3Actual = query1Actual.Where(i => i.TimeColumn <= time);
            query3Actual = query3Actual.OrderByDescending(i => i.TimeColumn);

            string query1Expected = "SELECT \"user\", \"date\", \"time\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? ALLOW FILTERING";
            string query2Expected = "SELECT \"user\", \"date\", \"time\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? AND \"time\" >= ? ORDER BY \"time\" ALLOW FILTERING";
            string query3Expected = "SELECT \"user\", \"date\", \"time\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? AND \"time\" <= ? ORDER BY \"time\" DESC ALLOW FILTERING";

            Assert.AreEqual(query1Expected, query1Actual.ToString());
            Assert.AreEqual(query2Expected, query2Actual.ToString());
            Assert.AreEqual(query3Expected, query3Actual.ToString());

            List<TestTable> result2Actual = query2Actual.Execute().ToList();
            List<TestTable> result3Actual = query3Actual.Execute().ToList();

            Assert.AreEqual(3, result2Actual.First().TimeColumn);
            Assert.AreEqual(5, result2Actual.Last().TimeColumn);
            Assert.AreEqual(3, result3Actual.First().TimeColumn);
            Assert.AreEqual(1, result3Actual.Last().TimeColumn);
        }

        /// @jira_ticket CSHARP-577
        [Test]
        public void LinqWhere_Expression_From_Property_Type()
        {
            MappingConfiguration config = new MappingConfiguration();
            config.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(TestTable), () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(TestTable)));
            
            Table<TestTable> table = new Table<TestTable>(_session, config);
            table.CreateIfNotExists();

            var obj = new TestTableMirror { Id = 1, Date = 12, TimeColumn = DateTime.UtcNow };
            var guid = Guid.NewGuid();
            var cqlInsert = table.Insert(new TestTable() {UserId = obj.Id, Date = obj.Date, TimeColumn = obj.TimeColumn.Ticks, Guid = guid, IntCol = TestTableMirror.IntConst});
            Session.Execute(cqlInsert);
            var data = table.Where(dto => dto.UserId == obj.Id 
                                    && dto.Date == obj.Date 
                                    && dto.TimeColumn == obj.TimeColumn.Ticks 
                                    && dto.Guid == new Guid(guid.ToString())
                                    && dto.IntCol == TestTableMirror.IntConst).Execute().First();
            Assert.AreEqual(obj.Id, data.UserId);
            Assert.AreEqual(obj.TimeColumn.Ticks, data.TimeColumn);
            Assert.AreEqual(obj.Date, data.Date);
            Assert.AreEqual(TestTableMirror.IntConst, data.IntCol);
            Assert.AreEqual(guid, data.Guid);
        }

        private class TestTableMirror
        {
            public int Id { get; set; }
            public int Date { get; set; }
            public DateTime TimeColumn { get; set; }

            public const int IntConst = 12;
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
            public long TimeColumn { get; set; }

            [ClusteringKey(2)]
            [Column("guid")]
            public Guid Guid { get; set; }

            [ClusteringKey(3)]
            [Column("intCol")]
            public int IntCol { get; set; }
        }
    }
}
