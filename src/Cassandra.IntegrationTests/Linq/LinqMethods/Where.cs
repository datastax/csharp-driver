using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;
#pragma warning disable 618
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Where : SharedClusterTest
    {
        private ISession _session;
        private readonly List<Movie> _movieList = Movie.GetDefaultMovieList();
        private readonly List<ManyDataTypesEntity> _manyDataTypesEntitiesList = ManyDataTypesEntity.GetDefaultAllDataTypesList();
        readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Table<ManyDataTypesEntity> _manyDataTypesEntitiesTable;
        private readonly List<Tuple<int, long>> _tupleList = new List<Tuple<int, long>> {Tuple.Create(0, 0L), Tuple.Create(1, 1L)};
        private static readonly List<Tuple<int, long>> TupleList = new List<Tuple<int, long>> {Tuple.Create(0, 0L), Tuple.Create(1, 1L)};
        const short ExpectedShortValue = 11;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var movieMappingConfig = new MappingConfiguration();
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(ManyDataTypesEntity),
                () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(ManyDataTypesEntity)));
            _movieTable = new Table<Movie>(_session, movieMappingConfig);
            _movieTable.Create();

            _manyDataTypesEntitiesTable = new Table<ManyDataTypesEntity>(_session, movieMappingConfig);
            _manyDataTypesEntitiesTable.Create();

            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();

            foreach (var manyData in _manyDataTypesEntitiesList)
                _manyDataTypesEntitiesTable.Insert(manyData).Execute();
        }

        [Test]
        public void LinqWhere_ExecuteAsync()
        {
            var expectedMovie = _movieList.First();

            // test
            var taskSelect = _movieTable.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).ExecuteAsync();
            var movies = taskSelect.Result.ToList();
            Assert.AreEqual(1, movies.Count);

            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void LinqWhere_ExecuteSync()
        {
            var expectedMovie = _movieList.First();

            // test
            var movies = _movieTable.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute().ToList();
            Assert.AreEqual(1, movies.Count);

            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void LinqWhere_ExecuteSync_Trace()
        {
            var expectedMovie = _movieList.First();

            // test
            var linqWhere = _movieTable.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker);
            linqWhere.EnableTracing();
            var movies = linqWhere.Execute().ToList();
            Assert.AreEqual(1, movies.Count);
            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
            var trace = linqWhere.QueryTrace;
            Assert.NotNull(trace);
            Assert.AreEqual(TestCluster.InitialContactPoint, trace.Coordinator.ToString());
        }

        [Test]
        public void LinqWhere_NoSuchRecord()
        {
            var existingMovie = _movieList.Last();
            var randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            var movies = _movieTable.Where(m => m.Title == existingMovie.Title && m.MovieMaker == randomStr).Execute().ToList();
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
            Assert.Throws<CqlLinqNotSupportedException>(() => _movieTable.Where(m => m.Year is int).Execute());
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
            config.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(TestTable), () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(TestTable)));
            var table = new Table<TestTable>(_session, config);
            table.CreateIfNotExists();

            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 1 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 2 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 3 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 4 }).Execute();
            table.Insert(new TestTable { UserId = 1, Date = 2, TimeColumn = 5 }).Execute();

            var query1Actual = table.Where(i => i.UserId == userId && i.Date == date);

            var query2Actual = query1Actual.Where(i => i.TimeColumn >= time);
            query2Actual = query2Actual.OrderBy(i => i.TimeColumn); // ascending

            var query3Actual = query1Actual.Where(i => i.TimeColumn <= time);
            query3Actual = query3Actual.OrderByDescending(i => i.TimeColumn);

            var query1Expected = "SELECT \"user\", \"date\", \"time\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? ALLOW FILTERING";
            var query2Expected = "SELECT \"user\", \"date\", \"time\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? AND \"time\" >= ? ORDER BY \"time\" ALLOW FILTERING";
            var query3Expected = "SELECT \"user\", \"date\", \"time\" FROM \"test1\" WHERE \"user\" = ? AND \"date\" = ? AND \"time\" <= ? ORDER BY \"time\" DESC ALLOW FILTERING";

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
            var map = new Map<TestTable>()
                .ExplicitColumns()
                .Column(t => t.UserId, cm => cm.WithName("id"))
                .Column(t => t.Date, cm => cm.WithName("date"))
                .Column(t => t.TimeColumn, cm => cm.WithName("time"))
                .PartitionKey(t => t.UserId)
                .ClusteringKey(t => t.Date)
                .TableName(TestUtils.GetUniqueTableName());
            
            var table = new Table<TestTable>(Session, new MappingConfiguration().Define(map));
            table.CreateIfNotExists();
            table.Insert(new TestTable
            {
                UserId = 1,
                Date = 1,
                TimeColumn = 1
            }).Execute();
            var localList = new List<int> {0, 2};
            var emptyResults = table.Where(t => t.UserId == 1 && localList.Contains(t.Date)).Execute();
            Assert.NotNull(emptyResults);
            Assert.AreEqual(0, emptyResults.ToArray().Length);
            
            localList.Add(1); //adding to list existent tuple
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
            var map = new Map<TestTable>()
                .ExplicitColumns()
                .Column(t => t.UserId, cm => cm.WithName("id"))
                .Column(t => t.Date, cm => cm.WithName("date"))
                .Column(t => t.TimeColumn, cm => cm.WithName("time"))
                .PartitionKey(t => t.UserId)
                .ClusteringKey(t => t.Date)
                .ClusteringKey(t => t.TimeColumn)
                .TableName(TestUtils.GetUniqueTableName());
            
            var table = new Table<TestTable>(Session, new MappingConfiguration().Define(map));
            table.CreateIfNotExists();
            table.Insert(new TestTable
            {
                UserId = 1,
                Date = 1,
                TimeColumn = 1
            }).Execute();
            var localList = new List<Tuple<int, long>> {Tuple.Create(0, 0L), Tuple.Create(0, 2L)};
            var emptyResults = table.Where(t => t.UserId == 1 && localList.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(emptyResults);
            Assert.AreEqual(0, emptyResults.ToArray().Length);
            
            emptyResults = table.Where(t => t.UserId == 1 && new List<Tuple<int, long>>().Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(emptyResults);
            Assert.AreEqual(0, emptyResults.ToArray().Length);
            
            localList.Add(Tuple.Create(1, 1L)); //adding to list existent tuple
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
            var map = new Map<TestTable>()
                .ExplicitColumns()
                .Column(t => t.UserId, cm => cm.WithName("id"))
                .Column(t => t.Date, cm => cm.WithName("date"))
                .Column(t => t.TimeColumn, cm => cm.WithName("time"))
                .PartitionKey(t => t.UserId)
                .ClusteringKey(t => t.Date)
                .ClusteringKey(t => t.TimeColumn)
                .TableName(TestUtils.GetUniqueTableName());

            var table = new Table<TestTable>(Session, new MappingConfiguration().Define(map));
            table.CreateIfNotExists();
            table.Insert(new TestTable
            {
                UserId = 1,
                Date = 1,
                TimeColumn = 1
            }).Execute();
            var anomObj = new
            {
                list = new List<Tuple<int, long>> {Tuple.Create(0, 0L), Tuple.Create(1, 1L), Tuple.Create(0, 2L)}
            };
            var listInsideObjResults = table.Where(t => t.UserId == 1 && anomObj.list.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(listInsideObjResults);
            Assert.AreEqual(1, listInsideObjResults.Count());

            var listOuterScopeResults = table.Where(t => t.UserId == 1 && _tupleList.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(listOuterScopeResults);
            Assert.AreEqual(1, listOuterScopeResults.Count());

            var listOuterStaticScopeResults = table.Where(t => t.UserId == 1 && TupleList.Contains(Tuple.Create(t.Date, t.TimeColumn))).Execute();
            Assert.NotNull(listOuterStaticScopeResults);
            Assert.AreEqual(1, listOuterStaticScopeResults.Count());
        }

        [Test]
        [TestCassandraVersion(3,0)]
        public void LinqWhere_Boolean()
        {
            var rs = _manyDataTypesEntitiesTable.Execute();
            Assert.NotNull(rs);
            Assert.Greater(rs.Count(), 0);
            //there are no records with BooleanType == true
            rs = _manyDataTypesEntitiesTable.Where(m => m.BooleanType).Execute();
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
            _manyDataTypesEntitiesTable.Insert(data).Execute();
            rs = _manyDataTypesEntitiesTable.Where(m => m.BooleanType).Execute();
            Assert.NotNull(rs);
            rows = rs.ToArray();
            Assert.AreEqual(1, rows.Length);
            Assert.AreEqual(guid, rows[0].GuidType);
            Assert.AreEqual("Boolean True", rows[0].StringType);
            Assert.IsTrue(rows[0].BooleanType);
            _manyDataTypesEntitiesTable.Select(m => m).Where(m => m.StringType == data.StringType).Delete().Execute();
        }

        [Test]
        [TestCassandraVersion(3,0)]
        public void LinqWhere_BooleanScopes()
        {
            var rs = _manyDataTypesEntitiesTable.Execute();
            Assert.NotNull(rs);
            var resultCount = rs.Count();
            Assert.Greater(resultCount, 0);
            //Get no records
            const bool all = true;
            rs = _manyDataTypesEntitiesTable.Where(m => m.BooleanType == all).Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(0, rs.Count());
            //get all records
            rs = _manyDataTypesEntitiesTable.Where(m => m.BooleanType == bool.Parse("false")).Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(resultCount, rs.Count());
        }

        [Test, TestCassandraVersion(3, 0)]
        public void LinqWhere_ShortScopes()
        {
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
            _manyDataTypesEntitiesTable.Insert(data).Execute();
            //Get poco using constant short
            const short expectedShortValue = 11;
            var rs = _manyDataTypesEntitiesTable
                     .Where(m => m.StringType == pk && m.IntType == expectedShortValue).AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && m.IntType == ExpectedShortValue)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && ExpectedShortValue == m.IntType)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && expectedShortValue == m.IntType)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && m.Int64Type == expectedShortValue)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && expectedShortValue == m.Int64Type)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && m.Int64Type == ExpectedShortValue)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            rs = _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && ExpectedShortValue == m.Int64Type)
                                            .AllowFiltering().Execute();
            Assert.NotNull(rs);
            Assert.AreEqual(1, rs.Count());
            _manyDataTypesEntitiesTable.Where(m => m.StringType == pk && m.IntType == expectedShortValue)
                                       .AllowFiltering().Delete();
        }

        [Test]
        public void LinqWhere_Recovers_From_Invalid_Query_Exception()
        {
            var table = new Table<Song>(_session, new MappingConfiguration().Define(
                new Map<Song>().TableName("tbl_recovers_invalid_test").PartitionKey(x => x.Title)));

            Assert.Throws<InvalidQueryException>(() => table.Where(x => x.Title == "Do I Wanna Know").Execute());

            table.Create();

            Assert.AreEqual(0, table.Where(x => x.Title == "Do I Wanna Know").Execute().Count());
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
        }
    }
}
