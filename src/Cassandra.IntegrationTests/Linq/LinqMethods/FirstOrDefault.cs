using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short"), TestCassandraVersion(2, 0)]
    public class FirstOrDefault : TestGlobals
    {
        private ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
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

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        [Test]
        public void LinqFirstOrDefault_Sync()
        {
            var expectedMovie = _movieList.First();

            // Test
            var first =
                _movieTable.FirstOrDefault(
                    m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute();
            Assert.IsNotNull(first);
            Assert.AreEqual(expectedMovie.MovieMaker, first.MovieMaker);
        }

        [Test]
        public void LinqFirstOrDefault_Sync_NoSuchRecord()
        {
            var first = _movieTable.FirstOrDefault(m => m.Director == "non_existant_" + Randomm.RandomAlphaNum(10)).Execute();
            Assert.IsNull(first);
        }

        [Test]
        public void LinqFirstOrDefault_Async()
        {
            // Setup
            _movieTable = new Table<Movie>(_session, new MappingConfiguration());
            var expectedMovie = _movieList.Last();

            // Test
            var actualMovie =
                _movieTable.FirstOrDefault(
                    m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker)
                     .ExecuteAsync()
                     .Result;
            Assert.IsNotNull(actualMovie);
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void LinqFirstOrDefault_Async_NoSuchRecord()
        {
            var table = new Table<Movie>(_session, new MappingConfiguration());
            var first = table.FirstOrDefault(m => m.Director == "non_existant_" + Randomm.RandomAlphaNum(10)).ExecuteAsync().Result;
            Assert.IsNull(first);
        }
    }
}
