using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Tests
{
    [Category("short")]
    public class FirstOrDefault : TestGlobals
    {
        private ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var table = new Table<Movie>(_session, new MappingConfiguration());
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
        public void LinqFirstOrDefault_Sync()
        {
            // Setup
            var table = new Table<Movie>(_session, new MappingConfiguration());
            var expectedMovie = _movieList.First();

            // Test
            var first =
                table.FirstOrDefault(
                    m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute();
            Assert.IsNotNull(first);
            Assert.AreEqual(expectedMovie.MovieMaker, first.MovieMaker);
        }

        [Test]
        public void LinqFirstOrDefault_Sync_NoSuchRecord()
        {
            var table = _session.GetTable<Movie>();
            var first = table.FirstOrDefault(m => m.Director == "non_existant_" + Randomm.RandomAlphaNum(10)).Execute();
            Assert.IsNull(first);
        }

        [Test]
        public void LinqFirstOrDefault_Async()
        {
            // Setup
            var table = new Table<Movie>(_session, new MappingConfiguration());
            var expectedMovie = _movieList.Last();

            // Test
            var actualMovie =
                table.FirstOrDefault(
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
