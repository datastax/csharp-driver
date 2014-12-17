using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Cassandra.IntegrationTests.Linq.Tests
{
    [Category("short")]
    public class First : TestGlobals
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
        public void First_ExecuteAsync()
        {
            var table = _session.GetTable<Movie>();
            var expectedMovie = _movieList.First();

            var actualMovie = table.First(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).ExecuteAsync().Result;
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void First_ExecuteSync()
        {
            var table = _session.GetTable<Movie>();
            var expectedMovie = _movieList.First();

            var actualMovie = table.First(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void First_NoSuchRecord()
        {
            var table = _session.GetTable<Movie>();
            Movie existingMovie = _movieList.Last();
            string randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            Movie foundMovie = table.First(m => m.Title == existingMovie.Title && m.MovieMaker == randomStr).Execute();
            Assert.Null(foundMovie);
        }

        ///////////////////////////////////////////////
        /// Exceptions
        ///////////////////////////////////////////////

        [Test]
        public void First_NoTranslationFromLinqToCql()
        {
            var table = _session.GetTable<Movie>();
            //No translation in CQL
            Assert.Throws<SyntaxError>(() => table.First(m => m.Year is int).Execute());
        }

        [Test]
        public void First_NoPartitionKey()
        {
            var table = _session.GetTable<Movie>();
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => table.First(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => table.First(m => m.MainActor == null).Execute());
        }

        [Test]
        public void First_WrongConsistencyLevel()
        {
            var table = _session.GetTable<Movie>();
            Assert.Throws<InvalidQueryException>(() => table.First(m => m.MovieMaker == "dum").SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        [Test]
        public void First_MissingPartitionKey()
        {
            var table = _session.GetTable<Movie>();
            string randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            try
            {
                table.First(m => m.Title == randomStr).Execute();
                Assert.Fail("expected exception was not thrown!");
            }
            catch (InvalidQueryException e)
            {
                string expectedErrMsg = "Partition key part movie_maker must be restricted since preceding part is";
                Assert.That(e.Message, new EqualConstraint(expectedErrMsg));
            }
        }




    }
}
