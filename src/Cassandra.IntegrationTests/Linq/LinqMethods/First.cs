using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short"), TestCassandraVersion(2,0)]
    public class First : TestGlobals
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        private string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
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

            // Wait for data to be query-able
            DateTime futureDateTime = DateTime.Now.AddSeconds(2); // it should not take very long for these records to become available for querying!
            while (DateTime.Now < futureDateTime && _movieTable.Count().Execute() < _movieList.Count)
                Thread.Sleep(200);
            Assert.AreEqual(_movieList.Count(), _movieTable.Count().Execute(), "Setup failure: Expected number of records are not query-able");
        }

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        [Test]
        public void First_ExecuteAsync()
        {
            var expectedMovie = _movieList.First();

            var actualMovie = _movieTable.First(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).ExecuteAsync().Result;
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void First_ExecuteSync()
        {
            var expectedMovie = _movieList.First();

            var actualMovie = _movieTable.First(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void First_NoSuchRecord()
        {
            Movie existingMovie = _movieList.Last();
            string randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            Movie foundMovie = _movieTable.First(m => m.Title == existingMovie.Title && m.MovieMaker == randomStr).Execute();
            Assert.Null(foundMovie);
        }

        ///////////////////////////////////////////////
        /// Exceptions
        ///////////////////////////////////////////////

        [Test]
        public void First_NoTranslationFromLinqToCql()
        {
            //No translation in CQL
            Assert.Throws<SyntaxError>(() => _movieTable.First(m => m.Year is int).Execute());
        }

        [Test]
        public void First_NoPartitionKey()
        {
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => _movieTable.First(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => _movieTable.First(m => m.MainActor == null).Execute());
        }

        [Test]
        public void First_WrongConsistencyLevel()
        {
            Assert.Throws<InvalidQueryException>(() => _movieTable.First(m => m.MovieMaker == "dum").SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        [Test]
        public void First_MissingPartitionKey()
        {
            string randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            try
            {
                _movieTable.First(m => m.Title == randomStr).Execute();
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
