using System;
using System.Collections.Generic;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class UpdateIf : TestGlobals
    {
        ISession _session = null;
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
            _session.DeleteKeyspace(_uniqueKsName);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void LinqTable_UpdateIf()
        {
            _movieTable.CreateIfNotExists();
            var movie = new Movie()
            {
                Title = "Dead Poets Society",
                Year = 1989,
                MainActor = "Robin Williams",
                Director = "Peter Weir",
                MovieMaker = "Touchstone"
            };
            _movieTable.Insert(movie).SetConsistencyLevel(ConsistencyLevel.Quorum).Execute();

            var retrievedMovie = _movieTable
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Movie.AssertEquals(movie, retrievedMovie);
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual(1989, retrievedMovie.Year);
            Assert.AreEqual("Robin Williams", retrievedMovie.MainActor);

            _movieTable
                .Where(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new Movie { MainActor = "Robin McLaurin Williams" })
                .UpdateIf(m => m.Year == 1989)
                .Execute();

            retrievedMovie = _movieTable
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual(1989, retrievedMovie.Year);
            Assert.AreEqual("Robin McLaurin Williams", retrievedMovie.MainActor);

            //Should not update as the if clause is not satisfied
            var updateIf = _movieTable
                .Where(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new Movie { MainActor = "WHOEVER" })
                .UpdateIf(m => m.Year == 1500);
            string updateIfToStr = updateIf.ToString();
            Console.WriteLine(updateIfToStr);

            updateIf.Execute();
            retrievedMovie = _movieTable
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual("Robin McLaurin Williams", retrievedMovie.MainActor);
        }


    }
}
