using System;
using System.Collections.Generic;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Tests
{
    [Category("short")]
    public class UpdateIf : TestGlobals
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
        [TestCassandraVersion(2, 0)]
        public void LinqTable_UpdateIf()
        {
            var table = _session.GetTable<Movie>();
            table.CreateIfNotExists();
            var movie = new Movie()
            {
                Title = "Dead Poets Society",
                Year = 1989,
                MainActor = "Robin Williams",
                Director = "Peter Weir",
                MovieMaker = "Touchstone"
            };
            table.Insert(movie).SetConsistencyLevel(ConsistencyLevel.Quorum).Execute();

            var retrievedMovie = table
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Movie.AssertEquals(movie, retrievedMovie);
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual(1989, retrievedMovie.Year);
            Assert.AreEqual("Robin Williams", retrievedMovie.MainActor);

            table
                .Where(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new Movie { MainActor = "Robin McLaurin Williams" })
                .UpdateIf(m => m.Year == 1989)
                .Execute();

            retrievedMovie = table
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual(1989, retrievedMovie.Year);
            Assert.AreEqual("Robin McLaurin Williams", retrievedMovie.MainActor);

            //Should not update as the if clause is not satisfied
            var updateIf = table
                .Where(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone" && m.Director == "Peter Weir")
                .Select(m => new Movie { MainActor = "WHOEVER" })
                .UpdateIf(m => m.Year == 1500);
            string updateIfToStr = updateIf.ToString();
            Console.WriteLine(updateIfToStr);

            updateIf.Execute();
            retrievedMovie = table
                .FirstOrDefault(m => m.Title == "Dead Poets Society" && m.MovieMaker == "Touchstone")
                .Execute();
            Assert.NotNull(retrievedMovie);
            Assert.AreEqual("Robin McLaurin Williams", retrievedMovie.MainActor);
        }


    }
}
