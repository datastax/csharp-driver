using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Update : SharedClusterTest
    {
        ISession _session;
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
            var movieMappingConfig = new MappingConfiguration();
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(_session, movieMappingConfig);
            _movieTable.Create();

            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        /// <summary>
        /// Successfully update multiple records using a single (non-batch) update
        /// </summary>
        [Test]
        public void LinqUpdate_Single()
        {
            // Setup
            var table = new Table<Movie>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            var movieToUpdate = _movieList[1];

            var expectedMovie = new Movie(movieToUpdate.Title, movieToUpdate.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate.MovieMaker, 1212);
            table.Where(m => m.Title == movieToUpdate.Title && m.MovieMaker == movieToUpdate.MovieMaker && m.Director == movieToUpdate.Director)
                 .Select(m => new Movie { Year = expectedMovie.Year, MainActor = expectedMovie.MainActor })
                 .Update()
                 .Execute();

            var actualMovieList = table.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie));
            Movie.AssertListContains(actualMovieList, expectedMovie);
        }

        /// <summary>
        /// Try to update a non existing record
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void LinqUpdate_IfExists()
        {
            // Setup
            var table = new Table<Movie>(_session, new MappingConfiguration());
            table.CreateIfNotExists();

            var unexistingMovie = new Movie("Unexisting movie title", "Unexisting movie director", "Unexisting movie actor", "Unexisting movie maker", 1212);
            var cql = table.Where(m => m.Title == unexistingMovie.Title && m.MovieMaker == unexistingMovie.MovieMaker && m.Director == unexistingMovie.Director)
                           .Select(m => new Movie { Year = unexistingMovie.Year, MainActor = unexistingMovie.MainActor })
                           .UpdateIfExists();
            var appliedInfo = cql.Execute();

            Assert.IsFalse(appliedInfo.Applied);
            var actualMovieList = table.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(actualMovieList, unexistingMovie));
        }

        /// <summary>
        /// Successfully update multiple records using a single (non-batch) update, using async execute
        /// </summary>
        [Test]
        public void LinqUpdate_Single_Async()
        {
            // Setup
            var table = new Table<Movie>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            var movieToUpdate = _movieList[1];

            var expectedMovie = new Movie(movieToUpdate.Title, movieToUpdate.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate.MovieMaker, 1212);
            table.Where(m => m.Title == movieToUpdate.Title && m.MovieMaker == movieToUpdate.MovieMaker && m.Director == movieToUpdate.Director)
                 .Select(m => new Movie { Year = expectedMovie.Year, MainActor = expectedMovie.MainActor })
                 .Update()
                 .Execute();

            var actualMovieList = table.ExecuteAsync().Result.ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie));
            Movie.AssertListContains(actualMovieList, expectedMovie);
        }

        /// <summary>
        /// Successfully update multiple records using a Batch update
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void LinqUpdate_Batch()
        {
            // Setup
            var table = new Table<Movie>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            var movieToUpdate1 = _movieList[1];
            var movieToUpdate2 = _movieList[2];

            var batch = new BatchStatement();

            var expectedMovie1 = new Movie(movieToUpdate1.Title, movieToUpdate1.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate1.MovieMaker, 1212);
            var update1 = table.Where(m => m.Title == movieToUpdate1.Title && m.MovieMaker == movieToUpdate1.MovieMaker && m.Director == movieToUpdate1.Director)
                 .Select(m => new Movie { Year = expectedMovie1.Year, MainActor = expectedMovie1.MainActor })
                 .Update();
            batch.Add(update1);

            var expectedMovie2 = new Movie(movieToUpdate2.Title, movieToUpdate2.Director, "also_something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate2.MovieMaker, 1212);
            var update2 = table.Where(m => m.Title == movieToUpdate2.Title && m.MovieMaker == movieToUpdate2.MovieMaker && m.Director == movieToUpdate2.Director)
                 .Select(m => new Movie { Year = expectedMovie2.Year, MainActor = expectedMovie2.MainActor })
                 .Update();
            batch.Add(update2);

            table.GetSession().Execute(batch);

            var actualMovieList = table.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.AreNotEqual(expectedMovie1.MainActor, expectedMovie2.MainActor);
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie1));
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie2));
            Movie.AssertListContains(actualMovieList, expectedMovie1);
            Movie.AssertListContains(actualMovieList, expectedMovie2);
        }

        [TestCase(BatchType.Unlogged)]
        [TestCase(BatchType.Logged)]
        [TestCase(null)]
        [TestCassandraVersion(2, 0)]
        public void LinqUpdate_UpdateBatchType(BatchType batchType)
        {
            // Setup
            var table = new Table<Movie>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            var movieToUpdate1 = _movieList[1];
            var movieToUpdate2 = _movieList[2];

            var batch = table.GetSession().CreateBatch(batchType);

            var expectedMovie1 = new Movie(movieToUpdate1.Title, movieToUpdate1.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate1.MovieMaker, 1212);
            var update1 = table.Where(m => m.Title == movieToUpdate1.Title && m.MovieMaker == movieToUpdate1.MovieMaker && m.Director == movieToUpdate1.Director)
                               .Select(m => new Movie { Year = expectedMovie1.Year, MainActor = expectedMovie1.MainActor })
                               .Update();
            batch.Append(update1);

            var expectedMovie2 = new Movie(movieToUpdate2.Title, movieToUpdate2.Director, "also_something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate2.MovieMaker, 1212);
            var update2 = table.Where(m => m.Title == movieToUpdate2.Title && m.MovieMaker == movieToUpdate2.MovieMaker && m.Director == movieToUpdate2.Director)
                               .Select(m => new Movie { Year = expectedMovie2.Year, MainActor = expectedMovie2.MainActor })
                               .Update();
            batch.Append(update2);

            batch.Execute();

            var actualMovieList = table.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count);
            Assert.AreNotEqual(expectedMovie1.MainActor, expectedMovie2.MainActor);
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie1));
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie2));
            Movie.AssertListContains(actualMovieList, expectedMovie1);
            Movie.AssertListContains(actualMovieList, expectedMovie2);
        }
    }
}
