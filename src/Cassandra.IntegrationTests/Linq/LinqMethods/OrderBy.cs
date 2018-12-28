using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class OrderBy : SharedClusterTest
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
        public void LinqOrderBy()
        {
            List<Movie> moreMovies = new List<Movie>();
            string sameTitle = "sameTitle";
            string sameMovieMaker = "sameMovieMaker";
            for (int i = 0; i < 10; i++)
            {
                Movie movie = Movie.GetRandomMovie();
                movie.Title = sameTitle;
                movie.MovieMaker = sameMovieMaker;
                moreMovies.Add(movie);
                _movieTable.Insert(movie).Execute();
            }

            var movieQuery = _movieTable.Where(m => m.Title == sameTitle && m.MovieMaker == sameMovieMaker);

            List<Movie> actualOrderedMovieList = movieQuery.Execute().ToList();
            List<Movie> expectedOrderedMovieList = moreMovies.OrderBy(m => m.Director).ToList();
            Assert.AreEqual(expectedOrderedMovieList.Count, actualOrderedMovieList.Count);
            for (int i = 0; i < expectedOrderedMovieList.Count; i++)
            {
                Assert.AreEqual(expectedOrderedMovieList[i].Director, actualOrderedMovieList[i].Director);
                Assert.AreEqual(expectedOrderedMovieList[i].MainActor, actualOrderedMovieList[i].MainActor);
                Assert.AreEqual(expectedOrderedMovieList[i].MovieMaker, actualOrderedMovieList[i].MovieMaker);
            }
        }

        [Test]
        public void LinqOrderBy_Unrestricted_Sync()
        {
            try
            {
                _movieTable.OrderBy(m => m.MainActor).Execute();
                Assert.Fail("Expected Exception was not thrown!");
            }
            catch (InvalidQueryException e)
            {
                Assert.AreEqual("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.", e.Message);
            }
        }

        [Test]
        public void LinqOrderBy_Unrestricted_Async()
        {
            var ex = Assert.ThrowsAsync<InvalidQueryException>(
                async () => await _movieTable.OrderBy(m => m.MainActor).ExecuteAsync().ConfigureAwait(false));
            const string expectedException = "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.";
            Assert.AreEqual(expectedException, ex.Message);
        }


    }
}
