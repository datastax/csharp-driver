using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class OrderBy : TestGlobals
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
        public void LinqOrderBy()
        {
            var table = _session.GetTable<Movie>();

            List<Movie> moreMovies = new List<Movie>();
            string sameTitle = "sameTitle";
            string sameMovieMaker = "sameMovieMaker";
            for (int i = 0; i < 10; i++)
            {
                Movie movie = Movie.GetRandomMovie();
                movie.Title = sameTitle;
                movie.MovieMaker = sameMovieMaker;
                moreMovies.Add(movie);
                table.Insert(movie).Execute();
            }

            var movieQuery = table.Where(m => m.Title == sameTitle && m.MovieMaker == sameMovieMaker);

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
            var table = _session.GetTable<Movie>();
            try
            {
                table.OrderBy(m => m.MainActor).Execute();
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
            var table = _session.GetTable<Movie>();
            try
            {
                var nonUsedEnumerable = table.OrderBy(m => m.MainActor).ExecuteAsync().Result;
                Assert.Fail("Expected Exception was not thrown!");
            }
            catch (Exception e) // Exception is gathered from the async task
            {
                string expectedException = "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.";
                Exception exceptionBeingChecked = e;
                int maxLayers = 50;
                int layersChecked = 0;
                while (layersChecked < maxLayers && !e.InnerException.Message.Contains(expectedException))
                {
                    layersChecked++;
                    e = e.InnerException;
                }
                Assert.AreEqual(expectedException, e.InnerException.Message);
            }
        }


    }
}
