﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    public class InsertTests : SharedClusterTest
    {
        private ISession _session;
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;

        protected override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            _movieTable = new Table<Movie>(_session, new MappingConfiguration());
            _movieTable.Create();
        }

        [Test, TestCassandraVersion(2, 0)]
        public void LinqInsert_Batch_Test()
        {
            Table<Movie> nerdMoviesTable = new Table<Movie>(_session, new MappingConfiguration());
            Batch batch = _session.CreateBatch();

            Movie movie1 = Movie.GetRandomMovie();
            Movie movie2 = Movie.GetRandomMovie();
            movie1.Director = "Joss Whedon";
            var movies = new List<Movie>
            {
                movie1,
                movie2,
            };

            batch.Append(from m in movies select nerdMoviesTable.Insert(m));
            Task taskSaveMovies = Task.Factory.FromAsync(batch.BeginExecute, batch.EndExecute, null);
            taskSaveMovies.Wait();

            Task taskSelectStartMovies = Task<IEnumerable<Movie>>.Factory.FromAsync(
                nerdMoviesTable.BeginExecute, nerdMoviesTable.EndExecute, null).
                ContinueWith(res => Movie.DisplayMovies(res.Result));
            taskSelectStartMovies.Wait();

            CqlQuery<Movie> selectAllFromWhere = from m in nerdMoviesTable where m.Director == movie1.Director select m;

            Task taskselectAllFromWhere =
                Task<IEnumerable<Movie>>.Factory.FromAsync(selectAllFromWhere.BeginExecute, selectAllFromWhere.EndExecute, null).
                ContinueWith(res => Movie.DisplayMovies(res.Result));
            taskselectAllFromWhere.Wait();
            Task<List<Movie>> taskselectAllFromWhereWithFuture = Task<IEnumerable<Movie>>.
                Factory.FromAsync(selectAllFromWhere.BeginExecute, selectAllFromWhere.EndExecute, null).
                ContinueWith(a => a.Result.ToList());

            Movie.DisplayMovies(taskselectAllFromWhereWithFuture.Result);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void LinqInsert_WithSetTimestamp_Test()
        {
            Table<Movie> nerdMoviesTable = new Table<Movie>(_session, new MappingConfiguration());
            Movie movie1 = Movie.GetRandomMovie();
            nerdMoviesTable.Insert(movie1).Execute();

            string mainActor = "Samuel L. Jackson";
            movie1.MainActor = mainActor;

            nerdMoviesTable
                .Insert(movie1)
                .SetTimestamp(DateTime.Now.AddDays(1))
                .Execute();

            Movie updatedMovie = nerdMoviesTable
                .Where(m => m.Title == movie1.Title && m.MovieMaker == movie1.MovieMaker)
                .Execute()
                .First();

            Assert.AreEqual(updatedMovie.MainActor, mainActor);
        }

        /// <summary>
        /// Testing the CQLGenerator insert query without null values.
        /// Testing if the query has a correct number of placeholders in case of the first column value is null.
        /// 
        /// @jira CSHARP-451 https://datastax-oss.atlassian.net/browse/CSHARP-451
        ///  
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void LinqInsert_WithNullInFirstColumnValue_Test()
        {
            Table<Movie> nerdMoviesTable = new Table<Movie>(_session, new MappingConfiguration());
            Movie movie1 = Movie.GetRandomMovie();
            movie1.MainActor = null; //Setting first column
            nerdMoviesTable.Insert(movie1, false).Execute();

            Movie updatedMovie = nerdMoviesTable
                .Where(m => m.Title == movie1.Title && m.MovieMaker == movie1.MovieMaker)
                .Execute()
                .First();

            Assert.IsNull(updatedMovie.MainActor);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void LinqInsert_Batch_MissingPartitionKeyPart_Test()
        {
            Table<Movie> nerdMoviesTable = new Table<Movie>(_session, new MappingConfiguration());
            Batch batch = _session.CreateBatch();
            Movie movie1 = Movie.GetRandomMovie();
            Movie movie2 = Movie.GetRandomMovie();
            movie1.MovieMaker = null; // missing partition key
            var movies = new List<Movie>
            {
                movie1,
                movie2,
            };
            batch.Append(from m in movies select nerdMoviesTable.Insert(m));
            Task taskSaveMovies = Task.Factory.FromAsync(batch.BeginExecute, batch.EndExecute, null);

            try
            {
                taskSaveMovies.Wait();
            }
            catch (Exception e) // Exception is gathered from the async task
            {
                int maxLayers = 50;
                int layersChecked = 0;
                while (layersChecked < maxLayers && !e.GetType().Equals(typeof(InvalidQueryException)))
                {
                    layersChecked++;
                    e = e.InnerException;
                }
                Assert.IsInstanceOf<InvalidQueryException>(e);
            }

        }

        [Test]
        public void LinqInsert_MissingPartitionKey_Sync_Test()
        {
            var table = new Table<Movie>(_session, new MappingConfiguration());
            Movie objectMissingPartitionKey = new Movie() {MainActor = "doesntmatter"};
            Assert.Throws<InvalidQueryException>(() => table.Insert(objectMissingPartitionKey).Execute());
        }

        [Test]
        public void LinqInsert_MissingPartitionKey_Async_Test()
        {
            var table = new Table<Movie>(_session, new MappingConfiguration());
            Movie objectMissingPartitionKey = new Movie() {MainActor = "doesntmatter"};
            
            try
            {
                table.Insert(objectMissingPartitionKey).ExecuteAsync().Wait();
            }
            catch (Exception e) // Exception is gathered from the async task
            {
                int maxLayers = 50;
                int layersChecked = 0;
                while (layersChecked < maxLayers && !e.GetType().Equals(typeof(InvalidQueryException)))
                {
                    layersChecked++;
                    e = e.InnerException;
                }
                Assert.IsInstanceOf<InvalidQueryException>(e);
            }
        }

        [Test]
        public void LinqInsert_IfNotExists_Test()
        {
            var nerdMoviesTable = new Table<Movie>(_session, new MappingConfiguration());
            var movie = Movie.GetRandomMovie();

            var appliedInfo = nerdMoviesTable.
                Insert(movie)
                .IfNotExists()
                .Execute();
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);

            Assert.NotNull(nerdMoviesTable
                .Where(m => m.Title == movie.Title && m.MovieMaker == movie.MovieMaker)
                .Execute()
                .FirstOrDefault());

            //Try to create another with the same partition and clustering keys
            appliedInfo = nerdMoviesTable
                .Insert(new Movie { Title = movie.Title, Director = movie.Director, MovieMaker = movie.MovieMaker})
                .IfNotExists()
                .Execute();

            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual(movie.Year, appliedInfo.Existing.Year);
        }
    }
}
