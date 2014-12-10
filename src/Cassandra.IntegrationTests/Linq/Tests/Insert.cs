using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Tests
{
    [Category("short")]
    public class Insert : TestGlobals
    {
        ISession _session = null;
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

        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        [Test]
        public void LinqInsert_Batch()
        {
            Table<Movie> nerdMoviesTable = _session.GetTable<Movie>();
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

        [Test]
        public void LinqInsert_Batch_MissingPartitionKeyPart()
        {
            Table<Movie> nerdMoviesTable = _session.GetTable<Movie>();
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

            string expectedErrMsg = "Invalid null value for partition key part movie_maker";
            try
            {
                taskSaveMovies.Wait();
            }
            catch (Exception e) // Exception is gathered from the async task
            {
                Exception exceptionBeingChecked = e;
                int maxLayers = 50;
                int layersChecked = 0;
                while (layersChecked < maxLayers && !e.InnerException.Message.Contains(expectedErrMsg))
                {
                    layersChecked++;
                    e = e.InnerException;
                }
                Assert.AreEqual(expectedErrMsg, e.InnerException.Message);
            }

        }

        [Test]
        public void LinqInsert_MissingPartitionKey_Sync()
        {
            var table = _session.GetTable<Movie>();
            Movie objectMissingPartitionKey = new Movie() {MainActor = "doesntmatter"};
            string expectedErrMsg = "Invalid null value for partition key part unique_movie_title";
            try
            {
                table.Insert(objectMissingPartitionKey).Execute();
            }
            catch (InvalidQueryException e)
            {
                Assert.IsTrue(e.Message.Contains(expectedErrMsg));
            }
        }

        [Test]
        public void LinqInsert_MissingPartitionKey_Async()
        {
            var table = _session.GetTable<Movie>();
            Movie objectMissingPartitionKey = new Movie() {MainActor = "doesntmatter"};
            string expectedErrMsg = "Invalid null value for partition key part unique_movie_title";
            try
            {
                RowSet rowSet = table.Insert(objectMissingPartitionKey).ExecuteAsync().Result;
            }
            catch (Exception e) // Exception is gathered from the async task
            {
                Exception exceptionBeingChecked = e;
                int maxLayers = 50;
                int layersChecked = 0;
                while (layersChecked < maxLayers && !e.InnerException.Message.Contains(expectedErrMsg))
                {
                    layersChecked++;
                    e = e.InnerException;
                }
                Assert.AreEqual(expectedErrMsg, e.InnerException.Message);
            }
        }


    }
}
