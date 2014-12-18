using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class TableSelectDefault: TestGlobals
    {
        private ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        private string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

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
        public void LinqTable_Sync()
        {
            var table = _session.GetTable<Movie>();
            var movies = table.Execute().ToArray();
            Assert.AreEqual(_movieList.Count, movies.Length);
        }

        [Test]
        public void LinqTable_Async()
        {
            // insert new row
            var table = _session.GetTable<Movie>();
            var movies = table.ExecuteAsync().Result.ToArray();
            Assert.AreEqual(_movieList.Count, movies.Length);
        }
    }
}
