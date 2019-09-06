using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqTable
{
    [Category("short"), Category("realcluster")]
    public class TableSelectDefault: SharedClusterTest
    {
        private ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        private string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = TestClusterManager.GetTestCluster(1).Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            MappingConfiguration movieMappingConfig = new MappingConfiguration();
            //Using attributes is deprecated
            #pragma warning disable 612
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(_session, movieMappingConfig);
            #pragma warning restore 612
            _movieTable.Create();


            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [Test]
        public void LinqTable_Sync()
        {
            var movies = _movieTable.Execute().ToArray();
            Assert.AreEqual(_movieList.Count, movies.Length);
        }

        [Test]
        public void LinqTable_Async()
        {
            // insert new row
            var movies = _movieTable.ExecuteAsync().Result.ToArray();
            Assert.AreEqual(_movieList.Count, movies.Length);
        }
    }
}
