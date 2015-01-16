using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class Delete : TestGlobals
    {
        ISession _session = null;
        private List<Movie> _movieList;
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Mapper _mapper;

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var config = new Map<Movie>().PartitionKey(c => c.Title).PartitionKey(c => c.MovieMaker);
            var mappingConfig = new MappingConfiguration().Define(config);
            _mapper = new Mapper(_session, mappingConfig);
            _movieTable = new Table<Movie>(_session, mappingConfig);
            _movieTable.Create();

            //Insert some data
            _movieList = Movie.GetDefaultMovieList();
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        /// <summary>
        /// Successfully delete a single record using a mapped instance
        /// </summary>
        [Test]
        public void Delete_Success()
        {
            // Setup
            Movie movieToDelete = _movieList[1];

            // Delete the record
            _mapper.Delete(movieToDelete);

            List<Movie> actualMovieList = _movieTable.Execute().ToList();
            Assert.AreEqual(_movieList.Count - 1, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(actualMovieList, movieToDelete));
        }

        /// <summary>
        /// Successfully delete a single record using a mapped instance, async
        /// </summary>
        [Test]
        public void Delete_Async_Success()
        {
            // Setup
            Movie movieToDelete = _movieList[1];

            // Delete the record
            _mapper.DeleteAsync(movieToDelete).Wait();

            List<Movie> actualMovieList = _movieTable.Execute().ToList();
            Assert.AreEqual(_movieList.Count - 1, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(actualMovieList, movieToDelete));
        }

        /// <summary>
        /// Successfully delete a single record using a mapped instance, async
        /// with
        /// </summary>
        [Test]
        public void Delete_ConsistencyLevel_Valids()
        {
            // Setup
            Movie movieToDelete = _movieList[1];

            // Insert the data
            var consistencyLevels = new ConsistencyLevel[]
            {
                ConsistencyLevel.All,
                ConsistencyLevel.Any,
                ConsistencyLevel.EachQuorum,
                ConsistencyLevel.LocalOne,
                ConsistencyLevel.LocalQuorum,
                ConsistencyLevel.One,
                ConsistencyLevel.Quorum,
            };
            foreach (var consistencyLevel in consistencyLevels)
            {
                // Delete the record
                _mapper.DeleteAsync(movieToDelete, new CqlQueryOptions().SetConsistencyLevel(consistencyLevel)).Wait();

                List<Movie> actualMovieList = _movieTable.Execute().ToList();
                DateTime futureDateTime = DateTime.Now.AddSeconds(2);
                while (actualMovieList.Count == _movieList.Count && futureDateTime > DateTime.Now)
                {
                    actualMovieList = _movieTable.Execute().ToList();
                }
                Assert.AreEqual(_movieList.Count - 1, actualMovieList.Count(), "Unexpected failure for consistency level: " + consistencyLevel);
                Assert.IsFalse(Movie.ListContains(actualMovieList, movieToDelete));

                // re-insert the movie
                _mapper.Insert(movieToDelete);
                actualMovieList.Clear();
                actualMovieList = _movieTable.Execute().ToList();
                futureDateTime = DateTime.Now.AddSeconds(2);
                while (actualMovieList.Count < _movieList.Count && futureDateTime > DateTime.Now)
                {
                    actualMovieList = _movieTable.Execute().ToList();
                }
                Assert.AreEqual(actualMovieList.Count, _movieList.Count);
            }
        }

        /// <summary>
        /// Successfully delete a single record using a mapped instance, async
        /// Also set the consistency level to one more than the current number of nodes
        /// Expect the request to fail silently.
        /// </summary>
        [Test]
        public void Delete_ConsistencyLevel_Invalids()
        {
            // Setup
            Movie movieToDelete = _movieList[1];

            // Attempt to Delete the record
            Assert.Throws<AggregateException>(() => _mapper.DeleteAsync(movieToDelete, new CqlQueryOptions().SetConsistencyLevel(ConsistencyLevel.Two)).Wait());

            Assert.Throws<AggregateException>(() => _mapper.DeleteAsync(movieToDelete, new CqlQueryOptions().SetConsistencyLevel(ConsistencyLevel.Three)).Wait());
        }


        /// <summary>
        /// Attempt to delete a record by passing in a mapped object that does not 
        /// </summary>
        [Test]
        public void Delete_PartitionNonExistent()
        {
            // Setup
            Movie movieToDelete = _movieList[1];
            movieToDelete.MovieMaker = "somethingRandom_" + Guid.NewGuid().ToString();

            // Delete the record
            _mapper.Delete(movieToDelete); 

            // Validate that nothing was deleted
            List<Movie> actualMovieList = _movieTable.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
        }

        /// <summary>
        /// Attempt to delete a record by passing in a mapped object that does not 
        /// </summary>
        [Test]
        public void Delete_PartitionKeyNull()
        {
            // Setup
            Movie movieToDelete = _movieList[1];
            movieToDelete.MovieMaker = null;

            // Error expected
            var ex = Assert.Throws<InvalidQueryException>(() => _mapper.Delete(movieToDelete));
            Assert.AreEqual("Invalid null value for partition key part moviemaker", ex.Message);
        }

        public class ExtMovie
        {
            public int Size;
            public string TheDirector;
            public string TheMaker;
        }


    }
}
