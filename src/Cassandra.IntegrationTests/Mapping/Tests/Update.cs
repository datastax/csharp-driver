using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class Update : TestGlobals
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
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
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        /// <summary>
        /// Attempt to update a single record to the same values, validate that the record is not altered.
        /// </summary>
        [Test, NUnit.Framework.Ignore("Update values are not being ordered correctly")]
        public void Update_Single_ToSameValues()
        {
            // Setup
            Movie movieToUpdate = _movieList[1];

            // update to the same values
            _mapper.Update(movieToUpdate);

            List<Movie> actualMovieList = _movieTable.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Movie.AssertListContains(actualMovieList, movieToUpdate);
        }

        /// <summary>
        /// Update a single record to the same values, validate that the resultant data in Cassandra is correct.
        /// </summary>
        [Test, NUnit.Framework.Ignore("Update values are not being ordered correctly")]
        public void Update_Single_ToDifferentValues()
        {
            // Setup
            Movie movieToUpdate = _movieList[1];

            // Update to different values
            var expectedMovie = new Movie(movieToUpdate.Title, movieToUpdate.Director, "something_different_" + Randomm.RandomAlphaNum(10), movieToUpdate.MovieMaker, 1212);
            _mapper.Update(expectedMovie);

            List<Movie> actualMovieList = _movieTable.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie));
            Movie.AssertListContains(actualMovieList, expectedMovie);
        }

        public class ExtMovie
        {
            public int Size;
            public string TheDirector;
            public string TheMaker;
        }


    }
}
