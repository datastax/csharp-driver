using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class Update : SharedClusterTest
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Mapper _mapper;

        protected override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var config = new Map<Movie>().PartitionKey(c => c.MovieMaker);
            var mappingConfig = new MappingConfiguration().Define(config);
            _mapper = new Mapper(_session, mappingConfig);
            _movieTable = new Table<Movie>(_session, mappingConfig);
            _movieTable.Create();

            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        /// <summary>
        /// Attempt to update a single record to the same values, validate that the record is not altered.
        /// 
        /// @test_category queries:basic
        /// </summary>
        [Test]
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
        /// Update a single record to different values, validate that the resultant data in Cassandra is correct.
        /// 
        /// @test_category queries:basic
        /// </summary>
        [Test]
        public void Update_Single_ToDifferentValues()
        {
            // Setup
            Movie movieToUpdate = _movieList[1];

            // Update to different values
            var expectedMovie = new Movie(movieToUpdate.Title + "_something_different", movieToUpdate.Director, movieToUpdate.MainActor + "_something_different", movieToUpdate.MovieMaker, 1212);
            _mapper.Update(expectedMovie);

            List<Movie> actualMovieList = _movieTable.Execute().ToList();
            Assert.AreEqual(_movieList.Count, actualMovieList.Count());
            Assert.IsFalse(Movie.ListContains(_movieList, expectedMovie));
            Movie.AssertListContains(actualMovieList, expectedMovie);
            Assert.IsFalse(Movie.ListContains(actualMovieList, movieToUpdate));
        }

        [Test]
        public void UpdateIf_Applied_Test()
        {
            var config = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_update_if"));
            //Use linq to create the table
            new Table<Song>(_session, config).Create();
            var mapper = new Mapper(_session, config);
            var song = new Song { Id = Guid.NewGuid(), Artist = "Cream", Title = "Crossroad", ReleaseDate = DateTimeOffset.Parse("1970/1/1")};
            //It is the first song there, it should apply it
            mapper.Insert(song);
            const string query = "SET artist = ?, title = ? WHERE id = ? IF releasedate = ?";
            var appliedInfo = mapper.UpdateIf<Song>(Cql.New(query, song.Artist, "Crossroad2", song.Id, song.ReleaseDate));
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);
            //Following times, it should not apply the mutation as the condition is not valid
            appliedInfo = mapper.UpdateIf<Song>(Cql.New(query, song.Artist, "Crossroad3", song.Id, DateTimeOffset.Now));
            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual(song.ReleaseDate, appliedInfo.Existing.ReleaseDate);
            Assert.AreEqual("Crossroad2", mapper.First<Song>("WHERE id = ?", song.Id).Title);
        }

        /// <summary>
        /// Attempt to update a record without defining the partition key
        /// 
        /// @test_category queries:basic
        /// </summary>
        [Test]
        public void Update_PartitionKeyOmitted()
        {
            // Setup
            Movie movieToUpdate = _movieList[1];

            // Update to different values
            var expectedMovie = new Movie(movieToUpdate.Title + "_something_different", movieToUpdate.Director, "something_different_" + Randomm.RandomAlphaNum(10), null, 1212);
            var err = Assert.Throws<InvalidQueryException>(() => _mapper.Update(expectedMovie));
            string expectedErrMsg = "Invalid null value (for partition key part|in condition for column) moviemaker";
            StringAssert.IsMatch(expectedErrMsg, err.Message);
        }

        public class ExtMovie
        {
            public int Size;
            public string TheDirector;
            public string TheMaker;
        }


    }
}
