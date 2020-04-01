//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Tests;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using Dse.Test.Unit.Mapping.Pocos;
using NUnit.Framework;

namespace Dse.Test.Integration.Mapping.Tests
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class Update : SharedClusterTest
    {
        ISession _session;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Mapper _mapper;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            _session.Execute(string.Format(PocoWithEnumCollections.DefaultCreateTableCql, "tbl_with_enum_collections"));
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

        [Test]
        public void Update_Poco_With_Enum_Collections_Test()
        {
            var expectedCollection = new[]{ HairColor.Blonde, HairColor.Gray };
            var expectedMap = new SortedDictionary<HairColor, TimeUuid>
            {
                { HairColor.Brown, TimeUuid.NewId() },
                { HairColor.Red, TimeUuid.NewId() }
            };
            var collectionValues = expectedCollection.Select(x => (int)x).ToArray();
            var mapValues =
                new SortedDictionary<int, Guid>(expectedMap.ToDictionary(kv => (int) kv.Key, kv => (Guid) kv.Value));

            var pocoToUpdate = new PocoWithEnumCollections
            {
                Id = 3000L,
                Dictionary1 = expectedMap.ToDictionary(x => x.Key, x=> x.Value),
                Dictionary2 = expectedMap.ToDictionary(x => x.Key, x=> x.Value),
                Dictionary3 = expectedMap,
                List1 = expectedCollection.ToList(),
                List2 = expectedCollection.ToList(),
                Set1 = new SortedSet<HairColor>(expectedCollection),
                Set2 = new SortedSet<HairColor>(expectedCollection),
                Set3 = new HashSet<HairColor>(expectedCollection)
            };
            pocoToUpdate.Array1 = new[]{ HairColor.Blonde, HairColor.Red, HairColor.Black };
            pocoToUpdate.Dictionary1.Add(HairColor.Black, Guid.NewGuid());
            pocoToUpdate.Dictionary2.Add(HairColor.Black, Guid.NewGuid());
            pocoToUpdate.List1.Add(HairColor.Black);
            pocoToUpdate.Set1.Add(HairColor.Black);
            pocoToUpdate.Set2.Add(HairColor.Black);
            pocoToUpdate.Set3.Add(HairColor.Black);
            const string insertQuery =
                "INSERT INTO tbl_with_enum_collections (id, list1, list2, array1, set1, set2, set3, map1, map2, map3)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            _session.Execute(new SimpleStatement(insertQuery, pocoToUpdate.Id, collectionValues, collectionValues,
                collectionValues, collectionValues, collectionValues, collectionValues, mapValues, mapValues,
                mapValues));
            
            var config =
                new MappingConfiguration().Define(
                    PocoWithEnumCollections.DefaultMapping.TableName("tbl_with_enum_collections"));
            var mapper = new Mapper(_session, config);
            mapper.Update(pocoToUpdate);
            var statement = new SimpleStatement("SELECT * FROM tbl_with_enum_collections WHERE id = ?", pocoToUpdate.Id);
            
            var row = _session.Execute(statement).First();
            Assert.AreEqual(pocoToUpdate.Id, row.GetValue<long>("id"));
            CollectionAssert.AreEquivalent(pocoToUpdate.List1.Select(x => (int)x).ToList(), row.GetValue<IEnumerable<int>>("list1"));
            CollectionAssert.AreEquivalent(pocoToUpdate.List2.Select(x => (int)x).ToList(), row.GetValue<IEnumerable<int>>("list2"));
            CollectionAssert.AreEquivalent(pocoToUpdate.Array1.Select(x => (int)x).ToArray(), row.GetValue<IEnumerable<int>>("array1"));
            CollectionAssert.AreEquivalent(pocoToUpdate.Set1.Select(x => (int)x), row.GetValue<IEnumerable<int>>("set1"));
            CollectionAssert.AreEquivalent(pocoToUpdate.Set2.Select(x => (int)x), row.GetValue<IEnumerable<int>>("set2"));
            CollectionAssert.AreEquivalent(pocoToUpdate.Set3.Select(x => (int)x), row.GetValue<IEnumerable<int>>("set3"));
            CollectionAssert.AreEquivalent(pocoToUpdate.Dictionary1.ToDictionary(x => (int) x.Key, x=> (Guid)x.Value), 
                row.GetValue<IDictionary<int, Guid>>("map1"));
            CollectionAssert.AreEquivalent(pocoToUpdate.Dictionary2.ToDictionary(x => (int) x.Key, x=> (Guid)x.Value), 
                row.GetValue<IDictionary<int, Guid>>("map2"));
            CollectionAssert.AreEquivalent(pocoToUpdate.Dictionary3.ToDictionary(x => (int) x.Key, x=> (Guid)x.Value), 
                row.GetValue<IDictionary<int, Guid>>("map3"));
        }

        public class ExtMovie
        {
            public int Size;
            public string TheDirector;
            public string TheMaker;
        }
    }
}
