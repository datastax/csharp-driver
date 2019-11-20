//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

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
    [Category("short"), Category("realcluster")]
    public class Delete : SharedClusterTest
    {
        ISession _session = null;
        private List<Movie> _movieList;
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;
        private Mapper _mapper;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
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
        }

        [SetUp]
        public void TestSetup()
        {
            foreach (var movie in _movieList)
            {
                _movieTable.Insert(movie).Execute();
            }
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

        [Test]
        public void DeleteIf_Applied_Test()
        {
            var config = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_delete_if"));
            //Use linq to create the table
            new Table<Song>(_session, config).Create();
            var mapper = new Mapper(_session, config);
            var song = new Song { Id = Guid.NewGuid(), Artist = "Cream", Title = "Crossroad", ReleaseDate = DateTimeOffset.Parse("1970/1/1") };
            mapper.Insert(song);
            //It should not apply it as the condition will NOT be satisfied
            var appliedInfo = mapper.DeleteIf<Song>(Cql.New("WHERE id = ? IF title = ?", song.Id, "Crossroad2"));
            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual("Crossroad", appliedInfo.Existing.Title);
            //It should apply it as the condition will be satisfied
            appliedInfo = mapper.DeleteIf<Song>(Cql.New("WHERE id = ? IF title = ?", song.Id, song.Title));
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);
        }
    }
}
